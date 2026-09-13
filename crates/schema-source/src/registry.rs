use anyhow::{Result, anyhow, ensure};
use base64::Engine;
use onetui_core::config::{safe_name, secret};
use serde::Deserialize;
use std::{
    collections::{HashMap, VecDeque},
    io::Read,
    time::{Duration, Instant},
};

const RESPONSE_BYTES: usize = 256 * 1024;
const CACHE_ENTRIES: usize = 8;

use crate::{Format, Preview};

fn registry_type(format: Format) -> &'static str {
    match format {
        Format::Avro => "AVRO",
        Format::Protobuf => "PROTOBUF",
    }
}

enum Compiled {
    Avro(onetui_avro::Decoder),
    Protobuf(onetui_protobuf::SourceSchema),
}

pub fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "required": "confluent only; forbidden with raw", "type": "table", "default": "none; no registry requests without a binding",
        "fields": {
            "url": {"required": true, "type": "HTTP(S) base URL up to 512 UTF-8 bytes without controls, userinfo, query or fragment", "purpose": "HTTPS for remote endpoints; HTTP only for literal loopback IPs. Base paths allowed. Redirects and environment proxies disabled."},
            "ca_file": {"required": false, "default": "platform TLS verification", "type": "absolute regular-file path up to 4096 UTF-8 bytes without controls", "purpose": "HTTPS-only PEM trust bundle up to 1 MiB; replaces system trust for this binding. No tilde/environment expansion."},
            "username_env": {"required": "with password_env", "default": "none", "type": "ASCII environment-variable name", "purpose": "HTTP Basic username; independent of broker credentials; no colon in resolved username"},
            "password_env": {"required": "with username_env", "default": "none", "type": "ASCII environment-variable name", "purpose": "HTTP Basic password; cannot combine Basic authentication with token_env"},
            "token_env": {"required": false, "default": "none", "type": "ASCII environment-variable name", "purpose": "Bearer token instead of Basic authentication; never broker OAuth"}
        },
        "credentials": "Resolved only for the selected connection, nonempty, at most 4096 UTF-8 bytes without controls; no credentials means anonymous access.",
        "limits": {"resolution_timeout_ms": 2000, "response_bytes": 262144, "header_bytes": 16384, "bundle_bytes": 262144, "references": 32, "reference_depth": 8, "cached_ids_per_binding": 8, "protobuf_message_index_depth": 32, "protobuf_source_nesting": 32, "protobuf_source_declarations": 4096},
        "behavior": "Read-only exact-ID and versioned-reference requests. FIFO cache includes lookup failures; eviction or reopening permits a new lookup. Two-second bound per uncached schema and its references, with foreground deadline/cancellation checks between requests. Source compilation is in-memory with protox, supplied imports and embedded Google types only; no filesystem lookup. Source and compiled descriptors each have a 256 KiB cap. Native cleanup may outlive foreground cancellation; compilation has input/work bounds, not a preemptive CPU deadline. No RSS guarantee. --check requests schemas/types, not every record schema."
    })
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub url: String,
    pub ca_file: Option<String>,
    pub username_env: Option<String>,
    pub password_env: Option<String>,
    pub token_env: Option<String>,
    #[serde(skip)]
    authorization: Option<String>,
    #[serde(skip)]
    secrets: Vec<String>,
}

impl Config {
    pub(crate) fn bearer(url: String, ca_file: Option<String>, token_env: Option<String>) -> Self {
        Self {
            url,
            ca_file,
            token_env,
            username_env: None,
            password_env: None,
            authorization: None,
            secrets: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        let url = url::Url::parse(&self.url)?;
        ensure!(
            self.url.len() <= 512
                && !self.url.chars().any(char::is_control)
                && url.host().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && matches!(url.scheme(), "http" | "https"),
            "Invalid registry URL"
        );
        if url.scheme() == "http" {
            // Literal loopback avoids resolving an attacker-controlled name to a remote address.
            ensure!(
                url.host_str().is_some_and(|h| h
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())),
                "HTTP registry requires a literal loopback address; use HTTPS remotely"
            );
            ensure!(self.ca_file.is_none(), "Registry ca_file requires HTTPS");
        }
        if let Some(path) = &self.ca_file {
            crate::validate_path(path)?;
        }
        ensure!(
            self.username_env.is_some() == self.password_env.is_some()
                && !(self.token_env.is_some() && self.username_env.is_some()),
            "Registry authentication requires username_env and password_env together, or token_env"
        );
        for name in [&self.username_env, &self.password_env, &self.token_env]
            .into_iter()
            .flatten()
        {
            ensure!(safe_name(name), "Invalid registry secret reference");
        }
        Ok(())
    }

    pub fn resolve(&mut self, env: &dyn Fn(&str) -> Option<String>) -> Result<Vec<String>> {
        let mut resolve = |name: &str| -> Result<String> {
            let value = secret(name, env)?;
            ensure!(
                value.len() <= 4096 && !value.chars().any(char::is_control),
                "Invalid registry credential"
            );
            self.secrets.push(value.clone());
            Ok(value)
        };
        self.authorization = if let Some(name) = &self.token_env {
            Some(format!("Bearer {}", resolve(name)?))
        } else if let Some(name) = &self.username_env {
            let username = resolve(name)?;
            ensure!(
                !username.contains(':'),
                "Registry Basic username must not contain colon"
            );
            let password = resolve(self.password_env.as_deref().unwrap())?;
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
            self.secrets.push(encoded.clone());
            Some(format!("Basic {encoded}"))
        } else {
            None
        };
        if let Some(auth) = &self.authorization {
            self.secrets.push(auth.clone());
        }
        Ok(self.secrets.clone())
    }
}

#[derive(Deserialize)]
struct Schema {
    schema: String,
    #[serde(rename = "schemaType")]
    kind: Option<String>,
    #[serde(default)]
    references: Vec<Reference>,
}

#[derive(Deserialize)]
struct Reference {
    name: String,
    subject: String,
    version: u32,
}

pub struct Registry {
    config: Config,
    agent: ureq::Agent,
    format: Format,
    cache: VecDeque<(u32, Result<Compiled, String>)>,
    reader: Option<onetui_avro::ReaderSchema>,
}

impl Registry {
    pub fn new(config: Config, format: Format) -> Result<Self> {
        let agent = config.agent()?;
        Ok(Self {
            config,
            agent,
            format,
            cache: VecDeque::new(),
            reader: None,
        })
    }

    pub fn with_reader(mut self, reader: Option<onetui_avro::ReaderSchema>) -> Self {
        self.reader = reader;
        self
    }

    fn request(
        &self,
        segments: &[&str],
        deadline: Instant,
        remaining: &impl Fn() -> Result<()>,
    ) -> Result<Vec<u8>> {
        self.config
            .request(&self.agent, segments, None, deadline, remaining)
    }
}

pub fn http_agent(ca_file: Option<&str>) -> Result<ureq::Agent> {
    let roots = if let Some(path) = ca_file {
        let pem = crate::read_file(path, 1024 * 1024)?;
        let mut certs = Vec::new();
        for item in ureq::tls::parse_pem(&pem) {
            if let ureq::tls::PemItem::Certificate(cert) = item? {
                certs.push(cert);
            }
        }
        ensure!(!certs.is_empty(), "HTTP ca_file contains no certificates");
        ureq::tls::RootCerts::new_with_certs(&certs)
    } else {
        ureq::tls::RootCerts::PlatformVerifier
    };
    let agent = ureq::Agent::config_builder()
        .tls_config(ureq::tls::TlsConfig::builder().root_certs(roots).build())
        .proxy(None)
        .max_redirects(0)
        .http_status_as_error(false)
        .max_response_header_size(16 * 1024)
        .max_idle_connections(1)
        .timeout_global(Some(Duration::from_secs(2)))
        .build()
        .into();
    Ok(agent)
}

impl Config {
    pub(crate) fn agent(&self) -> Result<ureq::Agent> {
        http_agent(self.ca_file.as_deref())
    }

    pub(crate) fn request(
        &self,
        agent: &ureq::Agent,
        segments: &[&str],
        body: Option<&serde_json::Value>,
        deadline: Instant,
        remaining: &impl Fn() -> Result<()>,
    ) -> Result<Vec<u8>> {
        remaining()?;
        let timeout = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| anyhow!("Registry resolution exceeded two seconds"))?;
        let mut url = url::Url::parse(&self.url)?;
        url.path_segments_mut()
            .map_err(|_| anyhow!("Invalid registry base URL"))?
            .pop_if_empty()
            .extend(segments);
        let mut response = if let Some(body) = body {
            let mut request = agent
                .post(url.as_str())
                .config()
                .timeout_global(Some(timeout))
                .build()
                .header("Content-Type", "application/json")
                .header("Connect-Protocol-Version", "1");
            if let Some(auth) = &self.authorization {
                request = request.header("Authorization", auth);
            }
            request.send(body.to_string())?
        } else {
            let mut request = agent
                .get(url.as_str())
                .config()
                .timeout_global(Some(timeout))
                .build();
            if let Some(auth) = &self.authorization {
                request = request.header("Authorization", auth);
            }
            request.call()?
        };
        let status = response.status();
        let mut bytes = Vec::new();
        response
            .body_mut()
            .as_reader()
            .take(RESPONSE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        remaining()?;
        ensure!(
            bytes.len() <= RESPONSE_BYTES,
            "Registry response exceeds 256 KiB"
        );
        ensure!(
            status.is_success(),
            "Registry HTTP {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
        Ok(bytes)
    }

    pub fn diagnostic(&self, error: anyhow::Error) -> String {
        onetui_core::diagnostic(
            error,
            &self.secrets.iter().map(String::as_str).collect::<Vec<_>>(),
        )
        .to_string()
    }
}

impl Registry {
    pub fn check(&self, remaining: &impl Fn() -> Result<()>) -> Result<()> {
        let bytes = self.request(
            &["schemas", "types"],
            Instant::now() + Duration::from_secs(2),
            remaining,
        )?;
        let types: Vec<String> = serde_json::from_slice(&bytes)?;
        ensure!(
            types.iter().any(|t| t == registry_type(self.format)),
            "Registry does not advertise {}",
            registry_type(self.format)
        );
        Ok(())
    }

    fn get(
        &self,
        segments: &[&str],
        deadline: Instant,
        remaining: &impl Fn() -> Result<()>,
    ) -> Result<Schema> {
        let bytes = self.request(segments, deadline, remaining)?;
        let schema: Schema = serde_json::from_slice(&bytes)?;
        ensure!(
            schema.kind.as_deref().unwrap_or("AVRO") == registry_type(self.format),
            "Registry schema is not {}",
            registry_type(self.format)
        );
        ensure!(
            schema.references.len() <= 32,
            "Registry references exceed 32"
        );
        Ok(schema)
    }

    fn load(&self, id: u32, remaining: &impl Fn() -> Result<()>) -> Result<Compiled> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let root = self.get(&["schemas", "ids", &id.to_string()], deadline, remaining)?;
        let mut pending = root
            .references
            .into_iter()
            .map(|r| (r, 1))
            .collect::<Vec<_>>();
        let mut names = HashMap::new();
        let mut references = Vec::new();
        let mut bytes = root.schema.len();
        while let Some((reference, depth)) = pending.pop() {
            ensure!(depth <= 8, "Registry reference depth exceeds 8");
            ensure!(
                reference.version > 0
                    && reference.version <= i32::MAX as u32
                    && !reference.name.is_empty()
                    && reference.name.len() <= 1024
                    && !reference.subject.is_empty()
                    && reference.subject.len() <= 1024
                    && !reference.subject.chars().any(char::is_control)
                    && !matches!(reference.subject.as_str(), "." | ".."),
                "Invalid registry reference"
            );
            let key = (reference.subject, reference.version);
            if let Some(previous) = names.get(&reference.name) {
                ensure!(previous == &key, "Conflicting registry reference name");
                continue;
            }
            ensure!(
                names.len() < 32,
                "Too many or conflicting registry references"
            );
            names.insert(reference.name.clone(), key.clone());
            let schema = self.get(
                &["subjects", &key.0, "versions", &key.1.to_string()],
                deadline,
                remaining,
            )?;
            bytes += schema.schema.len();
            ensure!(
                bytes <= RESPONSE_BYTES,
                "Registry schema bundle exceeds 256 KiB"
            );
            ensure!(
                pending.len() + schema.references.len() <= 32,
                "Registry reference queue exceeds 32"
            );
            pending.extend(schema.references.into_iter().map(|r| (r, depth + 1)));
            references.push((reference.name, schema.schema));
        }
        remaining()?;
        let decoder = match self.format {
            Format::Avro => {
                let mut decoder = onetui_avro::Decoder::with_references(
                    &root.schema,
                    &references
                        .iter()
                        .map(|(_, s)| s.as_str())
                        .collect::<Vec<_>>(),
                )?;
                if let Some(reader) = &self.reader {
                    decoder = decoder.with_reader(reader.clone());
                }
                Compiled::Avro(decoder)
            }
            Format::Protobuf => Compiled::Protobuf(onetui_protobuf::SourceSchema::compile(
                &root.schema,
                &references
                    .iter()
                    .map(|(name, s)| (name.as_str(), s.as_str()))
                    .collect::<Vec<_>>(),
            )?),
        };
        remaining()?;
        ensure!(
            Instant::now() <= deadline,
            "Registry resolution exceeded two seconds"
        );
        Ok(decoder)
    }

    pub fn preview(
        &mut self,
        raw: &[u8],
        remaining: &impl Fn() -> Result<()>,
    ) -> (Option<String>, Result<Preview>) {
        let envelope = || -> Result<(u32, &[u8])> {
            ensure!(raw.len() >= 5, "Truncated Confluent payload prefix");
            ensure!(raw[0] == 0, "Unsupported Confluent version byte");
            ensure!(
                raw.len() - 5 <= onetui_avro::MAX_PAYLOAD_BYTES,
                "Message exceeds 64 KiB decode limit; inspect raw bytes"
            );
            let id = u32::from_be_bytes(raw[1..5].try_into().unwrap());
            ensure!(
                id > 0 && id <= i32::MAX as u32,
                "Invalid Confluent schema ID"
            );
            Ok((id, &raw[5..]))
        };
        let (id, mut payload) = match envelope() {
            Ok(value) => value,
            Err(error) => return (None, Err(error)),
        };
        let mut identity = Some(format!("confluent:{}#id={id}", self.config.url));
        if let Some(reader) = &self.reader {
            identity
                .as_mut()
                .unwrap()
                .push_str(&format!("&reader={}", reader.schema_id()));
        }
        let indexes = if self.format == Format::Protobuf {
            match message_indexes(&mut payload) {
                Ok(indexes) => indexes,
                Err(error) => return (identity, Err(error)),
            }
        } else {
            Vec::new()
        };
        if !self.cache.iter().any(|(cached, _)| *cached == id) {
            let loaded = self.load(id, remaining).map_err(|error| {
                onetui_core::diagnostic(
                    error,
                    &self
                        .config
                        .secrets
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                )
                .to_string()
            });
            if let Err(error) = remaining() {
                return (identity, Err(error));
            }
            if self.cache.len() == CACHE_ENTRIES {
                self.cache.pop_front();
            }
            self.cache.push_back((id, loaded));
        }
        let decoder = &self
            .cache
            .iter()
            .find(|(cached, _)| *cached == id)
            .unwrap()
            .1;
        let result = decoder
            .as_ref()
            .map_err(|error| anyhow!("{error}"))
            .and_then(|decoder| match decoder {
                Compiled::Avro(decoder) => Preview::avro(decoder, payload),
                Compiled::Protobuf(schema) => {
                    let decoder = schema.decoder(&indexes)?;
                    let message = decoder.message_name();
                    identity
                        .as_mut()
                        .unwrap()
                        .push_str(&format!("&message={message}"));
                    Preview::protobuf(&decoder, payload)
                }
            });
        (identity, result)
    }
}

// Confluent uses nonnegative zigzag varints, with [0] abbreviated to one zero.
fn message_indexes(input: &mut &[u8]) -> Result<Vec<usize>> {
    fn number(input: &mut &[u8]) -> Result<usize> {
        let mut value = 0u32;
        for shift in (0..35).step_by(7) {
            let (&byte, tail) = input
                .split_first()
                .ok_or_else(|| anyhow!("Truncated Protobuf message indexes"))?;
            *input = tail;
            ensure!(shift < 28 || byte <= 15, "Protobuf message index overflow");
            value |= u32::from(byte & 127) << shift;
            if byte & 128 == 0 {
                ensure!(value & 1 == 0, "Negative Protobuf message index");
                return Ok((value >> 1) as usize);
            }
        }
        anyhow::bail!("Protobuf message index overflow")
    }
    let count = number(input)?;
    if count == 0 {
        return Ok(vec![0]);
    }
    ensure!(
        count <= onetui_protobuf::MAX_DEPTH,
        "Protobuf message index depth exceeds 32"
    );
    (0..count).map(|_| number(input)).collect()
}

#[cfg(test)]
#[path = "registry/tests.rs"]
mod tests;
