use anyhow::{Result, anyhow, ensure};
use base64::Engine;
use onetui_core::config::{safe_name, secret};
use serde::Deserialize;
use std::{
    collections::{HashSet, VecDeque},
    io::Read,
    time::{Duration, Instant},
};

const RESPONSE_BYTES: usize = 256 * 1024;
const CACHE_ENTRIES: usize = 8;

pub(crate) fn capabilities() -> serde_json::Value {
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
        "limits": {"resolution_timeout_ms": 2000, "response_bytes": 262144, "header_bytes": 16384, "bundle_bytes": 262144, "references": 32, "reference_depth": 8, "cached_ids_per_binding": 8},
        "behavior": "Read-only exact-ID and versioned-reference requests. FIFO cache includes lookup failures; eviction or reopening permits a new lookup. Two-second bound per uncached schema and its references, with foreground deadline/cancellation checks between requests. Native cleanup may outlive foreground cancellation. No RSS guarantee. --check requests schemas/types, not every record schema."
    })
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
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
            super::decoding::validate_path(path)?;
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

pub(crate) struct Registry {
    config: Config,
    agent: ureq::Agent,
    cache: VecDeque<(u32, Result<onetui_avro::Decoder, String>)>,
}

impl Registry {
    pub fn new(config: Config) -> Result<Self> {
        let roots = if let Some(path) = &config.ca_file {
            let pem = super::decoding::read_file(path, 1024 * 1024)?;
            let mut certs = Vec::new();
            for item in ureq::tls::parse_pem(&pem) {
                if let ureq::tls::PemItem::Certificate(cert) = item? {
                    certs.push(cert);
                }
            }
            ensure!(
                !certs.is_empty(),
                "Registry ca_file contains no certificates"
            );
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
        Ok(Self {
            config,
            agent,
            cache: VecDeque::new(),
        })
    }

    fn request(
        &self,
        segments: &[&str],
        deadline: Instant,
        remaining: &impl Fn() -> Result<()>,
    ) -> Result<Vec<u8>> {
        remaining()?;
        let timeout = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| anyhow!("Registry resolution exceeded two seconds"))?;
        let mut url = url::Url::parse(&self.config.url)?;
        url.path_segments_mut()
            .map_err(|_| anyhow!("Invalid registry base URL"))?
            .pop_if_empty()
            .extend(segments);
        let mut request = self
            .agent
            .get(url.as_str())
            .config()
            .timeout_global(Some(timeout))
            .build();
        if let Some(auth) = &self.config.authorization {
            request = request.header("Authorization", auth);
        }
        let mut response = request.call()?;
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

    pub fn check(&self, remaining: &impl Fn() -> Result<()>) -> Result<()> {
        let bytes = self.request(
            &["schemas", "types"],
            Instant::now() + Duration::from_secs(2),
            remaining,
        )?;
        let types: Vec<String> = serde_json::from_slice(&bytes)?;
        ensure!(
            types.iter().any(|t| t == "AVRO"),
            "Registry does not advertise AVRO"
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
            schema.kind.as_deref().is_none_or(|k| k == "AVRO"),
            "Registry schema is not AVRO"
        );
        ensure!(
            schema.references.len() <= 32,
            "Registry references exceed 32"
        );
        Ok(schema)
    }

    fn load(&self, id: u32, remaining: &impl Fn() -> Result<()>) -> Result<onetui_avro::Decoder> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let root = self.get(&["schemas", "ids", &id.to_string()], deadline, remaining)?;
        let mut pending = root
            .references
            .into_iter()
            .map(|r| (r, 1))
            .collect::<Vec<_>>();
        let mut seen = HashSet::new();
        let mut names = HashSet::new();
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
            if seen.contains(&key) {
                continue;
            }
            ensure!(
                seen.len() < 32 && names.insert(reference.name),
                "Too many or conflicting registry references"
            );
            seen.insert(key.clone());
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
            references.push(schema.schema);
        }
        remaining()?;
        let decoder = onetui_avro::Decoder::with_references(
            &root.schema,
            &references.iter().map(String::as_str).collect::<Vec<_>>(),
        )?;
        remaining()?;
        Ok(decoder)
    }

    pub fn preview(
        &mut self,
        raw: &[u8],
        remaining: &impl Fn() -> Result<()>,
    ) -> (Option<String>, Result<String>) {
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
        let (id, payload) = match envelope() {
            Ok(value) => value,
            Err(error) => return (None, Err(error)),
        };
        let identity = Some(format!("confluent:{}#id={id}", self.config.url));
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
        (
            identity,
            decoder
                .as_ref()
                .map_err(|error| anyhow!("{error}"))
                .and_then(|decoder| decoder.decode(payload)?.json()),
        )
    }
}

#[cfg(test)]
#[path = "registry/tests.rs"]
mod tests;
