use anyhow::{Result, anyhow, ensure};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use onetui_core::config::{safe_name, secret};
use rdkafka::client::OAuthToken;
use serde::Deserialize;
use std::{
    io::Read,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;

const RESPONSE_BYTES: usize = 64 * 1024;

pub(crate) fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "required": "with OAUTHBEARER; forbidden otherwise", "type": "table", "default": null,
        "purpose": "OAuth client-credentials grant over verified HTTPS; signed JWT bearer tokens only. Broker verifies signatures, issuer, audience and ACLs.",
        "fields": {
            "token_url": {"required": true, "type": "URL, max 512 bytes", "purpose": "Explicit token endpoint; no userinfo, query, fragment or controls. HTTP allowed only for literal loopback IPs. No redirects or proxy use."},
            "client_id_env": {"required": true, "type": "ASCII environment-variable name", "purpose": "Client ID; resolved only for selected alias, max 4096 bytes without controls"},
            "client_secret_env": {"required": true, "type": "ASCII environment-variable name", "purpose": "Client secret; resolved only for selected alias, max 4096 bytes without controls; sent using HTTP Basic with form-encoded credentials"},
            "scope": {"required": false, "default": null, "type": "1..2048 printable OAuth scope bytes, space-separated", "purpose": "Requested scopes; omitted when absent"},
            "ca_file": {"required": false, "default": "platform trust", "type": "absolute PEM path, max 4096 bytes; file at most 1 MiB", "purpose": "Token-endpoint CA bundle, separate from broker ca_file; HTTPS only"}
        },
        "limits": {"request_timeout_ms": 2000, "response_bytes": RESPONSE_BYTES, "response_header_bytes": 16384},
        "lifecycle": "Offline validation; credentials resolved on alias selection, HTTP starts on native polling. librdkafka schedules refresh during active requests; no idle HTTP. Idle expiry reconnects on the next read, preserving logical bookmarks. Earlier request deadlines apply. Foreground cancellation returns immediately; native cleanup retains its owner until bounded HTTP completes. Reopen to change credentials.",
        "tokens": "JWT sub and exp required; optional expires_in can only shorten lifetime. Tokens and credentials are redacted; errors retain bounded server details. No unsigned tokens, opaque tokens, discovery, interactive grants or SASL extensions."
    })
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub token_url: String,
    pub client_id_env: String,
    pub client_secret_env: String,
    pub scope: Option<String>,
    pub ca_file: Option<String>,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        let url =
            url::Url::parse(&self.token_url).map_err(|_| anyhow!("Invalid OAuth token_url"))?;
        ensure!(
            self.token_url.len() <= 512
                && !self.token_url.chars().any(char::is_control)
                && matches!(url.scheme(), "https" | "http")
                && url.host().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "Invalid OAuth token_url"
        );
        if url.scheme() == "http" {
            ensure!(
                url.host_str().is_some_and(|host| host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())),
                "HTTP OAuth requires a literal loopback address"
            );
            ensure!(self.ca_file.is_none(), "OAuth ca_file requires HTTPS");
        }
        ensure!(
            safe_name(&self.client_id_env) && safe_name(&self.client_secret_env),
            "Invalid OAuth secret reference"
        );
        if let Some(scope) = &self.scope {
            ensure!(!scope.is_empty() && scope.len() <= 2048 && scope.bytes().all(|b| b == b' ' || (b'!'..=b'~').contains(&b) && b != b'"' && b != b'\\'), "Invalid OAuth scope");
        }
        if let Some(path) = &self.ca_file {
            crate::decoding::validate_path(path)?;
        }
        Ok(())
    }

    pub fn resolve(&self, env: &dyn Fn(&str) -> Option<String>) -> Result<Session> {
        let id = secret(&self.client_id_env, env)?;
        let secret = secret(&self.client_secret_env, env)?;
        ensure!(
            [&id, &secret]
                .into_iter()
                .all(|s| s.len() <= 4096 && !s.chars().any(char::is_control)),
            "Invalid OAuth client credentials"
        );
        let encode =
            |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>();
        let (encoded_id, encoded_secret) = (encode(&id), encode(&secret));
        let encoded = STANDARD.encode(format!("{encoded_id}:{encoded_secret}"));
        let authorization = format!("Basic {encoded}");
        Ok(Session {
            config: self.clone(),
            agent: None,
            secrets: vec![
                id,
                secret,
                encoded_id,
                encoded_secret,
                encoded,
                authorization.clone(),
            ],
            authorization,
            tokens: std::collections::VecDeque::new(),
            expires_at: None,
        })
    }
}

pub(crate) struct Session {
    config: Config,
    agent: Option<ureq::Agent>,
    authorization: String,
    secrets: Vec<String>,
    tokens: std::collections::VecDeque<String>,
    expires_at: Option<i64>,
}

impl Session {
    pub fn expired(&self) -> bool {
        self.expires_at.is_some_and(|expires| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(true, |now| now.as_millis() >= expires as u128)
        })
    }

    pub fn token(&mut self, deadline: Instant) -> Result<OAuthToken> {
        ensure!(Instant::now() < deadline, "OAuth request timed out");
        if self.agent.is_none() {
            self.agent = Some(crate::registry::http_agent(self.config.ca_file.as_deref())?);
        }
        let timeout = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| anyhow!("OAuth request timed out"))?
            .min(Duration::from_secs(2));
        let mut form = url::form_urlencoded::Serializer::new(String::new());
        form.append_pair("grant_type", "client_credentials");
        if let Some(scope) = &self.config.scope {
            form.append_pair("scope", scope);
        }
        let mut response = self
            .agent
            .as_ref()
            .unwrap()
            .post(&self.config.token_url)
            .config()
            .timeout_global(Some(timeout))
            .build()
            .header("Authorization", &self.authorization)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .send(form.finish())?;
        let status = response.status();
        let mut body = Vec::new();
        response
            .body_mut()
            .as_reader()
            .take(RESPONSE_BYTES as u64 + 1)
            .read_to_end(&mut body)?;
        ensure!(
            body.len() <= RESPONSE_BYTES,
            "OAuth response exceeds 64 KiB"
        );
        // A server can echo an access token even in an error response.
        let json = serde_json::from_slice::<serde_json::Value>(&body);
        if let Some(token) = json
            .as_ref()
            .ok()
            .and_then(|v| v.get("access_token"))
            .and_then(|v| v.as_str())
            && !token.is_empty()
        {
            if self.tokens.len() == 2 {
                self.tokens.pop_front();
            }
            self.tokens.push_back(token.to_owned());
        }
        ensure!(
            status.is_success(),
            "OAuth HTTP {status}: {}",
            String::from_utf8_lossy(&body)
        );
        let json = json.map_err(|_| anyhow!("Invalid OAuth JSON response (HTTP {status})"))?;
        ensure!(Instant::now() < deadline, "OAuth request timed out");
        let token = json
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("OAuth response has no access_token"))?;
        ensure!(
            json.get("token_type")
                .and_then(|v| v.as_str())
                .is_some_and(|v| v.eq_ignore_ascii_case("bearer")),
            "OAuth token_type must be Bearer"
        );
        let mut parts = token.split('.');
        let (Some(header), Some(claims), Some(signature), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(anyhow!("OAuth requires a signed JWT access token"));
        };
        ensure!(
            !signature.is_empty() && URL_SAFE_NO_PAD.decode(signature).is_ok(),
            "Invalid OAuth JWT signature encoding"
        );
        let decode = |part| -> Result<serde_json::Value> {
            let bytes = URL_SAFE_NO_PAD
                .decode(part)
                .map_err(|_| anyhow!("Invalid OAuth JWT encoding"))?;
            serde_json::from_slice(&bytes).map_err(|_| anyhow!("Invalid OAuth JWT JSON"))
        };
        let header = decode(header)?;
        ensure!(
            header
                .get("alg")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.is_empty() && !s.eq_ignore_ascii_case("none")),
            "Unsigned OAuth JWT is not supported"
        );
        let claims = decode(claims)?;
        let principal = claims
            .get("sub")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("OAuth JWT has no sub"))?;
        ensure!(
            !principal.is_empty()
                && principal.len() <= 1024
                && !principal.chars().any(char::is_control),
            "Invalid OAuth JWT sub"
        );
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let mut expires = claims
            .get("exp")
            .and_then(|v| v.as_u64())
            .and_then(|s| s.checked_mul(1000))
            .ok_or_else(|| anyhow!("Invalid OAuth JWT exp"))?;
        if let Some(ttl) = json.get("expires_in") {
            let ttl = ttl
                .as_u64()
                .and_then(|s| s.checked_mul(1000))
                .ok_or_else(|| anyhow!("Invalid OAuth expires_in"))?;
            expires = expires.min(
                u64::try_from(now)?
                    .checked_add(ttl)
                    .ok_or_else(|| anyhow!("OAuth expiry overflow"))?,
            );
        }
        ensure!(u128::from(expires) > now, "OAuth access token is expired");
        let lifetime_ms = expires.try_into()?;
        self.expires_at = Some(lifetime_ms);
        Ok(OAuthToken {
            token: token.into(),
            principal_name: principal.into(),
            lifetime_ms,
        })
    }

    pub fn diagnostic(&self, error: anyhow::Error) -> anyhow::Error {
        let mut secrets = self
            .secrets
            .iter()
            .chain(self.tokens.iter())
            .flat_map(|secret| {
                let quoted = serde_json::to_string(secret).unwrap();
                [secret.clone(), quoted[1..quoted.len() - 1].to_owned()]
            })
            .collect::<Vec<_>>();
        // Replace complete tokens before any shorter credential that overlaps them.
        secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
        onetui_core::diagnostic(
            error,
            &secrets.iter().map(String::as_str).collect::<Vec<_>>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_server::Server;
    use serde_json::json;

    fn config(server: &Server) -> Config {
        Config {
            token_url: format!("{}/token", server.url),
            client_id_env: "CLIENT_ID".into(),
            client_secret_env: "CLIENT_SECRET".into(),
            scope: Some("read topic:events".into()),
            ca_file: server.ca_file.clone(),
        }
    }

    fn session(config: &Config) -> Session {
        config.validate().unwrap();
        config
            .resolve(&|name| {
                Some(
                    match name {
                        "CLIENT_ID" => "client:id",
                        "CLIENT_SECRET" => "secret&value",
                        _ => panic!("unrelated secret resolved"),
                    }
                    .into(),
                )
            })
            .unwrap()
    }

    fn token(expiry: u64) -> String {
        format!(
            "{}.{}.c2lnbmF0dXJl",
            URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256"}"#),
            URL_SAFE_NO_PAD.encode(json!({"sub":"fixture-reader", "exp":expiry}).to_string())
        )
    }

    #[test]
    fn strict_offline_configuration_and_secret_isolation() {
        let base = "bootstrap_servers=['broker:9093']\nsecurity_protocol='SASL_SSL'\nsasl_mechanism='OAUTHBEARER'\n";
        let oauth = "[oauth]\ntoken_url='https://idp.example/token'\nclient_id_env='CLIENT_ID'\nclient_secret_env='CLIENT_SECRET'\n";
        let parsed =
            crate::config::Config::parse(&toml::from_str(&format!("{base}{oauth}")).unwrap())
                .unwrap();
        let (native, _) = parsed
            .native(1, &|_| panic!("OAuth secrets resolved separately"))
            .unwrap();
        assert_eq!(native.get("sasl.mechanism"), Some("OAUTHBEARER"));
        assert_eq!(
            native.get("enable.sasl.oauthbearer.unsecure.jwt"),
            Some("false")
        );
        assert_eq!(native.get("sasl.password"), None);
        assert!(parsed.oauth.as_ref().unwrap().resolve(&|_| None).is_err());
        assert!(
            parsed
                .oauth
                .as_ref()
                .unwrap()
                .resolve(&|_| Some("bad\0secret".into()))
                .is_err()
        );
        for text in [
            base.to_string(),
            format!("{base}username_env='USER'\n{oauth}"),
            format!("{base}{oauth}unknown=true"),
            format!("{base}{oauth}").replace("SASL_SSL", "SSL"),
            format!("{base}{oauth}").replace("OAUTHBEARER", "PLAIN"),
        ] {
            assert!(crate::config::Config::parse(&toml::from_str(&text).unwrap()).is_err());
        }
        for url in [
            "http://remote/token",
            "http://localhost/token",
            "https://user:secret@host/token",
            "https://host/token?secret=x",
            "https://host/token#x",
            "file:///token",
        ] {
            let text = format!("{base}{oauth}").replace("https://idp.example/token", url);
            assert!(
                crate::config::Config::parse(&toml::from_str(&text).unwrap()).is_err(),
                "{url}"
            );
        }
    }

    #[test]
    fn verified_tls_client_credentials_and_refresh() {
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300;
        let jwt = token(expiry);
        let response =
            json!({"access_token":jwt,"token_type":"Bearer","expires_in":30}).to_string();
        let server = Server::start(true, move |_| (200, response.clone()));
        let mut config = config(&server);
        let mut current = session(&config);
        assert!(!current.expired());
        for _ in 0..2 {
            let token = current
                .token(Instant::now() + Duration::from_secs(2))
                .unwrap();
            assert_eq!(token.principal_name, "fixture-reader");
            assert!(token.lifetime_ms < (expiry * 1000) as i64);
            assert!(!current.expired());
        }
        current.expires_at = Some(1);
        assert!(current.expired());
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let auth = STANDARD.encode("client%3Aid:secret%26value");
        for request in requests.iter() {
            assert!(request.starts_with("POST /token HTTP/1.1"));
            assert!(request.contains(&format!("Basic {auth}")));
            assert!(request.ends_with("grant_type=client_credentials&scope=read+topic%3Aevents"));
        }
        drop(requests);
        config.ca_file = None;
        assert!(
            session(&config)
                .token(Instant::now() + Duration::from_secs(2))
                .is_err()
        );
    }

    #[test]
    fn errors_preserve_server_details_but_redact_credentials_and_tokens() {
        let server = Server::start(false, |_| {
            (401, "invalid_client: secret&value\u{1b}[31m".into())
        });
        let mut current = session(&config(&server));
        let error = current
            .token(Instant::now() + Duration::from_secs(2))
            .err()
            .unwrap();
        let error = current.diagnostic(error).to_string();
        assert!(error.contains("401") && error.contains("invalid_client"));
        assert!(!error.contains("secret&value") && !error.contains('\u{1b}'));
        let jwt = token(u64::MAX);
        let body = json!({"access_token":jwt,"error":"invalid_grant"}).to_string();
        let server = Server::start(false, move |_| (400, body.clone()));
        let mut current = session(&config(&server));
        let error = current
            .token(Instant::now() + Duration::from_secs(2))
            .err()
            .unwrap();
        let error = current.diagnostic(error).to_string();
        assert!(error.contains("invalid_grant") && !error.contains(&jwt));
        for secret in current.secrets.clone() {
            assert!(
                !current
                    .diagnostic(anyhow!("echo: {secret}"))
                    .to_string()
                    .contains(&secret)
            );
        }
        let current = config(&server)
            .resolve(&|_| Some("quote\"secret".into()))
            .unwrap();
        let echoed = json!({"error_description":"quote\"secret"}).to_string();
        assert!(
            !current
                .diagnostic(anyhow!(echoed))
                .to_string()
                .contains("secret")
        );
    }

    #[test]
    fn malformed_expired_and_unsigned_tokens_fail_closed() {
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300;
        let valid = token(expiry);
        let unsigned = format!(
            "{}.{}.c2ln",
            URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#),
            URL_SAFE_NO_PAD.encode(r#"{"sub":"reader","exp":9999999999}"#)
        );
        for response in [
            json!({}),
            json!({"access_token":valid,"token_type":"Basic"}),
            json!({"access_token":"opaque","token_type":"Bearer"}),
            json!({"access_token":token(1),"token_type":"Bearer"}),
            json!({"access_token":token(u64::MAX),"token_type":"Bearer"}),
            json!({"access_token":unsigned,"token_type":"Bearer"}),
            json!({"access_token":format!("{valid}\0"),"token_type":"Bearer"}),
            json!({"access_token":valid,"token_type":"Bearer","expires_in":0}),
            json!({"access_token":valid,"token_type":"Bearer","expires_in":-1}),
        ] {
            let server = Server::start(false, move |_| (200, response.to_string()));
            assert!(
                session(&config(&server))
                    .token(Instant::now() + Duration::from_secs(2))
                    .is_err()
            );
        }
    }

    #[test]
    fn bounded_http_no_redirects_and_deadlines() {
        for (status, body, expected) in [
            (200, "x".repeat(RESPONSE_BYTES + 1), "64 KiB"),
            (200, "not JSON".into(), "Invalid OAuth JSON"),
            (302, "redirect forbidden".into(), "302"),
        ] {
            let server = Server::start(false, move |_| (status, body.clone()));
            let error = session(&config(&server))
                .token(Instant::now() + Duration::from_secs(2))
                .err()
                .unwrap();
            assert!(error.to_string().contains(expected), "{error}");
            assert_eq!(server.requests.lock().unwrap().len(), 1);
        }
        let server = Server::start(false, |_| {
            std::thread::sleep(Duration::from_millis(300));
            (200, "{}".into())
        });
        let mut current = session(&config(&server));
        assert!(current.token(Instant::now()).is_err());
        assert!(server.requests.lock().unwrap().is_empty());
        let started = Instant::now();
        assert!(current.token(started + Duration::from_millis(80)).is_err());
        assert!(started.elapsed() < Duration::from_millis(250));
    }
}
