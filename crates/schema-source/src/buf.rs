use anyhow::{Result, ensure};
use serde::Deserialize;
use std::time::{Duration, Instant};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub url: String,
    pub module: String,
    pub revision: Option<String>,
    pub label: Option<String>,
    pub ca_file: Option<String>,
    pub token_env: Option<String>,
    #[serde(skip)]
    remote: Option<crate::registry::Config>,
    #[serde(skip)]
    resolved_revision: Option<String>,
}

fn commit_id(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl Config {
    fn remote(&self) -> crate::registry::Config {
        crate::registry::Config::bearer(
            self.url.clone(),
            self.ca_file.clone(),
            self.token_env.clone(),
        )
    }

    pub fn validate(&self) -> Result<()> {
        self.remote().validate()?;
        let url = url::Url::parse(&self.url)?;
        ensure!(
            url.path() == "/",
            "Buf URL must be an origin without a base path"
        );
        let parts: Vec<_> = self.module.split('/').collect();
        ensure!(
            parts.len() == 2
                && parts.iter().all(|part| {
                    !part.is_empty()
                        && part.len() <= 100
                        && part
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                        && part.as_bytes()[0].is_ascii_alphanumeric()
                        && part.as_bytes()[part.len() - 1].is_ascii_alphanumeric()
                }),
            "Buf module must be owner/module using lowercase letters, digits and hyphens"
        );
        ensure!(
            self.revision.is_some() != self.label.is_some(),
            "Buf requires exactly one of revision or label"
        );
        if let Some(revision) = &self.revision {
            ensure!(
                commit_id(revision),
                "Buf revision must be a pinned 32-character lowercase hex commit ID; use label for moving references"
            );
        }
        if let Some(label) = &self.label {
            ensure!(
                !label.is_empty()
                    && label.len() <= 250
                    && label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._/-".contains(&b)),
                "Buf label must be 1..250 ASCII letters/digits/dot/underscore/slash/hyphen"
            );
        }
        Ok(())
    }

    pub fn resolve(&mut self, env: &dyn Fn(&str) -> Option<String>) -> Result<Vec<String>> {
        let mut remote = self.remote();
        let secrets = remote.resolve(env)?;
        self.remote = Some(remote);
        Ok(secrets)
    }

    pub fn identity(&self) -> String {
        let reference = match self.resolved_revision.as_ref().or(self.revision.as_ref()) {
            Some(revision) => format!("commit={revision}"),
            None => format!("label={}", self.label.as_deref().unwrap_or_default()),
        };
        format!(
            "buf:{}/{}#{}",
            self.url.trim_end_matches('/'),
            self.module,
            reference
        )
    }

    pub fn diagnostic(&self, error: anyhow::Error) -> String {
        match &self.remote {
            Some(remote) => remote.diagnostic(error),
            None => format!("{error:#}"),
        }
    }

    pub fn load(&mut self, remaining: &impl Fn() -> Result<()>) -> Result<Vec<u8>> {
        remaining()?;
        self.validate()?;
        let anonymous = self.remote();
        ensure!(
            self.remote.is_some() || self.token_env.is_none(),
            "Buf token has not been resolved"
        );
        let remote = self.remote.as_ref().unwrap_or(&anonymous);
        let result = (|| {
            let agent = remote.agent()?;
            let (owner, module) = self.module.split_once('/').unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            let revision = if let Some(revision) = &self.revision {
                revision.clone()
            } else {
                let body = serde_json::json!({"resourceRefs":[{"name":{"owner":owner,"module":module,"labelName":self.label.as_deref().unwrap()}}]});
                let bytes = remote.request(
                    &agent,
                    &["buf.registry.module.v1.CommitService", "GetCommits"],
                    Some(&body),
                    deadline,
                    remaining,
                )?;
                #[derive(Deserialize)]
                struct Commit {
                    id: String,
                }
                #[derive(Deserialize)]
                struct Commits {
                    commits: Vec<Commit>,
                }
                let response: Commits = serde_json::from_slice(&bytes)?;
                ensure!(
                    response.commits.len() == 1 && commit_id(&response.commits[0].id),
                    "Buf label response must contain exactly one valid commit ID"
                );
                response.commits.into_iter().next().unwrap().id
            };
            // Fetch by the resolved commit even if the label moves between requests.
            self.resolved_revision = Some(revision.clone());
            remote.request(
                &agent,
                &[owner, module, "descriptor", &revision],
                None,
                deadline,
                remaining,
            )
        })();
        result.map_err(|error| anyhow::anyhow!("{}", remote.diagnostic(error)))
    }
}

pub fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "required": "raw protobuf only; mutually exclusive with schema_file, catalog and registry",
        "type": "table", "default": "none; no Buf requests",
        "fields": {
            "url": {"required": true, "type": "HTTP(S) origin, at most 512 UTF-8 bytes", "example": "https://buf.build", "purpose": "HTTPS remotely; HTTP only for literal loopback. No userinfo, base path, query, fragment, redirects or environment proxies."},
            "module": {"required": true, "type": "owner/module; each component 1..100 lowercase ASCII letters/digits/hyphens, alphanumeric ends"},
            "revision": {"required": "unless label is set; mutually exclusive with label", "type": "32 lowercase hex characters", "purpose": "Exact immutable commit ID."},
            "label": {"required": "unless revision is set; mutually exclusive with revision", "type": "1..250 ASCII letters/digits/dot/underscore/slash/hyphen", "purpose": "Explicit label resolved once through GetCommits, then descriptors fetched by that commit. No implicit latest/default label. Reconnect to resolve again."},
            "token_env": {"required": false, "default": "anonymous", "type": "ASCII environment-variable name", "purpose": "Buf Bearer token, independent of datasource credentials and other bindings; 1..4096 UTF-8 bytes without controls, resolved only for the selected alias."},
            "ca_file": {"required": false, "default": "platform TLS verification", "type": "absolute regular-file PEM path, at most 4096 UTF-8 bytes without controls", "purpose": "HTTPS-only trust bundle, at most 1 MiB; replaces system trust. No path expansion."}
        },
        "limits": {"response_bytes": 262144, "header_bytes": 16384, "request_timeout_ms": 2000, "cached_schemas_per_binding": 1},
        "behavior": "Fetch a binary FileDescriptorSet with imports on the native worker; message_name selects the exact message. --check resolves and validates it. Label resolution and descriptor fetching share a two-second I/O deadline; each response is bounded. Successes and failures stay cached until reconnect. Source, resolved commit and descriptor fingerprint accompany decoded values. Raw bytes survive failures. Foreground cancellation/deadline checked between requests and before returning; native request may finish later. No external buf/protoc executable."
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_server::Server;

    #[test]
    fn label_resolution_fetches_the_exact_commit_and_checks_cancellation() {
        const COMMIT: &str = "0123456789abcdef0123456789abcdef";
        let server = Server::start_bytes(false, |request| {
            match request.split_whitespace().nth(1).unwrap() {
                "/buf.registry.module.v1.CommitService/GetCommits" => {
                    assert!(request.contains(r#""labelName":"main""#));
                    (
                        200,
                        serde_json::json!({"commits":[{"id":COMMIT}]})
                            .to_string()
                            .into_bytes(),
                    )
                }
                path => {
                    assert_eq!(path, format!("/demo/events/descriptor/{COMMIT}"));
                    (200, vec![10, 0])
                }
            }
        });
        let mut config: Config = serde_json::from_value(
            serde_json::json!({"url":server.url,"module":"demo/events","label":"main"}),
        )
        .unwrap();
        config.validate().unwrap();
        assert!(server.requests.lock().unwrap().is_empty());
        assert!(config.load(&|| anyhow::bail!("cancelled")).is_err());
        assert!(server.requests.lock().unwrap().is_empty());
        assert_eq!(config.load(&|| Ok(())).unwrap(), [10, 0]);
        assert!(config.identity().ends_with(&format!("#commit={COMMIT}")));
        assert_eq!(server.requests.lock().unwrap().len(), 2);
    }
}
