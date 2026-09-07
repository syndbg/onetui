use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow, bail, ensure};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    connections: BTreeMap<String, Connection>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum Connection {
    Postgres {
        url_env: String,
        ca_file: Option<PathBuf>,
    },
    Qdrant {
        url: String,
        api_key_env: Option<String>,
    },
}

// Debug output would expose resolved credentials.
pub enum ResolvedConnection {
    Postgres {
        url: String,
        ca_file: Option<PathBuf>,
    },
    Qdrant {
        url: String,
        api_key: Option<String>,
    },
}

impl ResolvedConnection {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Postgres { .. } => "postgres",
            Self::Qdrant { .. } => "qdrant",
        }
    }
}

impl Config {
    pub fn aliases(&self) -> Vec<(&str, &'static str)> {
        self.connections
            .iter()
            .map(|(alias, connection)| {
                (
                    alias.as_str(),
                    match connection {
                        Connection::Postgres { .. } => "postgres",
                        Connection::Qdrant { .. } => "qdrant",
                    },
                )
            })
            .collect()
    }

    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|_| anyhow!("cannot read config file; check --config and file permissions"))?;
        Self::parse(&text)
    }

    fn parse(text: &str) -> Result<Self> {
        toml::from_str(text).map_err(|_| {
            anyhow!("invalid config: check TOML syntax, datasource kinds and supported fields; see --help and README")
        })
    }

    pub fn resolve(
        &self,
        alias: &str,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<ResolvedConnection> {
        ensure!(
            safe_name(alias),
            "connection alias must contain only ASCII letters, digits, underscores or hyphens"
        );
        let connection = self.connections.get(alias).ok_or_else(|| {
            anyhow!("unknown connection alias; check the config connections table")
        })?;
        let secret = |name: &str| -> Result<String> {
            ensure!(
                safe_name(name),
                "environment reference contains unsupported characters"
            );
            env(name)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    anyhow!(
                        "referenced environment variable {name} is missing, empty or not Unicode"
                    )
                })
        };
        match connection {
            Connection::Postgres { url_env, ca_file } => {
                ensure!(
                    ca_file.as_ref().is_none_or(|p| p.is_absolute()),
                    "ca_file must be an absolute path"
                );
                Ok(ResolvedConnection::Postgres {
                    url: secret(url_env)?,
                    ca_file: ca_file.clone(),
                })
            }
            Connection::Qdrant { url, api_key_env } => Ok(ResolvedConnection::Qdrant {
                url: url.clone(),
                api_key: api_key_env.as_deref().map(secret).transpose()?,
            }),
        }
    }
}

fn safe_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
}

pub fn config_path(explicit: Option<PathBuf>) -> Result<PathBuf> {
    default_path(
        explicit,
        std::env::var_os("XDG_CONFIG_HOME"),
        std::env::var_os("HOME"),
    )
}

fn default_path(
    explicit: Option<PathBuf>,
    xdg: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    if let Some(xdg) = xdg.filter(|p| Path::new(p).is_absolute()) {
        return Ok(PathBuf::from(xdg).join("onetui/config.toml"));
    }
    if let Some(home) = home.filter(|p| Path::new(p).is_absolute()) {
        return Ok(PathBuf::from(home).join(".config/onetui/config.toml"));
    }
    bail!("cannot determine configuration directory; pass --config <path>")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_only_selected_connection_and_rejects_empty_secrets() {
        let config = Config::parse("[connections.pg]\nkind='postgres'\nurl_env='PG_DSN'\n[connections.q]\nkind='qdrant'\nurl='http://127.0.0.1:6334'\n").unwrap();
        assert!(
            config
                .resolve("q", |_| panic!("unused secret resolved"))
                .is_ok()
        );
        assert!(config.resolve("pg", |_| Some("  ".into())).is_err());
        assert!(config.resolve("missing", |_| None).is_err());
        assert!(config.resolve("bad\x1b", |_| None).is_err());
    }

    #[test]
    fn parser_errors_never_echo_config() {
        for input in [
            "password='fake-super-secret'",
            "[connections.q]\nkind='qdrant'\nurl='http://localhost'\napi_key='fake-super-secret'",
            "[connections.q]\nkind='fake-super-secret'",
        ] {
            let error = Config::parse(input).err().unwrap().to_string();
            assert!(!error.contains("fake-super-secret"));
        }
    }

    #[test]
    fn configuration_path_precedence() {
        let home = Some("/example".into());
        assert_eq!(
            default_path(Some("local.toml".into()), None, None).unwrap(),
            PathBuf::from("local.toml")
        );
        assert_eq!(
            default_path(None, Some("/xdg".into()), home.clone()).unwrap(),
            PathBuf::from("/xdg/onetui/config.toml")
        );
        assert_eq!(
            default_path(None, Some("relative".into()), home).unwrap(),
            PathBuf::from("/example/.config/onetui/config.toml")
        );
        assert!(default_path(None, None, None).is_err());
    }
}
