use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::provider::{Provider, ProviderDescriptor, find_provider, validate_catalog};
use anyhow::{Result, anyhow, bail, ensure};
use onetui_theme::Theme;
use serde::Deserialize;

pub struct Config {
    pub theme: Theme,
    connections: BTreeMap<String, Connection>,
}

struct Connection {
    descriptor: &'static ProviderDescriptor,
    options: toml::Table,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    theme: Theme,
    connections: BTreeMap<String, toml::Table>,
}

impl Config {
    pub fn aliases(&self) -> Vec<(&str, &'static str)> {
        self.connections
            .iter()
            .map(|(alias, connection)| (alias.as_str(), connection.descriptor.kind))
            .collect()
    }

    pub fn descriptor(&self, alias: &str) -> Option<&'static ProviderDescriptor> {
        self.connections.get(alias).map(|c| c.descriptor)
    }

    pub fn load<P: Provider>(path: &Path, catalog: &[P]) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|_| anyhow!("cannot read config file; check --config and file permissions"))?;
        Self::parse(&text, catalog)
    }

    pub fn parse<P: Provider>(text: &str, catalog: &[P]) -> Result<Self> {
        validate_catalog(catalog)?;
        let raw: RawConfig = toml::from_str(text).map_err(|_| {
            anyhow!("invalid config: check TOML syntax and supported fields; see --help and README")
        })?;
        let mut connections = BTreeMap::new();
        for (alias, mut options) in raw.connections {
            ensure!(
                safe_name(&alias),
                "connection alias must contain only ASCII letters, digits, underscores or hyphens"
            );
            let kind = options
                .remove("kind")
                .ok_or_else(|| anyhow!("connection kind is required"))?;
            let provider = find_provider(
                catalog,
                kind.as_str()
                    .ok_or_else(|| anyhow!("connection kind must be a string"))?,
            )?;
            provider.validate_config(&options)?;
            connections.insert(
                alias,
                Connection {
                    descriptor: provider.descriptor(),
                    options,
                },
            );
        }
        Ok(Self {
            theme: raw.theme,
            connections,
        })
    }

    pub fn configure<P: Provider>(
        &self,
        alias: &str,
        catalog: &[P],
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<P::Executor> {
        let connection = self.connections.get(alias).ok_or_else(|| {
            anyhow!("unknown connection alias; check the config connections table")
        })?;
        find_provider(catalog, connection.descriptor.kind)?.configure(&connection.options, env)
    }
}

pub fn secret(name: &str, env: &dyn Fn(&str) -> Option<String>) -> Result<String> {
    ensure!(
        safe_name(name),
        "environment reference contains unsupported characters"
    );
    env(name)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow!("referenced environment variable {name} is missing, empty or not Unicode")
        })
}

pub fn safe_name(value: &str) -> bool {
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
    use crate::test_provider::CATALOG;

    #[test]
    fn themes_default_parse_and_reject_invalid_values_without_echoing_them() {
        assert_eq!(
            Config::parse("[connections]", CATALOG).unwrap().theme,
            Theme::Catppuccin
        );
        for theme in Theme::ALL {
            let name = serde_json::to_string(&theme).unwrap();
            let config = Config::parse(&format!("theme={name}\n[connections]"), CATALOG).unwrap();
            assert_eq!(config.theme, theme);
            assert!(config.aliases().is_empty());
        }
        for value in [
            "''",
            "'dark'",
            "'light'",
            "'Monokai'",
            "'tokyo_night'",
            "'fake-super-secret'",
            "42",
            "true",
            "[]",
            "{}",
        ] {
            let error = Config::parse(&format!("theme={value}\n[connections]"), CATALOG)
                .err()
                .unwrap()
                .to_string();
            assert!(!error.contains("fake-super-secret"));
        }
    }

    #[test]
    fn resolves_only_selected_connection_and_rejects_empty_secrets() {
        let config = Config::parse(
            "[connections.a]\nkind='fake'\nsecret_env='SECRET'\n[connections.b]\nkind='fake'",
            CATALOG,
        )
        .unwrap();
        assert!(
            config
                .configure("b", CATALOG, &|_| panic!("unused secret resolved"))
                .is_ok()
        );
        assert!(
            config
                .configure("a", CATALOG, &|_| Some("  ".into()))
                .is_err()
        );
        assert!(config.configure("missing", CATALOG, &|_| None).is_err());
        assert!(config.configure("bad\x1b", CATALOG, &|_| None).is_err());
    }

    #[test]
    fn parser_errors_never_echo_config() {
        for input in [
            "password='fake-super-secret'",
            "[connections.b]\nkind='fake'\nunknown='fake-super-secret'",
            "[connections.q]\nkind='fake-super-secret'",
        ] {
            let error = Config::parse(input, CATALOG).err().unwrap().to_string();
            assert!(!error.contains("fake-super-secret"));
        }
    }

    #[test]
    fn validates_unselected_entries_and_all_aliases() {
        for input in [
            "[connections.a]\nkind='fake'\n[connections.b]\nkind='fake'\nunknown=true",
            "[connections.a]\nkind='fake'\n[connections.b]\nkind='unknown'",
            "[connections.'bad alias']\nkind='fake'",
        ] {
            assert!(Config::parse(input, CATALOG).is_err());
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
