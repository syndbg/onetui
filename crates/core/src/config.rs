use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::provider::{Provider, ProviderDescriptor, find_provider, validate_catalog};
use anyhow::{Result, anyhow, bail, ensure};
use onetui_theme::Theme;
use serde::Deserialize;

pub struct Config {
    pub theme: Theme,
    pub display: crate::value::DisplayOptions,
    pub persist_query_history: bool,
    connections: BTreeMap<String, Connection>,
    source: Option<(PathBuf, Option<String>)>,
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
    #[serde(default)]
    display: crate::value::DisplayOptions,
    #[serde(default)]
    persist_query_history: bool,
    #[serde(default)]
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
        Self::load_for_startup(path, catalog, false)
    }

    pub fn load_for_startup<P: Provider>(
        path: &Path,
        catalog: &[P],
        allow_missing: bool,
    ) -> Result<Self> {
        let path = std::path::absolute(path)?;
        let text = read_config(&path)?;
        ensure!(
            allow_missing || text.is_some(),
            "config file not found: {}; use an existing --config <path> or start without --config or --check to add connections",
            crate::display(&path.display().to_string())
        );
        let mut config = Self::parse(text.as_deref().unwrap_or(""), catalog)?;
        config.source = Some((path, text));
        Ok(config)
    }

    pub fn path(&self) -> Option<&Path> {
        self.source.as_ref().map(|(path, _)| path.as_path())
    }

    pub fn add_connection<P: Provider>(
        &mut self,
        alias: &str,
        kind: &str,
        options: toml::Table,
        catalog: &[P],
    ) -> Result<()> {
        ensure!(
            safe_name(alias),
            "Alias must contain only ASCII letters, digits, underscores or hyphens"
        );
        ensure!(
            !self.connections.contains_key(alias),
            "Connection alias already exists"
        );
        let provider = find_provider(catalog, kind)?;
        provider.validate_config(&options)?;
        let (path, original) = self
            .source
            .as_ref()
            .ok_or_else(|| anyhow!("No configuration save path"))?;
        let mut entry = options;
        entry.insert("kind".into(), kind.into());
        let text = format!(
            "{}\n[connections.{alias}]\n{}",
            original.as_deref().unwrap_or(""),
            toml::to_string(&entry)?
        );
        ensure!(text.len() <= 1024 * 1024, "Config exceeds 1 MiB");
        // Validate the complete document before touching disk, including existing entries.
        let parsed = Self::parse(&text, catalog).map_err(|_| anyhow!("Cannot append to this TOML layout; expand inline connections into [connections.alias] sections before adding"))?;
        ensure!(
            read_config(path)? == *original,
            "Config changed on disk; reopen OneTUI before saving"
        );
        if original.is_some() {
            ensure!(
                !std::fs::symlink_metadata(path)?.file_type().is_symlink(),
                "Config is a symlink; use --config with its target to save"
            );
            ensure!(
                !std::fs::metadata(path)?.permissions().readonly(),
                "Config file is read-only"
            );
        }
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("Config path has no parent"))?;
        std::fs::create_dir_all(parent)?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        if original.is_some() {
            temporary
                .as_file()
                .set_permissions(std::fs::metadata(path)?.permissions())?;
        }
        temporary.write_all(text.as_bytes())?;
        temporary.as_file().sync_all()?;
        ensure!(
            read_config(path)? == *original,
            "Config changed on disk; reopen OneTUI before saving"
        );
        if original.is_some() {
            temporary.persist(path).map_err(|e| e.error)?;
        } else {
            temporary.persist_noclobber(path).map_err(|e| e.error)?;
        }
        self.connections = parsed.connections;
        self.source.as_mut().unwrap().1 = Some(text);
        Ok(())
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
            display: raw.display,
            persist_query_history: raw.persist_query_history,
            connections,
            source: None,
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

fn read_config(path: &Path) -> Result<Option<String>> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    let result = (|| -> Result<String> {
        ensure!(std::fs::metadata(path)?.is_file(), "not a regular file");
        let mut text = String::new();
        std::fs::File::open(path)?
            .take(1024 * 1024 + 1)
            .read_to_string(&mut text)?;
        ensure!(text.len() <= 1024 * 1024, "config exceeds 1 MiB");
        Ok(text)
    })();
    result.map(Some).map_err(|error| {
        anyhow!(
            "cannot read config {}: {error}",
            crate::display(&path.display().to_string())
        )
    })
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
    fn missing_default_is_empty_but_explicit_and_invalid_files_fail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing/config.toml");
        let config = Config::load_for_startup(&path, CATALOG, true).unwrap();
        assert!(config.aliases().is_empty());
        assert!(!path.parent().unwrap().exists());
        let error = Config::load(&path, CATALOG).err().unwrap().to_string();
        assert!(error.contains("config file not found"));
        assert!(error.contains("config.toml"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "invalid = [").unwrap();
        assert!(Config::load_for_startup(&path, CATALOG, true).is_err());
        assert!(Config::load_for_startup(dir.path(), CATALOG, true).is_err());
    }

    #[test]
    fn query_history_persistence_requires_explicit_opt_in() {
        assert!(!Config::parse("", CATALOG).unwrap().persist_query_history);
        assert!(
            Config::parse("persist_query_history = true", CATALOG)
                .unwrap()
                .persist_query_history
        );
        assert!(Config::parse("persist_query_history = 'true'", CATALOG).is_err());
    }

    #[test]
    fn saving_validates_preserves_text_and_detects_external_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let original = "# Keep this comment\ntheme='monokai'\n[display]\nword_wrap=false\n[connections.old]\nkind='fake'\n";
        std::fs::write(&path, original).unwrap();
        let mut config = Config::load(&path, CATALOG).unwrap();
        config.theme = Theme::Nord;
        config
            .add_connection(
                "new",
                "fake",
                toml::from_str("secret_env='UNSET_SECRET'").unwrap(),
                CATALOG,
            )
            .unwrap();
        assert!(
            std::fs::read_to_string(&path)
                .unwrap()
                .starts_with(original)
        );
        assert_eq!(config.theme, Theme::Nord, "session theme stays in memory");
        let loaded = Config::load(&path, CATALOG).unwrap();
        assert_eq!(loaded.theme, Theme::Monokai);
        assert!(!loaded.display.word_wrap);
        assert_eq!(loaded.aliases(), [("new", "fake"), ("old", "fake")]);
        let saved = std::fs::read_to_string(&path).unwrap();
        for (alias, kind, options) in [
            ("new", "fake", ""),
            ("bad.alias", "fake", ""),
            ("bad", "unknown", ""),
            ("bad", "fake", "unknown=1"),
        ] {
            assert!(
                config
                    .add_connection(alias, kind, toml::from_str(options).unwrap(), CATALOG)
                    .is_err()
            );
            assert_eq!(std::fs::read_to_string(&path).unwrap(), saved);
        }
        std::fs::write(&path, format!("{saved}\n# external edit\n")).unwrap();
        assert!(
            config
                .add_connection("other", "fake", toml::Table::new(), CATALOG)
                .unwrap_err()
                .to_string()
                .contains("changed on disk")
        );
        assert!(config.descriptor("other").is_none());
        assert!(
            std::fs::read_to_string(&path)
                .unwrap()
                .ends_with("# external edit\n")
        );
    }

    #[test]
    fn saving_first_connection_creates_private_file_only_after_validation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new/config.toml");
        let mut config = Config::load_for_startup(&path, CATALOG, true).unwrap();
        assert!(
            config
                .add_connection("", "fake", toml::Table::new(), CATALOG)
                .is_err()
        );
        assert!(!path.parent().unwrap().exists());
        config
            .add_connection("first", "fake", toml::Table::new(), CATALOG)
            .unwrap();
        assert_eq!(
            Config::load(&path, CATALOG).unwrap().aliases(),
            [("first", "fake")]
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn inline_connection_tables_fail_without_rewriting_existing_text() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let text = "connections = {} # keep\n";
        std::fs::write(&path, text).unwrap();
        let mut config = Config::load(&path, CATALOG).unwrap();
        let error = config
            .add_connection("new", "fake", toml::Table::new(), CATALOG)
            .unwrap_err();
        assert!(error.to_string().contains("inline connections"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
    }

    #[cfg(unix)]
    #[test]
    fn save_rejects_readonly_and_symlink_files_without_changing_them() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let text = "[connections]\n";
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();
        let mut config = Config::load(&path, CATALOG).unwrap();
        assert!(
            config
                .add_connection("new", "fake", toml::Table::new(), CATALOG)
                .is_err()
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
        let link = dir.path().join("link.toml");
        symlink(&path, &link).unwrap();
        let mut linked = Config::load(&link, CATALOG).unwrap();
        assert!(
            linked
                .add_connection("new", "fake", toml::Table::new(), CATALOG)
                .is_err()
        );
        assert!(link.is_symlink());
        let dangling = dir.path().join("dangling.toml");
        symlink(dir.path().join("absent"), &dangling).unwrap();
        assert!(Config::load_for_startup(&dangling, CATALOG, true).is_err());
    }

    #[test]
    fn display_configuration_is_optional_strict_and_offline() {
        let defaults = Config::parse("[connections]", CATALOG).unwrap();
        assert!(
            defaults.display.word_wrap
                && defaults.display.pretty_print
                && defaults.display.highlight
        );
        let config = Config::parse(
            "[display]\nformat='binary'\nword_wrap=false\nunicode='escaped'\n[connections]",
            CATALOG,
        )
        .unwrap();
        assert_eq!(config.display.format, crate::value::ValueFormat::Binary);
        assert!(!config.display.word_wrap);
        for bad in [
            "format='bad'",
            "word_wrap='off'",
            "pretty_print=0",
            "highlight=[]",
            "unicode=''",
            "unknown=true",
        ] {
            assert!(Config::parse(&format!("[display]\n{bad}\n[connections]"), CATALOG).is_err());
        }
    }

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
