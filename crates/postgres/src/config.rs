use anyhow::{Result, anyhow, ensure};
use onetui_core::config::safe_name;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub url_env: String,
    pub ca_file: Option<PathBuf>,
}

impl Config {
    pub fn parse(options: &toml::Table) -> Result<Self> {
        let config: Self = options.clone().try_into().map_err(|_| {
            anyhow!("invalid PostgreSQL config; expected url_env and optional ca_file")
        })?;
        ensure!(
            safe_name(&config.url_env),
            "environment reference contains unsupported characters"
        );
        ensure!(
            config.ca_file.as_ref().is_none_or(|p| p.is_absolute()),
            "ca_file must be an absolute path"
        );
        Ok(config)
    }
}
