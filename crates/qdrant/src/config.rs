use anyhow::{Result, anyhow, ensure};
use onetui_core::config::safe_name;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub url: String,
    pub rest_url: Option<String>,
    pub api_key_env: Option<String>,
}

impl Config {
    pub fn parse(options: &toml::Table) -> Result<Self> {
        let config: Self = options.clone().try_into().map_err(|_| {
            anyhow!("invalid Qdrant config; expected url and optional rest_url, api_key_env")
        })?;
        super::qdrant_url(&config.url)?;
        if let Some(url) = &config.rest_url {
            super::qdrant_url(url)?;
        }
        ensure!(
            config.api_key_env.as_deref().is_none_or(safe_name),
            "environment reference contains unsupported characters"
        );
        Ok(config)
    }
}
