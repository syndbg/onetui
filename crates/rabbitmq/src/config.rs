use std::path::PathBuf;

use anyhow::{Result, anyhow, ensure};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub url: String,
    pub username_env: String,
    pub password_env: String,
    pub ca_file: Option<PathBuf>,
}

impl Config {
    pub fn parse(options: &toml::Table) -> Result<Self> {
        let config: Self = options.clone().try_into().map_err(|_| {
            anyhow!("Invalid RabbitMQ config; expected url, username_env, password_env and optional ca_file")
        })?;
        endpoint(&config.url)?;
        ensure!(
            onetui_core::config::safe_name(&config.username_env)
                && onetui_core::config::safe_name(&config.password_env),
            "Invalid RabbitMQ credential environment reference"
        );
        ensure!(
            config
                .ca_file
                .as_ref()
                .is_none_or(|path| !path.as_os_str().is_empty()),
            "RabbitMQ ca_file cannot be empty"
        );
        Ok(config)
    }
}

pub(crate) fn endpoint(value: &str) -> Result<url::Url> {
    let url = url::Url::parse(value).map_err(|_| anyhow!("Invalid RabbitMQ management URL"))?;
    ensure!(
        url.host().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && matches!(url.path(), "" | "/")
            && url.query().is_none()
            && url.fragment().is_none(),
        "RabbitMQ URL must be an origin without credentials, path, query or fragment"
    );
    let loopback = match url.host() {
        Some(url::Host::Domain(host)) => host == "localhost",
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    };
    ensure!(
        url.scheme() == "https" || (url.scheme() == "http" && loopback),
        "RabbitMQ requires HTTPS, except HTTP on loopback for local development"
    );
    Ok(url)
}
