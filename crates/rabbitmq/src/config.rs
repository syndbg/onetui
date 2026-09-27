use std::path::PathBuf;

use anyhow::{Result, anyhow, ensure};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub username_env: Option<String>,
    pub password_env: Option<String>,
    pub ca_file: Option<PathBuf>,
}

impl Config {
    pub fn parse(options: &toml::Table) -> Result<Self> {
        let config: Self = options.clone().try_into().map_err(|_| {
            anyhow!("Invalid RabbitMQ config; expected url, username/password or username_env/password_env and optional ca_file")
        })?;
        endpoint(&config.url)?;
        ensure!(
            config.username.is_some() != config.username_env.is_some()
                && config.password.is_some() != config.password_env.is_some(),
            "RabbitMQ username and password each need one source: a value or an _env reference"
        );
        ensure!(
            config
                .username
                .as_deref()
                .is_none_or(|value| !value.is_empty())
                && config
                    .password
                    .as_deref()
                    .is_none_or(|value| !value.is_empty()),
            "RabbitMQ username and password cannot be empty"
        );
        ensure!(
            [&config.username_env, &config.password_env]
                .into_iter()
                .flatten()
                .all(|name| onetui_core::config::safe_name(name)),
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
