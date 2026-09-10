use anyhow::{Result, anyhow, ensure};
use onetui_core::config::{safe_name, secret};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub servers: Vec<String>,
    #[serde(default = "tls_default")]
    pub tls: bool,
    pub ca_file: Option<String>,
    pub token_env: Option<String>,
    pub username_env: Option<String>,
    pub password_env: Option<String>,
}

fn tls_default() -> bool {
    true
}

impl Config {
    pub fn parse(options: &toml::Table) -> Result<Self> {
        let config: Self = options
            .clone()
            .try_into()
            .map_err(|_| anyhow!("Invalid NATS config; use schema for supported settings"))?;
        ensure!(
            (1..=32).contains(&config.servers.len()),
            "NATS servers requires 1..32 URLs"
        );
        for server in &config.servers {
            ensure!(
                server.len() <= 1024 && !server.chars().any(char::is_whitespace),
                "Invalid NATS server URL"
            );
            let url = url::Url::parse(server).map_err(|_| anyhow!("Invalid NATS server URL"))?;
            ensure!(
                matches!(url.scheme(), "nats" | "tls")
                    && url.host().is_some()
                    && url.port().is_some_and(|p| p != 0)
                    && url.username().is_empty()
                    && url.password().is_none()
                    && matches!(url.path(), "" | "/")
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "NATS servers must be nats://host:port or tls://host:port without credentials or extra components"
            );
            if !config.tls {
                let local = match url.host() {
                    Some(url::Host::Domain(host)) => {
                        host == "localhost"
                            || host
                                .parse::<std::net::IpAddr>()
                                .is_ok_and(|ip| ip.is_loopback())
                    }
                    Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
                    Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
                    None => false,
                };
                ensure!(
                    local && url.scheme() == "nats",
                    "NATS tls=false requires loopback nats:// servers"
                );
            }
        }
        if let Some(path) = &config.ca_file {
            ensure!(
                config.tls
                    && path.len() <= 4096
                    && !path.contains('\0')
                    && std::path::Path::new(path).is_absolute(),
                "NATS ca_file requires TLS and an absolute path"
            );
        }
        for name in [
            &config.token_env,
            &config.username_env,
            &config.password_env,
        ]
        .into_iter()
        .flatten()
        {
            ensure!(
                safe_name(name),
                "NATS authentication references must be environment variable names"
            );
        }
        ensure!(
            config.username_env.is_some() == config.password_env.is_some(),
            "NATS username_env and password_env must be paired"
        );
        ensure!(
            config.token_env.is_none() || config.username_env.is_none(),
            "NATS token and username/password authentication are mutually exclusive"
        );
        Ok(config)
    }

    pub fn options(
        &self,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<(async_nats::ConnectOptions, Vec<String>)> {
        let mut options = async_nats::ConnectOptions::new()
            .name("onetui")
            .require_tls(self.tls)
            .ignore_discovered_servers()
            .max_reconnects(self.servers.len())
            .connection_timeout(std::time::Duration::from_secs(1))
            .ping_interval(std::time::Duration::from_secs(15))
            .subscription_capacity(16)
            .client_capacity(16);
        let mut secrets = Vec::new();
        if let Some(name) = &self.token_env {
            let token = secret(name, env)?;
            options = options.token(token.clone());
            secrets.push(token);
        }
        if let Some(name) = &self.username_env {
            let username = secret(name, env)?;
            let password = secret(self.password_env.as_deref().unwrap(), env)?;
            options = options.user_and_password(username.clone(), password.clone());
            secrets.extend([username, password]);
        }
        ensure!(
            secrets
                .iter()
                .all(|s| s.len() <= 65536 && !s.chars().any(char::is_control)),
            "Invalid NATS credential length or controls"
        );
        Ok((options, secrets))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_is_strict_offline_and_secrets_are_resolved_only_on_selection() {
        for server in [
            "nats://127.0.0.1:4222",
            "nats://localhost:4222",
            "nats://[::1]:4222",
        ] {
            assert!(
                Config::parse(
                    &toml::from_str(&format!("servers=['{server}']\ntls=false")).unwrap()
                )
                .is_ok()
            );
        }
        for invalid in [
            "servers=[]",
            "servers=['nats://remote:4222']\ntls=false",
            "servers=['nats://secret@localhost:4222']",
            "servers=['nats://localhost:4222/a']",
            "servers=['nats://localhost:4222']\nconsumer='app'",
            "servers=['nats://localhost:4222']\ntoken_env='X'\nusername_env='U'\npassword_env='P'",
            "servers=['nats://localhost:4222']\nca_file='relative'",
            "servers=['nats://localhost:4222']\npassword_env='P'",
        ] {
            assert!(Config::parse(&toml::from_str(invalid).unwrap()).is_err());
        }
        let config = Config::parse(
            &toml::from_str("servers=['tls://localhost:4222']\ntoken_env='TOKEN'").unwrap(),
        )
        .unwrap();
        assert!(config.tls);
        assert!(config.options(&|_| None).is_err());
        assert_eq!(
            config.options(&|_| Some("fixture-only".into())).unwrap().1,
            ["fixture-only"]
        );
        assert!(config.options(&|_| Some("secret\ncontrol".into())).is_err());
    }
}
