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
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    #[serde(default)]
    pub tls_first: bool,
    pub nkey_env: Option<String>,
    pub credentials_env: Option<String>,
    pub domain: Option<String>,
    #[serde(default)]
    pub decoders: Vec<crate::decoding::Binding>,
    pub token_env: Option<String>,
    pub username_env: Option<String>,
    pub password_env: Option<String>,
    #[serde(default)]
    pub subjects: Vec<String>,
    #[serde(default = "tls_default")]
    pub jetstream: bool,
    #[serde(default)]
    pub system_discovery: bool,
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
        crate::decoding::validate(&config.decoders)?;
        ensure!(
            config.subjects.len() <= 32,
            "NATS supports at most 32 configured subjects"
        );
        for (index, subject) in config.subjects.iter().enumerate() {
            crate::core_subscription::validate_subject(subject)?;
            ensure!(
                !config.subjects[..index].contains(subject),
                "Duplicate NATS subject"
            );
        }
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
        for path in [&config.ca_file, &config.cert_file, &config.key_file]
            .into_iter()
            .flatten()
        {
            ensure!(
                config.tls
                    && path.len() <= 4096
                    && !path.contains('\0')
                    && std::path::Path::new(path).is_absolute(),
                "NATS certificate/key paths require TLS and an absolute path"
            );
        }
        ensure!(
            config.cert_file.is_some() == config.key_file.is_some(),
            "NATS cert_file and key_file must be paired"
        );
        ensure!(
            !config.tls_first || config.tls,
            "NATS tls_first requires TLS"
        );
        if let Some(domain) = &config.domain {
            ensure!(
                config.jetstream && safe_name(domain) && domain.len() <= 255,
                "NATS domain requires JetStream and 1..255 ASCII letters, digits, underscores or hyphens"
            );
        }
        for name in [
            &config.token_env,
            &config.username_env,
            &config.password_env,
            &config.nkey_env,
            &config.credentials_env,
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
            [
                &config.token_env,
                &config.username_env,
                &config.nkey_env,
                &config.credentials_env
            ]
            .iter()
            .filter(|v| v.is_some())
            .count()
                <= 1,
            "NATS token, username/password, NKEY and JWT credentials are mutually exclusive"
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
        if self.tls_first {
            options = options.tls_first();
        }
        if let Some(name) = &self.nkey_env {
            let seed = secret(name, env)?;
            options = options.nkey(seed.clone());
            secrets.push(seed);
        }
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
        if let Some(name) = &self.credentials_env {
            let credentials = secret(name, env)?;
            ensure!(
                credentials.len() <= 65536
                    && !credentials
                        .chars()
                        .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t')),
                "Invalid NATS credentials length or controls"
            );
            // Native errors can mention one credential component, not the complete .creds document.
            secrets.extend(
                credentials
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty() && !line.starts_with('-'))
                    .map(str::to_owned),
            );
            secrets.push(credentials.clone());
            options = options.credentials(&credentials).map_err(|error| {
                onetui_core::diagnostic(
                    error.into(),
                    &secrets.iter().map(String::as_str).collect::<Vec<_>>(),
                )
            })?;
        }
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
            "servers=['nats://localhost:4222']\nsubjects=['a.>.b']",
            "servers=['nats://localhost:4222']\nsubjects=['a','a']",
            "servers=['nats://localhost:4222']\njetstream='false'",
            "servers=['nats://localhost:4222']\ncert_file='/tmp/cert'",
            "servers=['nats://localhost:4222']\ntls=false\ntls_first=true",
            "servers=['nats://localhost:4222']\ndomain='other.API'",
            "servers=['nats://localhost:4222']\njetstream=false\ndomain='OTHER'",
            "servers=['nats://localhost:4222']\nnkey_env='KEY'\ncredentials_env='CREDS'",
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
        let jwt = Config::parse(
            &toml::from_str("servers=['tls://localhost:4222']\ncredentials_env='CREDS'").unwrap(),
        )
        .unwrap();
        assert!(jwt.options(&|_| Some("x".repeat(65537))).is_err());
        let creds = include_str!("../../../hack/fixtures/nats-test.creds");
        let (_, secrets) = jwt.options(&|_| Some(creds.into())).unwrap();
        assert!(secrets.iter().any(|s| s.starts_with("SUACH")));
        assert!(secrets.iter().any(|s| s.starts_with("eyJ")));
    }
}
