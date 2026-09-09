use anyhow::{Result, anyhow, ensure};
use onetui_core::config::{safe_name, secret};
use rdkafka::ClientConfig;
use serde::Deserialize;
use std::path::Path;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub bootstrap_servers: Vec<String>,
    #[serde(default = "default_security")]
    pub security_protocol: String,
    pub ca_file: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub username_env: Option<String>,
    pub password_env: Option<String>,
}

fn default_security() -> String {
    "SSL".into()
}

impl Config {
    pub fn parse(options: &toml::Table) -> Result<Self> {
        let config: Self = options
            .clone()
            .try_into()
            .map_err(|_| anyhow!("Invalid Kafka config; use schema for supported settings"))?;
        ensure!(
            (1..=32).contains(&config.bootstrap_servers.len()),
            "Kafka bootstrap_servers requires 1..32 host:port entries"
        );
        ensure!(
            matches!(
                config.security_protocol.as_str(),
                "SSL" | "SASL_SSL" | "PLAINTEXT"
            ),
            "Kafka security_protocol must be SSL, SASL_SSL or PLAINTEXT"
        );
        for server in &config.bootstrap_servers {
            ensure!(
                server.len() <= 255,
                "Kafka bootstrap address exceeds 255 bytes"
            );
            let url = url::Url::parse(&format!("kafka://{server}"))
                .map_err(|_| anyhow!("Invalid Kafka bootstrap host:port"))?;
            ensure!(
                !server.contains(['/', '?', '#', '@', ',', '\\'])
                    && !server.chars().any(char::is_whitespace)
                    && url.host().is_some()
                    && url.port().is_some_and(|port| port != 0)
                    && url.username().is_empty()
                    && url.password().is_none(),
                "Kafka bootstrap addresses must be host:port without credentials or URL components"
            );
            if config.security_protocol == "PLAINTEXT" {
                let local = match url.host() {
                    Some(url::Host::Domain(host)) => {
                        host.eq_ignore_ascii_case("localhost")
                            || host
                                .parse::<std::net::IpAddr>()
                                .is_ok_and(|ip| ip.is_loopback())
                    }
                    Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
                    Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
                    None => false,
                };
                ensure!(
                    local,
                    "Plaintext Kafka bootstrap hosts must be loopback; use TLS remotely"
                );
            }
        }
        if let Some(path) = &config.ca_file {
            ensure!(
                path.len() <= 4096 && !path.contains('\0') && Path::new(path).is_absolute(),
                "Kafka ca_file must be an absolute path"
            );
            ensure!(
                config.security_protocol != "PLAINTEXT",
                "Kafka ca_file requires TLS"
            );
        }
        if config.security_protocol == "SASL_SSL" {
            ensure!(
                matches!(
                    config.sasl_mechanism.as_deref(),
                    Some("PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512")
                ),
                "Kafka SASL_SSL requires PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512"
            );
            ensure!(
                config.username_env.as_deref().is_some_and(safe_name)
                    && config.password_env.as_deref().is_some_and(safe_name),
                "Kafka SASL_SSL requires valid username_env and password_env references"
            );
        } else {
            ensure!(
                config.sasl_mechanism.is_none()
                    && config.username_env.is_none()
                    && config.password_env.is_none(),
                "Kafka SASL settings require SASL_SSL"
            );
        }
        Ok(config)
    }

    pub fn native(
        &self,
        identity: u64,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<(ClientConfig, Vec<String>)> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", self.bootstrap_servers.join(","))
            .set("security.protocol", &self.security_protocol)
            .set("client.id", "onetui")
            .set(
                "group.id",
                format!("onetui-{}-{identity}", std::process::id()),
            )
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("allow.auto.create.topics", "false")
            .set("auto.offset.reset", "error")
            .set("isolation.level", "read_committed")
            .set("enable.partition.eof", "true")
            .set("queued.min.messages", "1")
            .set("queued.max.messages.kbytes", "1024")
            .set("fetch.message.max.bytes", "1048576")
            .set("fetch.max.bytes", "1048576")
            // Zstandard frames can omit their decoded size. Native allocation grows in steps;
            // leave room to reach 1 MiB, then enforce the smaller retained-page limit ourselves.
            .set("receive.message.max.bytes", "4194304")
            .set("socket.timeout.ms", "1000")
            .set("socket.connection.setup.timeout.ms", "1000")
            .set("enable.ssl.certificate.verification", "true")
            .set("ssl.endpoint.identification.algorithm", "https");
        if let Some(path) = &self.ca_file {
            config.set("ssl.ca.location", path);
        }
        let mut secrets = Vec::new();
        if let Some(mechanism) = &self.sasl_mechanism {
            let username = secret(self.username_env.as_deref().unwrap(), env)?;
            let password = secret(self.password_env.as_deref().unwrap(), env)?;
            ensure!(
                !username.contains('\0') && !password.contains('\0'),
                "Kafka SASL credentials must not contain NUL"
            );
            config
                .set("sasl.mechanism", mechanism)
                .set("sasl.username", &username)
                .set("sasl.password", &password);
            secrets.extend([username, password]);
        }
        Ok((config, secrets))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_offline_config_and_nonoverridable_read_safety() {
        let options =
            toml::from_str("bootstrap_servers=['127.0.0.1:9092']\nsecurity_protocol='PLAINTEXT'")
                .unwrap();
        let config = Config::parse(&options).unwrap();
        let (native, _) = config
            .native(1, &|_| panic!("no secret configured"))
            .unwrap();
        assert_eq!(native.get("enable.auto.commit"), Some("false"));
        assert_eq!(native.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(native.get("allow.auto.create.topics"), Some("false"));
        assert_eq!(native.get("auto.offset.reset"), Some("error"));
        assert_eq!(native.get("isolation.level"), Some("read_committed"));
        for options in [
            "bootstrap_servers=[]",
            "bootstrap_servers=['localhost']",
            "bootstrap_servers=['fake-secret@localhost:9092']",
            "bootstrap_servers=['remote:9092']\nsecurity_protocol='PLAINTEXT'",
            "bootstrap_servers=['localhost:9092']\ngroup_id='app'",
            "bootstrap_servers=['localhost:9092']\nca_file='relative.pem'",
            "bootstrap_servers=['localhost:9092']\nsecurity_protocol='SASL_SSL'",
            "bootstrap_servers=['localhost:9092']\npassword_env='KEY'",
        ] {
            let error = Config::parse(&toml::from_str(options).unwrap())
                .err()
                .expect("invalid config");
            assert!(!error.to_string().contains("fake-secret"));
        }
        let options = toml::from_str("bootstrap_servers=['broker:9093']\nsecurity_protocol='SASL_SSL'\nsasl_mechanism='SCRAM-SHA-256'\nusername_env='USER_REF'\npassword_env='PASS_REF'").unwrap();
        let config = Config::parse(&options).unwrap();
        assert!(config.native(2, &|_| None).is_err());
        let (native, secrets) = config.native(2, &|name| Some(name.into())).unwrap();
        assert_eq!(secrets, ["USER_REF", "PASS_REF"]);
        assert_eq!(native.get("sasl.password"), Some("PASS_REF"));
    }
}
