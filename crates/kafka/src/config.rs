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
    pub client_cert_file: Option<String>,
    pub client_key_file: Option<String>,
    pub client_key_password_env: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub username_env: Option<String>,
    pub password_env: Option<String>,
    pub kerberos_principal: Option<String>,
    pub kerberos_service_name: Option<String>,
    pub oauth: Option<crate::oauth::Config>,
    #[serde(default)]
    pub decoders: Vec<crate::decoding::Binding>,
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
        for (name, path) in [
            ("ca_file", &config.ca_file),
            ("client_cert_file", &config.client_cert_file),
            ("client_key_file", &config.client_key_file),
        ] {
            let Some(path) = path else { continue };
            ensure!(
                path.len() <= 4096
                    && !path.chars().any(char::is_control)
                    && Path::new(path).is_absolute(),
                "Kafka {name} must be an absolute path without controls (max 4096 bytes)"
            );
            ensure!(
                config.security_protocol != "PLAINTEXT",
                "Kafka {name} requires TLS"
            );
        }
        ensure!(
            config.client_cert_file.is_some() == config.client_key_file.is_some(),
            "Kafka client_cert_file and client_key_file must be configured together"
        );
        if let Some(name) = &config.client_key_password_env {
            ensure!(
                config.client_key_file.is_some() && safe_name(name),
                "Kafka client_key_password_env requires a client key and a valid environment reference"
            );
        }
        if config.security_protocol == "SASL_SSL" {
            ensure!(
                matches!(
                    config.sasl_mechanism.as_deref(),
                    Some("PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" | "OAUTHBEARER" | "GSSAPI")
                ),
                "Kafka SASL_SSL requires PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER or GSSAPI"
            );
            if config.sasl_mechanism.as_deref() == Some("OAUTHBEARER") {
                ensure!(
                    config.username_env.is_none() && config.password_env.is_none(),
                    "Kafka OAUTHBEARER uses oauth credentials, not username_env/password_env"
                );
                config
                    .oauth
                    .as_ref()
                    .ok_or_else(|| anyhow!("Kafka OAUTHBEARER requires oauth settings"))?
                    .validate()?;
            } else if config.sasl_mechanism.as_deref() == Some("GSSAPI") {
                ensure!(
                    config.oauth.is_none()
                        && config.username_env.is_none()
                        && config.password_env.is_none(),
                    "Kafka GSSAPI uses an existing Kerberos ticket cache, not password or oauth settings"
                );
                ensure!(
                    config
                        .kerberos_principal
                        .as_deref()
                        .is_some_and(|principal| !principal.is_empty()
                            && principal.len() <= 1024
                            && !principal
                                .chars()
                                .any(|c| c.is_control() || c.is_whitespace())),
                    "Kafka GSSAPI requires kerberos_principal without whitespace or controls (max 1024 bytes)"
                );
                if let Some(service) = &config.kerberos_service_name {
                    ensure!(
                        (1..=255).contains(&service.len())
                            && service
                                .bytes()
                                .all(|c| c.is_ascii_alphanumeric() || b"_-.".contains(&c)),
                        "Kafka kerberos_service_name requires 1..255 ASCII letters, digits, underscore, hyphen or dot"
                    );
                }
            } else {
                ensure!(config.oauth.is_none(), "Kafka oauth requires OAUTHBEARER");
                ensure!(
                    config.username_env.as_deref().is_some_and(safe_name)
                        && config.password_env.as_deref().is_some_and(safe_name),
                    "Kafka SASL_SSL requires valid username_env and password_env references"
                );
            }
        } else {
            ensure!(
                config.sasl_mechanism.is_none()
                    && config.username_env.is_none()
                    && config.password_env.is_none(),
                "Kafka SASL settings require SASL_SSL"
            );
            ensure!(
                config.oauth.is_none(),
                "Kafka oauth requires SASL_SSL and OAUTHBEARER"
            );
        }
        ensure!(
            config.sasl_mechanism.as_deref() == Some("GSSAPI")
                || (config.kerberos_principal.is_none() && config.kerberos_service_name.is_none()),
            "Kafka Kerberos settings require SASL_SSL and GSSAPI"
        );
        crate::decoding::validate(&config.decoders)?;
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
            // The default 1-second refill backoff stalls this deliberately small queue.
            .set("fetch.queue.backoff.ms", "100")
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
        if let Some(path) = &self.client_cert_file {
            config.set("ssl.certificate.location", path);
        }
        if let Some(path) = &self.client_key_file {
            config.set("ssl.key.location", path);
        }
        if let Some(name) = &self.client_key_password_env {
            let password = secret(name, env)?;
            ensure!(
                !password.contains('\0'),
                "Kafka client key password must not contain NUL"
            );
            config.set("ssl.key.password", &password);
            secrets.push(password);
        }
        if let Some(mechanism) = &self.sasl_mechanism {
            config.set("sasl.mechanism", mechanism);
            config.set("enable.sasl.oauthbearer.unsecure.jwt", "false");
        }
        if let Some(name) = &self.username_env {
            let username = secret(name, env)?;
            let password = secret(self.password_env.as_deref().unwrap(), env)?;
            ensure!(
                !username.contains('\0') && !password.contains('\0'),
                "Kafka SASL credentials must not contain NUL"
            );
            config
                .set("sasl.username", &username)
                .set("sasl.password", &password);
            secrets.extend([username, password]);
        }
        if let Some(principal) = &self.kerberos_principal {
            config
                .set("sasl.kerberos.principal", principal)
                .set(
                    "sasl.kerberos.service.name",
                    self.kerberos_service_name.as_deref().unwrap_or("kafka"),
                )
                // Credentials belong to the caller. Never let librdkafka invoke its shell-based kinit command.
                .set("sasl.kerberos.min.time.before.relogin", "0")
                .set("sasl.kerberos.kinit.cmd", "");
        }
        Ok((config, secrets))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kerberos_uses_existing_credentials_without_shell_or_secret_lookup() {
        let base = "bootstrap_servers=['broker:9093']\nsecurity_protocol='SASL_SSL'\nsasl_mechanism='GSSAPI'\n";
        let valid = format!("{base}kerberos_principal='reader@EXAMPLE.COM'\n");
        let config = Config::parse(&toml::from_str(&valid).unwrap()).unwrap();
        let (native, secrets) = config
            .native(1, &|_| panic!("Kerberos owns credentials"))
            .unwrap();
        assert!(secrets.is_empty());
        assert_eq!(
            native.get("sasl.kerberos.principal"),
            Some("reader@EXAMPLE.COM")
        );
        assert_eq!(native.get("sasl.kerberos.service.name"), Some("kafka"));
        assert_eq!(
            native.get("sasl.kerberos.min.time.before.relogin"),
            Some("0")
        );
        assert_eq!(native.get("sasl.kerberos.kinit.cmd"), Some(""));
        native.create_native_config().unwrap();
        let custom = Config::parse(
            &toml::from_str(&format!("{valid}kerberos_service_name='broker'")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            custom
                .native(1, &|_| None)
                .unwrap()
                .0
                .get("sasl.kerberos.service.name"),
            Some("broker")
        );
        assert!(Config::parse(&toml::from_str(base).unwrap()).is_err());
        for extra in [
            "username_env='USER'",
            "password_env='PASS'",
            "kerberos_service_name=''",
            "kerberos_service_name='bad/name'",
            "kerberos_keytab='/keytab'",
            "kerberos_kinit_cmd='kinit'",
            "oauth={}",
        ] {
            assert!(
                Config::parse(&toml::from_str(&format!("{valid}{extra}")).unwrap()).is_err(),
                "{extra}"
            );
        }
        for principal in ["", "reader user", "reader\\nuser"] {
            assert!(
                Config::parse(
                    &toml::from_str(&format!("{base}kerberos_principal=\"{principal}\"")).unwrap()
                )
                .is_err()
            );
        }
        assert!(
            Config::parse(
                &toml::from_str("bootstrap_servers=['broker:9093']\nkerberos_principal='reader'")
                    .unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn client_certificates_are_paired_offline_and_secrets_are_alias_local() {
        let base = "bootstrap_servers=['broker:9093']\n";
        let pair = "client_cert_file='/not-read/client.pem'\nclient_key_file='/not-read/key.pem'\n";
        for invalid in [
            "client_cert_file='/client.pem'",
            "client_key_file='/key.pem'",
            "client_key_password_env='KEY_PASSWORD'",
            "client_cert_file='relative.pem'\nclient_key_file='/key.pem'",
            "client_cert_file='/client.pem'\nclient_key_file='relative.pem'",
            "client_cert_file='/client.pem'\nclient_key_file='/key.pem'\nclient_key_password_env='bad name'",
            "client_cert_file='/client.pem'\nclient_key_file='/key.pem'\nsecurity_protocol='PLAINTEXT'",
            "client_cert_file=\"/client\\n.pem\"\nclient_key_file='/key.pem'",
        ] {
            assert!(Config::parse(&toml::from_str(&format!("{base}{invalid}")).unwrap()).is_err());
        }
        let config = Config::parse(&toml::from_str(&format!("{base}{pair}")).unwrap()).unwrap();
        let (native, secrets) = config
            .native(1, &|_| panic!("no password configured"))
            .unwrap();
        assert!(secrets.is_empty());
        assert_eq!(
            native.get("ssl.certificate.location"),
            Some("/not-read/client.pem")
        );
        assert_eq!(native.get("ssl.key.location"), Some("/not-read/key.pem"));
        assert_eq!(
            native.get("enable.ssl.certificate.verification"),
            Some("true")
        );
        assert_eq!(
            native.get("ssl.endpoint.identification.algorithm"),
            Some("https")
        );
        let config = Config::parse(&toml::from_str(&format!("{base}{pair}client_key_password_env='KEY_PASSWORD'\nsecurity_protocol='SASL_SSL'\nsasl_mechanism='PLAIN'\nusername_env='USER'\npassword_env='PASS'")).unwrap()).unwrap();
        assert!(config.native(2, &|_| None).is_err());
        assert!(config.native(2, &|_| Some("bad\0secret".into())).is_err());
        let (native, secrets) = config.native(2, &|name| Some(name.into())).unwrap();
        assert_eq!(native.get("ssl.key.password"), Some("KEY_PASSWORD"));
        assert_eq!(native.get("sasl.password"), Some("PASS"));
        assert_eq!(secrets, ["KEY_PASSWORD", "USER", "PASS"]);
    }

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
        assert_eq!(native.get("fetch.queue.backoff.ms"), Some("100"));
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
