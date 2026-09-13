use anyhow::{Result, anyhow, ensure};
use aws_sdk_dynamodb::config::{Credentials, Region};
use onetui_core::config::{safe_name, secret};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub region: String,
    pub profile: Option<String>,
    pub endpoint_url: Option<String>,
    pub streams_endpoint_url: Option<String>,
    pub access_key_id_env: Option<String>,
    pub secret_access_key_env: Option<String>,
    pub session_token_env: Option<String>,
}

impl Config {
    pub fn parse(options: &toml::Table) -> Result<Self> {
        let config: Self = options
            .clone()
            .try_into()
            .map_err(|_| anyhow!("Invalid DynamoDB configuration"))?;
        ensure!(
            !config.region.is_empty()
                && config.region.len() <= 64
                && config
                    .region
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
            "DynamoDB region requires 1..64 lowercase ASCII letters, digits or hyphens"
        );
        if let Some(profile) = &config.profile {
            ensure!(
                !profile.is_empty()
                    && profile.len() <= 256
                    && !profile.chars().any(char::is_control),
                "Invalid AWS profile name"
            );
        }
        ensure!(
            config.access_key_id_env.is_some() == config.secret_access_key_env.is_some(),
            "DynamoDB access_key_id_env and secret_access_key_env must be paired"
        );
        ensure!(
            config.session_token_env.is_none() || config.access_key_id_env.is_some(),
            "DynamoDB session_token_env requires explicit credential references"
        );
        ensure!(
            config.profile.is_none() || config.access_key_id_env.is_none(),
            "Choose AWS profile or explicit credential references"
        );
        for name in [
            &config.access_key_id_env,
            &config.secret_access_key_env,
            &config.session_token_env,
        ]
        .into_iter()
        .flatten()
        {
            ensure!(
                safe_name(name),
                "Invalid AWS credential environment reference"
            );
        }
        for endpoint in [&config.endpoint_url, &config.streams_endpoint_url]
            .into_iter()
            .flatten()
        {
            let url = url::Url::parse(endpoint)?;
            ensure!(
                endpoint.len() <= 1024
                    && !endpoint.chars().any(char::is_control)
                    && matches!(url.scheme(), "https" | "http")
                    && url.host().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.path() == "/",
                "Invalid DynamoDB HTTP(S) endpoint; credentials, path, query and fragment are forbidden"
            );
            if url.scheme() == "http" {
                ensure!(
                    url.host_str().is_some_and(|host| host
                        .trim_matches(['[', ']'])
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())),
                    "DynamoDB HTTP requires a literal loopback address; use HTTPS remotely"
                );
                ensure!(
                    config.access_key_id_env.is_some(),
                    "Local DynamoDB requires explicit fixture credential references, not the AWS credential chain"
                );
            }
        }
        Ok(config)
    }

    pub fn credentials(
        &self,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<(Option<Credentials>, Vec<String>)> {
        let mut secrets = Vec::new();
        let mut value = |name: &str| -> Result<String> {
            let value = secret(name, env)?;
            ensure!(
                value.len() <= 65536 && !value.chars().any(char::is_control),
                "Invalid AWS credential value"
            );
            secrets.push(value.clone());
            Ok(value)
        };
        let credentials = if let Some(name) = &self.access_key_id_env {
            Some(Credentials::new(
                value(name)?,
                value(self.secret_access_key_env.as_deref().unwrap())?,
                self.session_token_env
                    .as_deref()
                    .map(&mut value)
                    .transpose()?,
                None,
                "onetui-config",
            ))
        } else {
            None
        };
        Ok((credentials, secrets))
    }

    pub async fn load(&self, credentials: Option<Credentials>) -> aws_config::SdkConfig {
        use aws_smithy_http_client::{
            Builder,
            tls::{Provider, rustls_provider::CryptoMode},
        };
        // Reuse the workspace's ring provider; enabling two Rustls defaults can panic.
        let http = Builder::new()
            .tls_provider(Provider::Rustls(CryptoMode::Ring))
            .build_https();
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(self.region.clone()))
            .http_client(http);
        if let Some(profile) = &self.profile {
            loader = loader.profile_name(profile);
        }
        if let Some(credentials) = credentials {
            loader = loader.credentials_provider(credentials);
        }
        loader.load().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_offline_config_and_explicit_local_credentials() {
        let config = Config::parse(&toml::from_str("region='eu-west-1'").unwrap()).unwrap();
        assert!(
            config
                .credentials(&|_| panic!("unselected secret"))
                .unwrap()
                .0
                .is_none()
        );
        for text in [
            "region='bad region'",
            "region='eu-west-1'\nextra=true",
            "region='eu-west-1'\naccess_key_id_env='KEY'",
            "region='eu-west-1'\nendpoint_url='http://127.0.0.1:8000'",
            "region='eu-west-1'\nendpoint_url='https://user:secret@example.com'",
            "region='eu-west-1'\nendpoint_url='http://example.com'",
        ] {
            assert!(
                Config::parse(&toml::from_str(text).unwrap()).is_err(),
                "{text}"
            );
        }
        let config=Config::parse(&toml::from_str("region='us-east-1'\nendpoint_url='http://127.0.0.1:8000'\naccess_key_id_env='KEY'\nsecret_access_key_env='SECRET'").unwrap()).unwrap();
        assert!(config.credentials(&|_| None).is_err());
        let (credentials, secrets) = config.credentials(&|name| Some(name.into())).unwrap();
        assert_eq!(credentials.unwrap().access_key_id(), "KEY");
        assert_eq!(secrets, ["KEY", "SECRET"]);
    }
}
