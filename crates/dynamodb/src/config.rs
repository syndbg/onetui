use anyhow::{Result, anyhow, ensure};
use aws_sdk_dynamodb::config::{Credentials, Region};
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use onetui_core::config::{safe_name, secret};
use serde::Deserialize;
use std::time::Duration;

// Keep cancelled trust loading from accumulating threads behind a stalled OS read.
static TRUST_LOAD: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(5);

async fn load_tls_capable_http_client(
    build: impl FnOnce() -> SharedHttpClient + Send + 'static,
) -> Result<SharedHttpClient> {
    let permit = TRUST_LOAD
        .acquire()
        .await
        .expect("trust semaphore stays open");
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        build()
    })
    .await
    .map_err(|error| anyhow!("DynamoDB trust loading failed: {error}"))
}

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

    pub async fn load(&self, credentials: Option<Credentials>) -> Result<aws_config::SdkConfig> {
        use aws_smithy_http_client::{
            Connector,
            tls::{Provider, rustls_provider::CryptoMode},
        };
        use aws_smithy_runtime_api::client::http::{
            HttpConnectorSettings, SharedHttpConnector, http_client_fn,
        };
        let settings = || {
            HttpConnectorSettings::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .read_timeout(READ_TIMEOUT)
                .build()
        };
        let plain_http_only = self.endpoint_url.as_deref().is_some_and(|endpoint| {
            url::Url::parse(endpoint).is_ok_and(|url| url.scheme() == "http")
        }) && self.streams_endpoint_url.as_deref().is_none_or(|endpoint| {
            url::Url::parse(endpoint).is_ok_and(|url| url.scheme() == "http")
        });
        let http_client = if plain_http_only {
            // Plain HTTP is allowed only for validated loopback endpoints and needs no TLS roots.
            let connector = SharedHttpConnector::new(
                Connector::builder()
                    .sleep_impl(aws_smithy_async::rt::sleep::TokioSleep::new())
                    .connector_settings(settings())
                    .build_http(),
            );
            http_client_fn(move |_, _| connector.clone())
        } else {
            load_tls_capable_http_client(move || {
                // HTTPS, including over loopback, needs TLS and native roots. This connector
                // also supports plain HTTP when the two configured endpoints use mixed schemes.
                let connector = SharedHttpConnector::new(
                    Connector::builder()
                        .tls_provider(Provider::Rustls(CryptoMode::Ring))
                        .sleep_impl(aws_smithy_async::rt::sleep::TokioSleep::new())
                        .connector_settings(settings())
                        .build(),
                );
                http_client_fn(move |_, _| connector.clone())
            })
            .await?
        };
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(self.region.clone()))
            // Browsing and checks keep SDK retries; query clients override this to disable them.
            .retry_config(
                aws_sdk_dynamodb::config::retry::RetryConfig::standard().with_max_attempts(3),
            )
            .timeout_config(
                aws_sdk_dynamodb::config::timeout::TimeoutConfig::builder()
                    .connect_timeout(CONNECT_TIMEOUT)
                    .read_timeout(READ_TIMEOUT)
                    .operation_timeout(Duration::from_secs(10))
                    .build(),
            )
            .http_client(http_client);
        if let Some(profile) = &self.profile {
            loader = loader.profile_name(profile);
        }
        if let Some(credentials) = credentials {
            loader = loader.credentials_provider(credentials);
        }
        Ok(loader.load().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_trust_loading_retains_slot_until_native_work_finishes() {
        use onetui_core::provider::RequestContext;
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        fn unused_tls_http_client() -> SharedHttpClient {
            aws_smithy_http_client::Builder::new()
                .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                    aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
                ))
                .build_https()
        }

        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (cancel, mut context) = RequestContext::new(Duration::from_secs(3));
        let first = tokio::spawn(async move {
            context
                .run(load_tls_capable_http_client(move || {
                    let _ = started.send(());
                    let _ = blocked.recv();
                    unused_tls_http_client()
                }))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), ready)
            .await
            .unwrap()
            .unwrap();
        cancel.send(()).unwrap();
        assert!(first.await.unwrap().is_err());

        let entered = Arc::new(AtomicBool::new(false));
        let marker = entered.clone();
        let (_cancel, mut context) = RequestContext::new(Duration::from_millis(30));
        assert!(
            context
                .run(load_tls_capable_http_client(move || {
                    marker.store(true, Ordering::SeqCst);
                    unused_tls_http_client()
                }))
                .await
                .is_err()
        );
        assert!(!entered.load(Ordering::SeqCst));

        release.send(()).unwrap();
        let (_cancel, mut context) = RequestContext::new(Duration::from_secs(1));
        context
            .run(load_tls_capable_http_client(unused_tls_http_client))
            .await
            .unwrap()
            .unwrap();
    }

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
        Config::parse(
            &toml::from_str("region='us-east-1'\nendpoint_url='https://127.0.0.1:8000'").unwrap(),
        )
        .unwrap();
    }
}
