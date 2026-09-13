use aws_sdk_dynamodb::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
};
use aws_smithy_http_client::{
    Builder,
    tls::{Provider, rustls_provider::CryptoMode},
};

pub fn client() -> Client {
    Client::from_conf(
        aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url("http://127.0.0.1:18000")
            .credentials_provider(Credentials::new(
                "onetuiFixtureOnly",
                "fixture-secret-only",
                None,
                None,
                "onetui-fixtures",
            ))
            .http_client(
                Builder::new()
                    .tls_provider(Provider::Rustls(CryptoMode::Ring))
                    .build_https(),
            )
            .build(),
    )
}
