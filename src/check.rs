use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Result, anyhow, ensure};
use qdrant_client::qdrant::{ListCollectionsRequest, collections_client::CollectionsClient};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;
use tonic::transport::{ClientTlsConfig, Endpoint};

use crate::config::ResolvedConnection;

pub async fn check(connection: &ResolvedConnection, deadline: Duration) -> Result<String> {
    match connection {
        ResolvedConnection::Postgres { url, ca_file } => {
            postgres(url, ca_file.as_deref(), deadline).await
        }
        ResolvedConnection::Qdrant { url, api_key } => {
            qdrant(url, api_key.as_deref(), deadline).await
        }
    }
}

fn postgres_config(dsn: &str, deadline: Duration) -> Result<tokio_postgres::Config> {
    let mut config: tokio_postgres::Config = dsn.parse()
        .map_err(|_| anyhow!("invalid PostgreSQL connection string; supported sslmode values are disable, prefer and require"))?;
    ensure!(
        !config.get_hosts().is_empty(),
        "PostgreSQL connection must specify a host"
    );
    if config.get_ssl_mode() == SslMode::Disable {
        let local = config.get_hosts().iter().all(|host| match host {
            Host::Tcp(host) => is_loopback(host),
            #[cfg(unix)]
            Host::Unix(_) => true,
        }) && config.get_hostaddrs().iter().all(IpAddr::is_loopback);
        ensure!(
            local,
            "plaintext PostgreSQL is allowed only for loopback hosts or local Unix sockets"
        );
    } else {
        // The driver's default Prefer can fall back to plaintext. Never allow that downgrade.
        config.ssl_mode(SslMode::Require);
    }
    config.connect_timeout(deadline);
    config.application_name("onetui-check");
    config.options(format!(
        "-c default_transaction_read_only=on -c statement_timeout={}",
        deadline.as_millis()
    ));
    Ok(config)
}

fn pg_tls(ca_file: Option<&Path>) -> Result<MakeRustlsConnect> {
    let mut roots = rustls::RootCertStore::empty();
    if let Some(path) = ca_file {
        let certs = CertificateDer::pem_file_iter(path)
            .map_err(|_| anyhow!("cannot read PostgreSQL ca_file as PEM certificates"))?;
        for cert in certs {
            roots
                .add(cert.map_err(|_| anyhow!("invalid certificate in PostgreSQL ca_file"))?)
                .map_err(|_| anyhow!("invalid trust anchor in PostgreSQL ca_file"))?;
        }
    } else {
        let native = rustls_native_certs::load_native_certs();
        roots.add_parsable_certificates(native.certs);
    }
    ensure!(
        !roots.is_empty(),
        "no usable PostgreSQL CA certificates; configure an absolute ca_file path"
    );
    Ok(MakeRustlsConnect::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

async fn postgres(dsn: &str, ca_file: Option<&Path>, deadline: Duration) -> Result<String> {
    let config = postgres_config(dsn, deadline)?;
    let tls = if config.get_ssl_mode() == SslMode::Disable {
        ensure!(
            ca_file.is_none(),
            "ca_file cannot be combined with sslmode=disable"
        );
        MakeRustlsConnect::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        )
    } else {
        pg_tls(ca_file)?
    };
    let (client, connection) = config.connect(tls).await.map_err(pg_error)?;
    // Poll the connection here instead of detaching a task: cancellation drops the socket too.
    tokio::pin!(connection);
    let row = tokio::select! {
        result = client.query_one("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE pg_catalog.has_schema_privilege(oid, 'USAGE'))", &[]) => result.map_err(pg_error)?,
        result = &mut connection => {
            return Err(result.err().map(pg_error).unwrap_or_else(|| anyhow!("PostgreSQL connection closed before metadata check completed")));
        }
    };
    let accessible: bool = row.try_get(0).map_err(pg_error)?;
    Ok(if accessible {
        "schema metadata readable"
    } else {
        "connected; no accessible schemas"
    }
    .into())
}

fn pg_error(error: tokio_postgres::Error) -> anyhow::Error {
    match error.code().map(|code| code.code()) {
        Some(code) if code.starts_with("28") => {
            anyhow!("PostgreSQL authentication failed; check the referenced credentials")
        }
        Some("42501") => anyhow!("PostgreSQL metadata access denied"),
        Some("57014") => anyhow!("PostgreSQL metadata check timed out or was cancelled"),
        Some(code) if code.len() == 5 && code.bytes().all(|c| c.is_ascii_alphanumeric()) => {
            anyhow!("PostgreSQL server rejected the check (SQLSTATE {code})")
        }
        Some(_) => anyhow!("PostgreSQL server returned an invalid SQLSTATE"),
        None => anyhow!(
            "PostgreSQL connection/check failed; verify endpoint, reachability and TLS certificate trust"
        ),
    }
}

fn is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

fn qdrant_url(value: &str) -> Result<url::Url> {
    let url = url::Url::parse(value).map_err(|_| anyhow!("invalid Qdrant URL"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https") && url.host().is_some(),
        "Qdrant requires an http:// or https:// gRPC endpoint"
    );
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && matches!(url.path(), "" | "/"),
        "Qdrant URL must not contain credentials, a path, query or fragment; use api_key_env"
    );
    ensure!(
        url.port_or_known_default().is_some(),
        "Qdrant URL requires a valid port"
    );
    if url.scheme() == "http" {
        let local = match url.host() {
            Some(url::Host::Domain(host)) => is_loopback(host),
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            None => false,
        };
        ensure!(
            local,
            "plaintext Qdrant is allowed only for loopback hosts; use https:// remotely"
        );
    }
    Ok(url)
}

async fn qdrant(value: &str, api_key: Option<&str>, deadline: Duration) -> Result<String> {
    let url = qdrant_url(value)?;
    let mut request = tonic::Request::new(ListCollectionsRequest {});
    request.set_timeout(deadline);
    if let Some(key) = api_key {
        request.metadata_mut().insert(
            "api-key",
            key.parse()
                .map_err(|_| anyhow!("Qdrant API key is not valid ASCII metadata"))?,
        );
    }
    let mut endpoint = Endpoint::from_shared(url.to_string())
        .map_err(|_| anyhow!("invalid Qdrant gRPC endpoint"))?
        .connect_timeout(deadline)
        .timeout(deadline);
    if url.scheme() == "https" {
        endpoint = endpoint
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|_| anyhow!("cannot configure Qdrant TLS using native trust roots"))?;
    }
    let channel = endpoint.connect().await.map_err(|_| {
        anyhow!(
            "Qdrant connection failed; verify gRPC endpoint, reachability and TLS certificate trust"
        )
    })?;
    // The SDK's high-level collections call uses usize::MAX; keep this metadata check bounded.
    let response = CollectionsClient::new(channel).max_decoding_message_size(1024 * 1024)
        .list(request).await.map_err(|status| match status.code() {
            tonic::Code::Unauthenticated => anyhow!("Qdrant authentication failed; check api_key_env"),
            tonic::Code::PermissionDenied => anyhow!("Qdrant collection listing denied; the key may be collection-scoped"),
            tonic::Code::OutOfRange => anyhow!("Qdrant metadata response exceeded the 1 MiB limit, or the server rejected an out-of-range request"),
            tonic::Code::ResourceExhausted => anyhow!("Qdrant response exceeded the 1 MiB metadata limit or server resources were exhausted"),
            tonic::Code::DeadlineExceeded => anyhow!("Qdrant metadata check timed out"),
            _ => anyhow!("Qdrant metadata check failed (gRPC code {})", status.code() as i32),
        })?;
    Ok(format!(
        "collection metadata readable ({} collections)",
        response.into_inner().collections.len()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_never_downgrades_tls() {
        let deadline = Duration::from_secs(5);
        for dsn in ["host=example.com", "host=localhost sslmode=prefer"] {
            assert_eq!(
                postgres_config(dsn, deadline).unwrap().get_ssl_mode(),
                SslMode::Require
            );
        }
        assert!(postgres_config("host=127.0.0.1 sslmode=disable", deadline).is_ok());
        assert!(postgres_config("host=example.com sslmode=disable", deadline).is_err());
        assert!(
            postgres_config(
                "host=localhost hostaddr=192.0.2.1 sslmode=disable",
                deadline
            )
            .is_err()
        );
        assert!(postgres_config("host=127.0.0.1,example.com sslmode=disable", deadline).is_err());
        let error = postgres_config("fake-secret", deadline)
            .err()
            .unwrap()
            .to_string();
        assert!(!error.contains("fake-secret"));
    }

    #[test]
    fn qdrant_endpoint_validation() {
        for url in [
            "http://127.0.0.1:6334",
            "http://[::1]:6334",
            "https://example.com:6334",
        ] {
            assert!(qdrant_url(url).is_ok(), "{url}");
        }
        for url in [
            "http://example.com:6334",
            "https://user:fake-secret@example.com",
            "https://example.com?api_key=fake-secret",
            "https://example.com/path",
            "file:///tmp/data",
        ] {
            let error = qdrant_url(url).unwrap_err().to_string();
            assert!(!error.contains("fake-secret"));
        }
    }

    #[test]
    fn empty_or_invalid_ca_is_rejected() {
        let file = tempfile::NamedTempFile::new().unwrap();
        assert!(pg_tls(Some(file.path())).is_err());
        assert!(pg_tls(Some(Path::new("/nonexistent/onetui-ca.pem"))).is_err());
    }
}
