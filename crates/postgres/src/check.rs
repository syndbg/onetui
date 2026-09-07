use anyhow::{Result, anyhow, ensure};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;

pub(crate) fn postgres_config(dsn: &str, deadline: Duration) -> Result<tokio_postgres::Config> {
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

pub(crate) fn postgres_tls(
    config: &tokio_postgres::Config,
    ca_file: Option<&Path>,
) -> Result<MakeRustlsConnect> {
    Ok(if config.get_ssl_mode() == SslMode::Disable {
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
    })
}

pub async fn check(dsn: &str, ca_file: Option<&Path>, deadline: Duration) -> Result<String> {
    let config = postgres_config(dsn, deadline)?;
    let tls = postgres_tls(&config, ca_file)?;
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
    fn empty_or_invalid_ca_is_rejected() {
        let file = tempfile::NamedTempFile::new().unwrap();
        assert!(pg_tls(Some(file.path())).is_err());
        assert!(pg_tls(Some(Path::new("/nonexistent/onetui-ca.pem"))).is_err());
    }
}
