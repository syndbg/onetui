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
        let defaults = postgres_config("host=localhost", deadline).unwrap();
        assert!(defaults.get_keepalives());
        assert_eq!(defaults.get_keepalives_idle(), Duration::from_secs(7200));
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
