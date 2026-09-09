use anyhow::{Context, Result, anyhow, ensure};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::io::Read;
use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;

pub(crate) const CA_BYTES: u64 = 1024 * 1024;
// ponytail: one trust load per process; a stalled OS read cannot spawn more jobs on retry.
static TRUST_LOAD: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

async fn load_trust(
    load: impl FnOnce() -> Result<MakeRustlsConnect> + Send + 'static,
) -> Result<MakeRustlsConnect> {
    let permit = TRUST_LOAD
        .acquire()
        .await
        .expect("trust semaphore stays open");
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        load()
    })
    .await
    .map_err(|_| anyhow!("PostgreSQL trust loading failed"))?
}

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
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // Check the opened descriptor, not a path that could be swapped for a FIFO.
            options.custom_flags(nix::libc::O_NONBLOCK | nix::libc::O_NOCTTY);
        }
        let file = options
            .open(path)
            .context("cannot read PostgreSQL ca_file")?;
        let metadata = file
            .metadata()
            .context("cannot inspect PostgreSQL ca_file")?;
        ensure!(
            metadata.is_file(),
            "PostgreSQL ca_file must be a regular file"
        );
        ensure!(
            metadata.len() <= CA_BYTES,
            "PostgreSQL ca_file exceeds the 1 MiB limit"
        );
        let mut bytes = Vec::new();
        file.take(CA_BYTES + 1)
            .read_to_end(&mut bytes)
            .context("cannot read PostgreSQL ca_file")?;
        ensure!(
            bytes.len() as u64 <= CA_BYTES,
            "PostgreSQL ca_file exceeds the 1 MiB limit"
        );
        for cert in CertificateDer::pem_slice_iter(&bytes) {
            roots
                .add(cert.context("invalid certificate in PostgreSQL ca_file")?)
                .context("invalid trust anchor in PostgreSQL ca_file")?;
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

pub(crate) async fn postgres_tls(
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
        let path = ca_file.map(Path::to_path_buf);
        load_trust(move || pg_tls(path.as_deref())).await?
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

    #[test]
    fn ca_files_reject_directories_devices_and_oversized_contents() {
        let directory = tempfile::tempdir().unwrap();
        assert!(
            pg_tls(Some(directory.path()))
                .err()
                .unwrap()
                .to_string()
                .contains("regular file")
        );
        #[cfg(unix)]
        assert!(
            pg_tls(Some(Path::new("/dev/zero")))
                .err()
                .unwrap()
                .to_string()
                .contains("regular file")
        );
        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file().set_len(CA_BYTES + 1).unwrap();
        assert!(
            pg_tls(Some(file.path()))
                .err()
                .unwrap()
                .to_string()
                .contains("1 MiB")
        );
    }

    #[tokio::test]
    async fn cancelled_trust_loading_keeps_one_slot_until_os_work_finishes() {
        use onetui_core::provider::RequestContext;
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (cancel, mut context) = RequestContext::new(Duration::from_secs(3));
        let first = tokio::spawn(async move {
            context
                .run(load_trust(move || {
                    let _ = started.send(());
                    let _ = blocked.recv();
                    Err(anyhow!("test trust read finished"))
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
                .run(load_trust(move || {
                    marker.store(true, Ordering::SeqCst);
                    Err(anyhow!("should not run while previous OS read is blocked"))
                }))
                .await
                .is_err()
        );
        assert!(!entered.load(Ordering::SeqCst));
        release.send(()).unwrap();
        let (_cancel, mut context) = RequestContext::new(Duration::from_secs(1));
        let result = context
            .run(load_trust(|| Err(anyhow!("slot released"))))
            .await
            .unwrap();
        assert_eq!(result.err().unwrap().to_string(), "slot released");
    }
}
