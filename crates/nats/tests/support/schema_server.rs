use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

pub struct Server {
    pub url: String,
    pub requests: Arc<Mutex<Vec<String>>>,
    pub ca_file: Option<String>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    _files: tempfile::TempDir,
}

impl Server {
    pub fn start(tls: bool, handler: impl Fn(&str) -> (u16, String) + Send + 'static) -> Self {
        Self::start_bytes(tls, move |request| {
            let (status, body) = handler(request);
            (status, body.into_bytes())
        })
    }

    pub fn start_bytes(
        tls: bool,
        handler: impl Fn(&str) -> (u16, Vec<u8>) + Send + 'static,
    ) -> Self {
        let files = tempfile::tempdir().unwrap();
        let cert = files.path().join("cert.pem");
        let key = files.path().join("key.pem");
        let tls = if tls {
            use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
            let config = files.path().join("openssl.cnf");
            std::fs::write(&config, "[req]\ndistinguished_name=dn\nx509_extensions=ext\nprompt=no\n[dn]\nCN=localhost\n[ext]\nsubjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n").unwrap();
            let output = std::process::Command::new("openssl")
                .args([
                    "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-config",
                ])
                .arg(config)
                .arg("-keyout")
                .arg(&key)
                .arg("-out")
                .arg(&cert)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let certs = CertificateDer::pem_file_iter(&cert)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let key = PrivateKeyDer::from_pem_file(&key).unwrap();
            Some(Arc::new(
                rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(certs, key)
                    .unwrap(),
            ))
        } else {
            None
        };
        let ca_file = tls.as_ref().map(|_| cert.to_str().unwrap().to_owned());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!(
            "{}://{}",
            if tls.is_some() { "https" } else { "http" },
            listener.local_addr().unwrap()
        );
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (done, seen) = (stop.clone(), requests.clone());
        let thread = thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        // macOS accepts can inherit the listener's nonblocking mode.
                        stream.set_nonblocking(false).unwrap();
                        stream
                            .set_read_timeout(Some(Duration::from_millis(300)))
                            .unwrap();
                        stream
                            .set_write_timeout(Some(Duration::from_millis(300)))
                            .unwrap();
                        if let Some(config) = &tls {
                            let conn = rustls::ServerConnection::new(config.clone()).unwrap();
                            serve(rustls::StreamOwned::new(conn, stream), &handler, &seen);
                        } else {
                            serve(stream, &handler, &seen);
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(e) => panic!("{e}"),
                }
            }
        });
        Self {
            url,
            requests,
            ca_file,
            stop,
            thread: Some(thread),
            _files: files,
        }
    }
}

fn serve(
    mut stream: impl Read + Write,
    handler: &impl Fn(&str) -> (u16, Vec<u8>),
    requests: &Mutex<Vec<String>>,
) {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") && request.len() < 16_384 {
        let mut b = [0];
        if stream.read_exact(&mut b).is_err() {
            return;
        }
        request.push(b[0]);
    }
    let headers = String::from_utf8(request.clone()).unwrap();
    let length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    assert!(length <= 16_384, "test request body exceeds bound");
    let mut body = vec![0; length];
    if stream.read_exact(&mut body).is_err() {
        return;
    }
    request.extend(body);
    let request = String::from_utf8(request).unwrap();
    requests.lock().unwrap().push(request.clone());
    let (status, body) = handler(&request);
    let location = if status == 302 {
        "Location: http://127.0.0.1:1/forbidden\r\n"
    } else {
        ""
    };
    let _ = write!(
        stream,
        "HTTP/1.1 {status} Test\r\n{location}Content-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(&body);
    let _ = stream.flush();
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}
