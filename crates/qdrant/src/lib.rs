use anyhow::{Result, anyhow, ensure};
mod config;
mod provider;
pub use provider::{QdrantExecutor, QdrantProvider};
use std::net::IpAddr;

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

fn rpc_error(status: tonic::Status) -> anyhow::Error {
    match status.code() {
        tonic::Code::Unauthenticated => anyhow!("Qdrant authentication failed; check api_key_env"),
        tonic::Code::PermissionDenied => {
            anyhow!("Qdrant collection listing denied; the key may be collection-scoped")
        }
        tonic::Code::OutOfRange => anyhow!(
            "Qdrant metadata response exceeded the 1 MiB limit, or the server rejected an out-of-range request"
        ),
        tonic::Code::ResourceExhausted => anyhow!(
            "Qdrant response exceeded the 1 MiB metadata limit or server resources were exhausted"
        ),
        tonic::Code::DeadlineExceeded => anyhow!("Qdrant metadata check timed out"),
        _ => anyhow!(
            "Qdrant metadata check failed (gRPC code {})",
            status.code() as i32
        ),
    }
}

pub(crate) fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "id": "qdrant", "operations": ["check"], "resources": [],
        "session": "Lazy reusable size-capped gRPC channel; failed/cancelled checks discard it. Shutdown drops the channel. HTTP/2 keepalive interval unset, idle pings disabled; no periodic metadata check or heartbeat TOML setting.",
        "configuration": {
            "kind": {"required": true, "values": ["qdrant"], "purpose": "Select the Qdrant connector"},
            "url": {"required": true, "type": "HTTP(S) gRPC URL", "purpose": "Explicit endpoint; plaintext only on loopback; no URL credentials, path prefix, query or fragment", "example": "http://127.0.0.1:6334"},
            "api_key_env": {"required": false, "type": "string", "default": "no API key", "purpose": "Environment variable containing the API key", "values": "Nonempty ASCII letters, digits, underscores or hyphens", "example": "ONETUI_QDRANT_API_KEY"}
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
