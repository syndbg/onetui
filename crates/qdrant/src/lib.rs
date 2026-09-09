use anyhow::{Result, anyhow, ensure};
mod browse;
mod config;
mod provider;
mod query;
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
    // Status Display/Debug includes metadata; only the code and message belong in diagnostics.
    anyhow!(
        "Qdrant [gRPC {:?} ({})] {}",
        status.code(),
        status.code() as i32,
        status.message()
    )
}

pub(crate) fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "id": "qdrant", "operations": ["check", "fetch_page", "query_page"],
        "query_syntax": {
            "operation": "Filtered Scroll on the current or selected collection; IDs only, payload and vectors fetched on demand",
            "fields": {"filter": "optional object: must, should, must_not arrays of field conditions", "limit": "optional integer 1..100; default 100"},
            "condition": "Nonempty key plus exactly one of match: {value: string|boolean|i64} or range: {gt?, gte?, lt?, lte?}; range requires a numeric bound",
            "unsupported": "Unknown fields, nested conditions, match any/except/text, geo/datetime filters, order_by, user offsets, payload/vector selectors and similarity queries are rejected"
        },
        "session": "Lazy reusable size-capped gRPC channel; failed/cancelled operations discard it. Shutdown drops the channel. HTTP/2 keepalive interval unset, idle pings disabled; no periodic metadata check or heartbeat TOML setting.",
        "limits": {"page_rows": 100, "rpc_bytes": 1048576, "display_page_bytes": 1048576, "retained_pages_per_view": 3},
        "navigation": "Enter: collections -> collection -> points or metadata; points -> point -> payload or vectors. Payload and vectors are separate reads. Enter on a data row inspects cached fields; h/l selects fields.",
        "paths": {"qdrant.collections": [], "qdrant.collection": ["collection"], "qdrant.metadata": ["collection"], "qdrant.points": ["collection"], "qdrant.point": ["collection", "numeric ID or hyphenated UUID"], "qdrant.payload": ["collection", "ID"], "qdrant.vectors": ["collection", "ID"]},
        "paging": "ID-only Scroll uses the exact server continuation, scoped to executor and resource; refresh restarts. Collections re-read the size-capped List response and display 100 sorted names per page. Neither provides a cross-request snapshot. Filter/sort only inspect displayed text; no payload path expressions or server-side filters.",
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
