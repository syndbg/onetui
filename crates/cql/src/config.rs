use std::path::PathBuf;

use anyhow::{Result, anyhow, ensure};
use onetui_core::config::safe_name;
use serde::Deserialize;

/// Contact points are addresses only: the driver discovers the rest of the cluster from
/// them. A bounded list keeps a typo from becoming a wide connection attempt.
const MAX_NODES: usize = 32;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub nodes: Vec<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub username_env: Option<String>,
    #[serde(default)]
    pub password_env: Option<String>,
    #[serde(default)]
    pub tls: bool,
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
    /// Default keyspace, so unqualified table names in the editor resolve against it.
    /// Access control stays with the server's roles, not with this setting.
    #[serde(default)]
    pub keyspace: Option<String>,
}

impl Config {
    pub fn parse(options: &toml::Table) -> Result<Self> {
        let config: Self = options.clone().try_into().map_err(|_| {
            anyhow!(
                "Invalid CQL config; expected nodes and optional username, password, username_env, password_env, tls, ca_file and keyspace"
            )
        })?;
        ensure!(
            (1..=MAX_NODES).contains(&config.nodes.len()),
            "CQL nodes requires 1..{MAX_NODES} host:port contact points"
        );
        for node in &config.nodes {
            parsed(node)?;
        }
        ensure!(
            !config.username.as_deref().is_some_and(str::is_empty)
                && !config.password.as_deref().is_some_and(str::is_empty),
            "CQL username and password cannot be empty"
        );
        ensure!(
            !(config.username.is_some() && config.username_env.is_some())
                && !(config.password.is_some() && config.password_env.is_some()),
            "CQL username and password each need one source: a value or an _env reference"
        );
        ensure!(
            (config.username.is_some() || config.username_env.is_some())
                == (config.password.is_some() || config.password_env.is_some()),
            "CQL username and password must be configured together"
        );
        for name in [&config.username_env, &config.password_env]
            .into_iter()
            .flatten()
        {
            ensure!(
                safe_name(name),
                "Invalid CQL credential environment reference"
            );
        }
        if let Some(path) = &config.ca_file {
            ensure!(
                config.tls,
                "CQL ca_file requires tls = true; a trust root is unused on a plaintext connection"
            );
            ensure!(
                !path.as_os_str().is_empty() && path.is_absolute(),
                "CQL ca_file must be an absolute path"
            );
        }
        // Credentials travel in the CQL handshake, so a plaintext remote connection would
        // put them on the wire. Loopback stays allowed for local development.
        if !config.tls && (config.username.is_some() || config.username_env.is_some()) {
            for node in &config.nodes {
                ensure!(
                    loopback(node)?,
                    "CQL credentials require tls = true except on loopback contact points"
                );
            }
        }
        Ok(config)
    }
}

/// A contact point is `host:port` with no URL parts around it. Parsing through a URL
/// borrows its host and port handling, including bracketed IPv6.
fn parsed(node: &str) -> Result<url::Url> {
    ensure!(
        node.len() <= 255 && !node.chars().any(char::is_whitespace),
        "CQL contact point must be host:port within 255 bytes"
    );
    let url = url::Url::parse(&format!("cql://{node}"))
        .map_err(|_| anyhow!("Invalid CQL contact point {node}; use host:port"))?;
    ensure!(
        !node.contains(['/', '?', '#', '@', ',', '\\'])
            && url.host().is_some()
            && url.port().is_some_and(|port| port != 0)
            && url.username().is_empty()
            && url.password().is_none(),
        "CQL contact points must be host:port without credentials or URL components"
    );
    Ok(url)
}

fn loopback(node: &str) -> Result<bool> {
    Ok(match parsed(node)?.host() {
        Some(url::Host::Domain(host)) => {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        }
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    })
}

/// Quote a keyspace, table or column name for interpolation. Unquoted CQL names fold to
/// lower case; a quoted name keeps its case and may hold any character with `"` doubled,
/// so quoting reaches every name the server has while keeping it one token.
pub(crate) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(text: &str) -> toml::Table {
        text.parse().expect("TOML fixture")
    }

    #[test]
    fn contact_points_are_bounded_addresses_without_url_parts() {
        let config = Config::parse(&table("nodes = ['127.0.0.1:9042', '[::1]:9042']")).unwrap();
        assert_eq!(config.nodes.len(), 2);
        assert!(!config.tls && config.keyspace.is_none());

        for text in [
            "nodes = []",
            "nodes = ['127.0.0.1']",
            "nodes = ['127.0.0.1:0']",
            "nodes = ['user:pass@127.0.0.1:9042']",
            "nodes = ['127.0.0.1:9042/path']",
            "nodes = ['127.0.0.1:9042?query']",
            "nodes = ['host with space:9042']",
            "nodes = ['127.0.0.1:9042']\nunknown = true",
        ] {
            assert!(Config::parse(&table(text)).is_err(), "{text}");
        }
        // The bound is on the list, not just each entry.
        let many = (0..=MAX_NODES)
            .map(|n| format!("'10.0.0.{n}:9042'"))
            .collect::<Vec<_>>()
            .join(",");
        assert!(Config::parse(&table(&format!("nodes = [{many}]"))).is_err());
    }

    #[test]
    fn credentials_are_paired_and_need_tls_off_loopback() {
        let local = "nodes = ['127.0.0.1:9042']\nusername_env = 'U'\npassword_env = 'P'";
        assert!(Config::parse(&table(local)).is_ok());
        assert!(
            Config::parse(&table(
                "nodes=['127.0.0.1:9042']\nusername='user'\npassword='secret'"
            ))
            .is_ok()
        );
        assert!(
            Config::parse(&table(
                "nodes=['127.0.0.1:9042']\nusername='user'\npassword_env='P'"
            ))
            .is_ok()
        );
        // localhost by name is loopback too.
        assert!(
            Config::parse(&table(
                "nodes = ['localhost:9042']\nusername_env = 'U'\npassword_env = 'P'"
            ))
            .is_ok()
        );
        // Credentials would cross the network in the clear without TLS.
        let remote = "nodes = ['db.example:9042']\nusername_env = 'U'\npassword_env = 'P'";
        assert!(Config::parse(&table(remote)).is_err());
        assert!(Config::parse(&table(&format!("{remote}\ntls = true"))).is_ok());
        // A mixed list is only as safe as its weakest contact point.
        assert!(
            Config::parse(&table(
                "nodes = ['127.0.0.1:9042', 'db.example:9042']\nusername_env = 'U'\npassword_env = 'P'"
            ))
            .is_err()
        );

        for text in [
            "nodes = ['127.0.0.1:9042']\nusername_env = 'U'",
            "nodes = ['127.0.0.1:9042']\npassword_env = 'P'",
            "nodes = ['127.0.0.1:9042']\nusername_env = 'bad name'\npassword_env = 'P'",
            "nodes = ['127.0.0.1:9042']\nusername = 'user'",
            "nodes = ['127.0.0.1:9042']\nusername = 'user'\nusername_env = 'U'\npassword = 'secret'",
            "nodes = ['127.0.0.1:9042']\nusername = 'user'\npassword = 'secret'\npassword_env = 'P'",
            "nodes = ['remote:9042']\nusername = 'user'\npassword = 'secret'",
        ] {
            assert!(Config::parse(&table(text)).is_err(), "{text}");
        }
    }

    #[test]
    fn trust_roots_need_tls() {
        assert!(
            Config::parse(&table(
                "nodes = ['db.example:9042']\ntls = true\nca_file = '/etc/ca.pem'"
            ))
            .is_ok()
        );
        for text in [
            // A trust root without TLS is a configuration the connection would ignore.
            "nodes = ['db.example:9042']\nca_file = '/etc/ca.pem'",
            "nodes = ['db.example:9042']\ntls = true\nca_file = 'relative.pem'",
            "nodes = ['db.example:9042']\ntls = true\nca_file = ''",
        ] {
            assert!(Config::parse(&table(text)).is_err(), "{text}");
        }
    }

    #[test]
    fn quoting_keeps_any_name_one_token() {
        assert_eq!(quote("events"), r#""events""#);
        assert_eq!(quote("Mixed Case"), r#""Mixed Case""#);
        // An embedded quote is doubled, so it cannot end the identifier early.
        assert_eq!(quote(r#"a"; DROP TABLE x"#), r#""a""; DROP TABLE x""#);
    }
}
