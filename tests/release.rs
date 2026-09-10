use std::process::Command;

#[test]
fn native_kafka_notices_match_locked_dependencies() {
    let lock: toml::Value = toml::from_str(include_str!("../Cargo.lock")).unwrap();
    let notices = include_str!("../THIRD_PARTY_NOTICES.md");
    for (name, version, attribution) in [
        ("rdkafka", "0.39.0", "Copyright (c) 2016 Federico Giraud"),
        (
            "rdkafka-sys",
            "4.10.0+2.12.1",
            "Copyright (c) 2012-2022, Magnus Edenhill",
        ),
        ("openssl-src", "300.6.1+3.6.3", "OpenSSL 3.6.3"),
        (
            "openssl-sys",
            "0.9.117",
            "openssl-src and openssl-sys Rust bindings",
        ),
        (
            "libz-sys",
            "1.1.29",
            "1995-2026 Jean-loup Gailly and Mark Adler",
        ),
        (
            "zstd-sys",
            "2.1.0+zstd.1.5.7",
            "Copyright (c) Meta Platforms, Inc.",
        ),
    ] {
        let package = lock["package"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["name"].as_str() == Some(name))
            .unwrap();
        assert_eq!(
            package["version"].as_str(),
            Some(version),
            "Review {name} notices after upgrading"
        );
        assert!(notices.contains(attribution), "Missing {name} attribution");
    }
    assert!(include_str!("../hack/release.sh").contains("LICENSE THIRD_PARTY_NOTICES.md"));
}

#[test]
fn release_tag_must_match_the_cargo_version() {
    let valid = format!("v{}", env!("CARGO_PKG_VERSION"));
    for tag in [
        valid.as_str(),
        "",
        "0.1.0",
        "v9.9.9",
        "v0.1.0-rc.1",
        "v0.1.0\n",
    ] {
        let output = Command::new("bash")
            .args(["hack/release.sh", "check", tag])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            tag == valid,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
