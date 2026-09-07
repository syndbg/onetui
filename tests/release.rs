use std::process::Command;

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
