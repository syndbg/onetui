#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::process::Command;

#[test]
fn local_run_does_not_select_a_connection() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp.path().join("hack")).unwrap();
    std::fs::create_dir_all(temp.path().join("target/debug")).unwrap();
    std::fs::copy(
        concat!(env!("CARGO_MANIFEST_DIR"), "/hack/dev.sh"),
        temp.path().join("hack/dev.sh"),
    )
    .unwrap();
    let binary = temp.path().join("target/debug/onetui");
    std::fs::write(&binary, "#!/bin/bash\nprintf '%s\\n' \"$@\"\n").unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
    let output = Command::new("bash")
        .args(["hack/dev.sh", "run"])
        .current_dir(temp.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "--config\nhack/connections.toml\n"
    );
}

#[test]
fn fixture_setup_preserves_existing_containers_and_cleans_failed_startup() {
    let temp = tempfile::tempdir().unwrap();
    let docker = temp.path().join("docker");
    std::fs::write(
        &docker,
        r#"#!/bin/bash
set -eu
printf '%s\n' "$*" >> "$ONETUI_TEST_DOCKER_LOG"
case "$*" in
    'context inspect '*)
        if [[ "$ONETUI_TEST_DOCKER_STATE" == remote ]]; then
            printf 'ssh://example.invalid\n'
        else
            printf 'unix:///fixture-test.sock\n'
        fi ;;
    info|*' version') ;;
    *' ps --all --quiet')
        if [[ "$ONETUI_TEST_DOCKER_STATE" == existing ]]; then printf 'existing-fixture\n'; fi ;;
    *' up -d --wait --wait-timeout 60') exit 27 ;;
    *' logs --no-color --tail 100'|*' down --timeout 10') ;;
    *) exit 99 ;;
esac
"#,
    )
    .unwrap();
    std::fs::set_permissions(&docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    for state in ["existing", "failed-start", "remote"] {
        let log = temp.path().join(state);
        let output = Command::new("bash")
            .args(["hack/dev.sh", "test"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    temp.path().display(),
                    std::env::var("PATH").unwrap()
                ),
            )
            .env_remove("DOCKER_HOST")
            .env_remove("DOCKER_CONTEXT")
            .env("ONETUI_TEST_DOCKER_LOG", &log)
            .env("ONETUI_TEST_DOCKER_STATE", state)
            .output()
            .unwrap();
        let error = String::from_utf8_lossy(&output.stderr);
        let calls = std::fs::read_to_string(log).unwrap();
        assert!(!output.status.success(), "{error}");
        if state == "failed-start" {
            assert_eq!(output.status.code(), Some(27), "{error}");
            assert!(calls.contains(" up -d "));
            assert!(calls.contains(" logs --no-color "));
            assert!(calls.contains(" down --timeout 10"));
        } else {
            assert!(!calls.contains(" up -d "));
            assert!(!calls.contains(" down --timeout 10"));
            assert!(error.contains(if state == "remote" {
                "local Unix-socket"
            } else {
                "refusing to reset"
            }));
        }
    }
}
