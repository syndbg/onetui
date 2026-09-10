#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::process::Command;

struct TrafficHarness {
    child: std::process::Child,
    root: tempfile::TempDir,
}

impl TrafficHarness {
    fn spawn(failure: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        for directory in ["hack", "bin", "target/debug/examples"] {
            std::fs::create_dir_all(root.path().join(directory)).unwrap();
        }
        std::fs::copy(
            concat!(env!("CARGO_MANIFEST_DIR"), "/hack/dev.sh"),
            root.path().join("hack/dev.sh"),
        )
        .unwrap();
        let scripts = [
            (
                "bin/docker",
                "#!/bin/bash\nif [[ $1 == context ]]; then printf 'unix:///fixture-test.sock\\n'; fi\n",
            ),
            (
                "bin/cargo",
                "#!/bin/bash\nprintf '%s\\n' \"$*\" >> builds\n",
            ),
            (
                "target/debug/onetui",
                "#!/bin/bash\nprintf '%s\\n' \"$*\" >> checks\nif [[ ${ONETUI_TEST_TRAFFIC_FAIL:-} == check ]]; then exit 19; fi\n",
            ),
            (
                "target/debug/examples/produce_demo",
                "#!/bin/bash\nprintf '%s' \"$$\" > kafka.pid\nif [[ ${ONETUI_TEST_TRAFFIC_FAIL:-} == kafka ]]; then while [[ ! -f nats.pid ]]; do sleep 0.01; done; exit 23; fi\nexec sleep 60\n",
            ),
            (
                "target/debug/examples/produce_nats",
                "#!/bin/bash\nprintf '%s' \"$$\" > nats.pid\nif [[ ${ONETUI_TEST_TRAFFIC_FAIL:-} == nats ]]; then while [[ ! -f kafka.pid ]]; do sleep 0.01; done; exit 24; fi\nexec sleep 60\n",
            ),
        ];
        for (name, script) in scripts {
            let path = root.path().join(name);
            std::fs::write(&path, script).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let child = Command::new("bash")
            .args(["hack/dev.sh", "traffic"])
            .current_dir(root.path())
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    root.path().join("bin").display(),
                    std::env::var("PATH").unwrap()
                ),
            )
            .env_remove("DOCKER_HOST")
            .env_remove("DOCKER_CONTEXT")
            .env("ONETUI_TEST_TRAFFIC_FAIL", failure)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        Self { child, root }
    }

    fn producers(&self) -> Vec<String> {
        ["kafka.pid", "nats.pid"]
            .iter()
            .filter_map(|name| std::fs::read_to_string(self.root.path().join(name)).ok())
            .collect()
    }

    fn wait(&mut self) -> std::process::ExitStatus {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "traffic supervisor did not exit"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
}

impl Drop for TrafficHarness {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .output();
        }
        // Also clean mock children if an assertion catches a broken supervisor.
        for pid in self.producers() {
            let _ = Command::new("kill").args(["-TERM", &pid]).output();
        }
        let _ = self.child.wait();
    }
}

#[test]
fn shared_traffic_starts_both_and_reaps_them_on_interrupt_or_failure() {
    for signal in ["-INT", "-TERM"] {
        let mut harness = TrafficHarness::spawn("");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while harness.producers().len() != 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "both producers must start"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let checks = std::fs::read_to_string(harness.root.path().join("checks")).unwrap();
        assert!(checks.contains("--connection local_kafka"));
        assert!(checks.contains("--connection local_nats"));
        let builds = std::fs::read_to_string(harness.root.path().join("builds")).unwrap();
        assert!(builds.contains("-p onetui-kafka --example produce_demo --locked"));
        assert!(builds.contains("-p onetui-nats --example produce_nats --locked"));
        assert!(
            Command::new("kill")
                .args([signal, &harness.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        assert_eq!(
            harness.wait().code(),
            Some(if signal == "-INT" { 130 } else { 143 })
        );
        for pid in harness.producers() {
            assert!(
                !Command::new("kill")
                    .args(["-0", &pid])
                    .output()
                    .unwrap()
                    .status
                    .success(),
                "producer survived shutdown"
            );
        }
    }
    for (failure, code) in [("kafka", 23), ("nats", 24), ("check", 19)] {
        let mut harness = TrafficHarness::spawn(failure);
        assert_eq!(harness.wait().code(), Some(code));
        if failure == "check" {
            assert!(harness.producers().is_empty());
        }
        for pid in harness.producers() {
            assert!(
                !Command::new("kill")
                    .args(["-0", &pid])
                    .output()
                    .unwrap()
                    .status
                    .success(),
                "sibling survived failure"
            );
        }
    }
}

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
