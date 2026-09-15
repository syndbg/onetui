#![cfg(unix)]

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct Fixture {
    root: tempfile::TempDir,
    prefix: PathBuf,
}

fn executable(path: &Path, content: &str) {
    fs::write(path, content).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let prefix = root.path().join("install with spaces");
        fs::create_dir(root.path().join("tools")).unwrap();
        fs::create_dir(root.path().join("assets")).unwrap();
        executable(
            &root.path().join("tools/curl"),
            r#"#!/bin/sh
set -eu
output=
url=
while [ "$#" -gt 0 ]; do
    case "$1" in
        -o) output=$2; shift 2 ;;
        --proto|--proto-redir|--connect-timeout|--max-time|-w) shift 2 ;;
        https://*) url=$1; shift ;;
        *) shift ;;
    esac
done
case "$url" in
    https://github.com/syndbg/onetui/releases/latest)
        printf 'https://github.com/syndbg/onetui/releases/tag/v0.1.0' ;;
    https://github.com/syndbg/onetui/releases/download/*)
        cp "$TEST_ASSETS/${url##*/}" "$output" ;;
    *) exit 99 ;;
esac
"#,
        );
        executable(
            &root.path().join("tools/uname"),
            "#!/bin/sh\ncase $1 in -s) echo \"$TEST_OS\" ;; -m) echo \"$TEST_ARCH\" ;; esac\n",
        );
        Self { root, prefix }
    }

    fn archive(&self, tag: &str, binary_version: &str, target: &str) -> PathBuf {
        let source = tempfile::tempdir_in(self.root.path()).unwrap();
        executable(
            &source.path().join("onetui"),
            &format!("#!/bin/sh\nprintf '%s\\n' 'onetui {binary_version}'\n"),
        );
        fs::write(source.path().join("LICENSE"), "license").unwrap();
        fs::write(source.path().join("THIRD_PARTY_NOTICES.md"), "notices").unwrap();
        let archive = self
            .root
            .path()
            .join(format!("assets/onetui-{tag}-{target}.tar.gz"));
        assert!(
            Command::new("tar")
                .arg("-czf")
                .arg(&archive)
                .args(["-C"])
                .arg(source.path())
                .args(["onetui", "LICENSE", "THIRD_PARTY_NOTICES.md"])
                .status()
                .unwrap()
                .success()
        );
        let checksum = Command::new("shasum")
            .args(["-a", "256"])
            .arg(&archive)
            .output()
            .unwrap();
        assert!(checksum.status.success());
        fs::write(archive.with_extension("gz.sha256"), checksum.stdout).unwrap();
        archive
    }

    fn run(&self, os: &str, arch: &str, args: &[&str]) -> Output {
        let paths = std::env::join_paths(
            [self.root.path().join("tools")]
                .into_iter()
                .chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
        )
        .unwrap();
        Command::new("sh")
            .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/install.sh"))
            .arg("--prefix")
            .arg(&self.prefix)
            .args(args)
            .env("PATH", paths)
            .env("TEST_OS", os)
            .env("TEST_ARCH", arch)
            .env("TEST_ASSETS", self.root.path().join("assets"))
            .output()
            .unwrap()
    }

    fn existing(&self) -> PathBuf {
        fs::create_dir_all(self.prefix.join("bin")).unwrap();
        let binary = self.prefix.join("bin/onetui");
        fs::write(&binary, "previous installation").unwrap();
        binary
    }
}

#[test]
fn installs_each_supported_archive_and_upgrades_in_place() {
    for (os, arch, target) in [
        ("Linux", "x86_64", "x86_64-unknown-linux-gnu"),
        ("Darwin", "arm64", "aarch64-apple-darwin"),
        ("Darwin", "x86_64", "x86_64-apple-darwin"),
    ] {
        let fixture = Fixture::new();
        fixture.archive("v0.1.0", "0.1.0", target);
        let output = fixture.run(os, arch, &[]);
        assert!(output.status.success(), "{:?}", output);
        assert_eq!(
            fs::read_to_string(fixture.prefix.join("share/licenses/onetui/LICENSE")).unwrap(),
            "license"
        );
        fixture.archive("v0.2.0", "0.2.0", target);
        let output = fixture.run(os, arch, &["--version", "v0.2.0"]);
        assert!(output.status.success(), "{:?}", output);
        let version = Command::new(fixture.prefix.join("bin/onetui"))
            .arg("--version")
            .output()
            .unwrap();
        assert_eq!(version.stdout, b"onetui 0.2.0\n");
    }
}

#[test]
fn corrupt_download_and_wrong_version_preserve_existing_installation() {
    for corrupt in [true, false] {
        let fixture = Fixture::new();
        let binary = fixture.existing();
        let archive = fixture.archive("v0.1.0", "9.9.9", "x86_64-unknown-linux-gnu");
        if corrupt {
            fs::write(archive, "corrupt archive").unwrap();
        }
        let output = fixture.run("Linux", "x86_64", &[]);
        assert!(!output.status.success());
        assert_eq!(fs::read_to_string(binary).unwrap(), "previous installation");
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains(if corrupt {
                "SHA-256 mismatch"
            } else {
                "version does not match"
            }),
            "{error}"
        );
    }
}

#[test]
fn missing_checksum_never_replaces_the_binary() {
    let fixture = Fixture::new();
    let binary = fixture.existing();
    let archive = fixture.archive("v0.1.0", "0.1.0", "x86_64-unknown-linux-gnu");
    fs::remove_file(archive.with_extension("gz.sha256")).unwrap();
    assert!(!fixture.run("Linux", "x86_64", &[]).status.success());
    assert_eq!(fs::read_to_string(binary).unwrap(), "previous installation");
}

#[test]
fn rejects_package_manager_symlinks_and_invalid_requests() {
    let fixture = Fixture::new();
    let binary = fixture.existing();
    let managed = fixture.root.path().join("managed-binary");
    fs::rename(&binary, &managed).unwrap();
    symlink(&managed, &binary).unwrap();
    fixture.archive("v0.1.0", "0.1.0", "x86_64-unknown-linux-gnu");
    let output = fixture.run("Linux", "x86_64", &[]);
    assert!(String::from_utf8_lossy(&output.stderr).contains("symlink"));
    assert!(binary.is_symlink());
    assert_eq!(
        fs::read_to_string(managed).unwrap(),
        "previous installation"
    );
    for args in [
        vec!["--version", "../../bad"],
        vec!["--version"],
        vec!["--prefix", "relative"],
        vec!["--unknown"],
    ] {
        assert!(!fixture.run("Linux", "x86_64", &args).status.success());
    }
    assert!(!fixture.run("Linux", "aarch64", &[]).status.success());
}
