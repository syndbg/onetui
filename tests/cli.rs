use std::process::Command;

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_onetui"))
}

#[test]
fn help_version_and_nonterminal_error_work_without_configuration() {
    for arg in ["--help", "--version"] {
        let output = binary().arg(arg).output().unwrap();
        assert!(output.status.success());
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(text.contains("onetui"), "{text}");
        if arg == "--version" {
            assert_eq!(text.trim(), concat!("onetui ", env!("CARGO_PKG_VERSION")));
        }
    }
    let output = binary().output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires a terminal"));
    assert!(!binary().arg("--check").output().unwrap().status.success());
}
