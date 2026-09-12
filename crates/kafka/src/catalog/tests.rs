use super::*;
use std::{fs, os::unix::fs::symlink};

fn config(directory: &std::path::Path) -> Config {
    Config {
        directory: directory.to_str().unwrap().into(),
        schema: "event".into(),
        references: vec![],
    }
}

#[test]
fn inventory_is_bounded_and_selection_is_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let mut catalog = config(dir.path());
    fs::write(dir.path().join("event.avsc"), b"root").unwrap();
    fs::write(dir.path().join("child.avsc"), b"child").unwrap();
    fs::write(dir.path().join("unused.avsc"), b"not a schema").unwrap();
    catalog.references.push("child".into());
    let bundle = catalog.load(Format::Avro, &|| Ok(())).unwrap();
    assert_eq!(bundle.schema, b"root");
    assert_eq!(bundle.references, [b"child".to_vec()]);
    assert!(catalog.load(Format::Protobuf, &|| Ok(())).is_err());
    catalog.references = vec!["missing".into()];
    assert!(catalog.load(Format::Avro, &|| Ok(())).is_err());
    for invalid in ["../event", ".", "..", "", "a/b", "a\\b", "🌊"] {
        catalog.schema = invalid.into();
        assert!(catalog.validate(Format::Avro).is_err());
    }
    catalog.schema = "event".into();
    catalog.references = vec!["child".into(), "child".into()];
    assert!(catalog.validate(Format::Avro).is_err());
    catalog.references.clear();
    fs::write(dir.path().join("event.pb"), b"duplicate").unwrap();
    assert!(
        catalog
            .load(Format::Avro, &|| Ok(()))
            .err()
            .unwrap()
            .to_string()
            .contains("Duplicate")
    );
    fs::remove_file(dir.path().join("event.pb")).unwrap();
    for n in 0..62 {
        fs::write(dir.path().join(format!("s{n}.avsc")), b"").unwrap();
    }
    assert!(
        catalog
            .load(Format::Avro, &|| Ok(()))
            .err()
            .unwrap()
            .to_string()
            .contains("64 schemas")
    );
    let many = tempfile::tempdir().unwrap();
    for n in 0..129 {
        fs::write(many.path().join(format!("ignored{n}")), b"").unwrap();
    }
    assert!(
        config(many.path())
            .load(Format::Avro, &|| Ok(()))
            .err()
            .unwrap()
            .to_string()
            .contains("128 entries")
    );
}

#[test]
fn catalog_rejects_symlinks_special_files_and_oversized_bundles() {
    use nix::{sys::stat::Mode, unistd::mkfifo};
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let path = dir.path().join("event.avsc");
    fs::write(outside.path().join("secret"), b"outside").unwrap();
    symlink(outside.path().join("secret"), &path).unwrap();
    let mut catalog = config(dir.path());
    assert!(catalog.load(Format::Avro, &|| Ok(())).is_err());
    fs::remove_file(&path).unwrap();
    mkfifo(&path, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
    assert!(catalog.load(Format::Avro, &|| Ok(())).is_err());
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();
    assert!(catalog.load(Format::Avro, &|| Ok(())).is_err());
    fs::remove_dir(&path).unwrap();
    fs::write(&path, vec![b' '; BYTES]).unwrap();
    fs::write(dir.path().join("child.avsc"), b"x").unwrap();
    catalog.references.push("child".into());
    assert!(
        catalog
            .load(Format::Avro, &|| Ok(()))
            .err()
            .unwrap()
            .to_string()
            .contains("256 KiB")
    );
    let root_link = outside.path().join("root");
    symlink(dir.path(), &root_link).unwrap();
    assert!(config(&root_link).load(Format::Avro, &|| Ok(())).is_err());
    let linked = Config {
        directory: format!("{}/", root_link.display()),
        ..config(dir.path())
    };
    assert!(linked.load(Format::Avro, &|| Ok(())).is_err());
}

#[test]
fn directory_fd_survives_path_replacement_and_checks_cancellation() {
    use nix::{dir::Dir, fcntl::OFlag, sys::stat::Mode};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("event.avsc"), b"original").unwrap();
    let pinned = Dir::open(&root, OFlag::O_RDONLY | OFlag::O_DIRECTORY, Mode::empty()).unwrap();
    fs::rename(&root, dir.path().join("moved")).unwrap();
    fs::create_dir(&root).unwrap();
    fs::write(root.join("event.avsc"), b"replacement").unwrap();
    assert_eq!(read_at(&pinned, "event.avsc", BYTES).unwrap(), b"original");
    let calls = std::cell::Cell::new(0);
    let result = config(&root).load(Format::Avro, &|| {
        calls.set(calls.get() + 1);
        ensure!(calls.get() < 3, "cancelled");
        Ok(())
    });
    assert_eq!(result.err().unwrap().to_string(), "cancelled");
}
