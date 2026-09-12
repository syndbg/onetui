use anyhow::{Result, ensure};
use serde::Deserialize;

use crate::decoding::{Format, validate_path};

const ENTRIES: usize = 128;
const SCHEMAS: usize = 64;
const BYTES: usize = 256 * 1024;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub directory: String,
    pub schema: String,
    #[serde(default)]
    pub references: Vec<String>,
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && !matches!(id, "." | "..")
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

impl Config {
    pub fn validate(&self, format: Format) -> Result<()> {
        ensure!(cfg!(unix), "Directory catalogs require Unix");
        validate_path(&self.directory)?;
        ensure!(valid_id(&self.schema), "Invalid catalog schema ID");
        ensure!(self.references.len() <= 32, "Catalog references exceed 32");
        ensure!(
            format == Format::Avro || self.references.is_empty(),
            "Protobuf catalog entries require descriptors with imports; omit references"
        );
        for (index, name) in self.references.iter().enumerate() {
            ensure!(
                valid_id(name) && name != &self.schema && !self.references[..index].contains(name),
                "Invalid or duplicate catalog reference ID"
            );
        }
        Ok(())
    }

    pub fn identity(&self) -> String {
        format!("directory:{}#schema={}", self.directory, self.schema)
    }

    #[cfg(unix)]
    pub fn load(&self, format: Format, remaining: &impl Fn() -> Result<()>) -> Result<Bundle> {
        use nix::{dir::Dir, fcntl::OFlag, sys::stat::Mode};
        use std::collections::BTreeMap;

        self.validate(format)?;
        remaining()?;
        // A trailing slash would make open follow a symlink before O_NOFOLLOW applies.
        let path = self.directory.trim_end_matches('/');
        let mut directory = Dir::open(
            if path.is_empty() { "/" } else { path },
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )?;
        let mut inventory = BTreeMap::new();
        let mut visited = 0;
        for entry in directory.iter() {
            remaining()?;
            let entry = entry?;
            let bytes = entry.file_name().to_bytes();
            if matches!(bytes, b"." | b"..") {
                continue;
            }
            visited += 1;
            ensure!(visited <= ENTRIES, "Catalog directory exceeds 128 entries");
            let kind = if bytes.ends_with(b".avsc") {
                Format::Avro
            } else if bytes.ends_with(b".pb") {
                Format::Protobuf
            } else {
                continue;
            };
            let name = entry.file_name().to_str()?;
            let (id, _) = name.rsplit_once('.').unwrap();
            ensure!(valid_id(id), "Invalid catalog schema filename");
            ensure!(inventory.len() < SCHEMAS, "Catalog exceeds 64 schemas");
            ensure!(
                inventory
                    .insert(id.to_owned(), (name.to_owned(), kind))
                    .is_none(),
                "Duplicate catalog schema ID: {id}"
            );
        }
        let mut budget = BYTES;
        let mut read = |id: &str| -> Result<Vec<u8>> {
            remaining()?;
            let (name, kind) = inventory
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("Catalog schema not found: {id}"))?;
            ensure!(
                *kind == format,
                "Catalog schema format does not match binding: {id}"
            );
            let bytes = read_at(&directory, name, budget)?;
            budget -= bytes.len();
            remaining()?;
            Ok(bytes)
        };
        let schema = read(&self.schema)?;
        let references = self
            .references
            .iter()
            .map(|id| read(id))
            .collect::<Result<Vec<_>>>()?;
        Ok(Bundle { schema, references })
    }

    #[cfg(not(unix))]
    pub fn load(&self, _: Format, _: &impl Fn() -> Result<()>) -> Result<Bundle> {
        anyhow::bail!("Directory catalogs require Unix")
    }
}

pub(crate) struct Bundle {
    pub schema: Vec<u8>,
    pub references: Vec<Vec<u8>>,
}

#[cfg(unix)]
fn read_at(directory: &nix::dir::Dir, name: &str, budget: usize) -> Result<Vec<u8>> {
    use nix::{
        fcntl::{OFlag, openat},
        sys::stat::Mode,
    };
    use std::{fs::File, io::Read};

    // A pinned directory FD and a single filename avoid symlink/rename races.
    // Nonblocking open lets fstat reject FIFOs without waiting for a writer.
    let fd = openat(
        directory,
        name,
        OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK,
        Mode::empty(),
    )?;
    let file = File::from(fd);
    ensure!(
        file.metadata()?.is_file(),
        "Catalog schema must be a regular file"
    );
    let mut bytes = Vec::new();
    file.take(budget as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= budget,
        "Catalog schema bundle exceeds 256 KiB"
    );
    Ok(bytes)
}

pub(crate) fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "required": false, "default": "none", "type": "table; raw framing only; mutually exclusive with schema_file and registry",
        "fields": {
            "directory": {"required": true, "type": "absolute directory path up to 4096 UTF-8 bytes without controls", "purpose": "Flat .avsc/.pb inventory on Unix; no recursive scan or path expansion"},
            "schema": {"required": true, "type": "filename stem, 1..128 ASCII letters/digits/dot/underscore/hyphen; not dot or dot-dot", "purpose": "Exact identity; no trial decoding. Duplicate stems across extensions fail."},
            "references": {"required": false, "default": [], "type": "up to 32 distinct Avro catalog IDs; excludes the root ID", "purpose": "All named writer dependencies, including transitive ones. Protobuf requires imports inside its descriptor set instead."}
        },
        "limits": {"directory_entries": ENTRIES, "schemas": SCHEMAS, "bundle_bytes": BYTES},
        "lifecycle": "Snapshot each binding on first use or --check. Reopen the connection to reload, including cached failures. Row refresh does not reload schemas or reinterpret retained values. Reads stay on the native worker, with cancellation checks between entries/files; OS filesystem calls are not preemptible.",
        "safety": "Schema files open relative to a pinned directory FD, without following symlinks; only regular files are read. Configured root must not itself be a symlink. Unselected schemas are inventoried but not parsed. No watcher or source compilation."
    })
}

#[cfg(all(test, unix))]
mod tests;
