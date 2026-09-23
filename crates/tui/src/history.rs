use std::collections::VecDeque;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail, ensure};

use onetui_core::config::Config;

pub(crate) const LIMIT: usize = 100;
const MAX_BYTES: u64 = 12 * 1024 * 1024;

pub(crate) type Entries = VecDeque<(String, String)>;

pub(crate) fn path(config: &Config) -> Option<PathBuf> {
    if !config.persist_query_history {
        return None;
    }
    config
        .path()
        .filter(|path| path.is_file())
        .map(|path| path.with_extension("history.json"))
}

pub(crate) fn load(path: &Path) -> Result<Entries> {
    let file = match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                !metadata.file_type().is_symlink(),
                "History file is a symlink"
            );
            std::fs::File::open(path)?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Entries::new()),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= MAX_BYTES, "History file is too large");
    let mut entries: Entries = serde_json::from_slice(&bytes)?;
    while entries.len() > LIMIT {
        entries.pop_front();
    }
    Ok(entries)
}

pub(crate) fn save(path: &Path, entries: &Entries) -> Result<()> {
    let bytes = serde_json::to_vec(entries)?;
    if bytes.len() as u64 > MAX_BYTES {
        bail!("History file would be too large");
    }
    let parent = path.parent().expect("history file has a parent");
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    use std::io::Write;
    temporary.write_all(&bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_file_is_private_and_corrupt_data_is_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.history.json");
        let entries = Entries::from([("pg".into(), "SELECT 1".into())]);
        save(&path, &entries).unwrap();
        assert_eq!(load(&path).unwrap(), entries);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        std::fs::write(&path, b"broken").unwrap();
        assert!(load(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"broken");
    }
}
