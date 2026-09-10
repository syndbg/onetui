use anyhow::{Result, ensure};
use std::io::Write;

#[derive(Default)]
pub(crate) struct Budget {
    nodes: usize,
}

impl Budget {
    pub fn node(&mut self, depth: usize) -> Result<()> {
        ensure!(depth <= crate::MAX_DEPTH, "Decode depth exceeds 32");
        self.nodes += 1;
        ensure!(
            self.nodes <= crate::MAX_NODES,
            "Decode node count exceeds 4096"
        );
        Ok(())
    }
}

pub(crate) fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    ensure!(len <= input.len(), "Truncated message");
    let (part, rest) = input.split_at(len);
    *input = rest;
    Ok(part)
}

// Refuse output growth before allocating, including JSON escaping expansion.
struct Output(Vec<u8>);
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > crate::MAX_OUTPUT_BYTES - self.0.len() {
            return Err(std::io::Error::other("Decoded JSON exceeds 1 MiB"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn json(value: &impl serde::Serialize) -> Result<String> {
    let mut output = Output(Vec::new());
    serde_json::to_writer(&mut output, value)?;
    Ok(String::from_utf8(output.0)?)
}
