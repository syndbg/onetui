use anyhow::{Result, bail, ensure};
use onetui_protobuf::{Decoder, MAX_PAYLOAD_BYTES, MAX_SCHEMA_BYTES};
use std::{fs::File, io::Read};

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [path, message, hex] = args.as_slice() else {
        bail!("Usage: decode_protobuf DESCRIPTOR_SET FULL_MESSAGE_NAME HEX");
    };
    ensure!(
        hex.len() <= MAX_PAYLOAD_BYTES * 2 && hex.len().is_multiple_of(2),
        "Hex input must contain at most 65536 complete bytes"
    );
    let raw: Vec<u8> = hex
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
        .collect::<Result<_>>()?;
    let mut schema = Vec::new();
    File::open(path)?
        .take(MAX_SCHEMA_BYTES as u64 + 1)
        .read_to_end(&mut schema)?;
    ensure!(
        schema.len() <= MAX_SCHEMA_BYTES,
        "Schema file exceeds 256 KiB"
    );
    let decoder = Decoder::new(&schema, message)?;
    let decoded = decoder.decode(&raw)?;
    eprintln!(
        "Schema: {:?}; raw bytes: {}",
        decoded.schema_id(),
        decoded.raw().len()
    );
    println!("{}", decoded.json()?);
    Ok(())
}
