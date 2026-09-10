#![doc = include_str!("../README.md")]
mod avro;
mod bounds;

use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};

pub const MAX_SCHEMA_BYTES: usize = 256 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_DEPTH: usize = 32;
pub const MAX_NODES: usize = 4096;
pub const MAX_OUTPUT_BYTES: usize = 1024 * 1024;

pub struct Decoder {
    schema_id: String,
    schema: apache_avro::Schema,
}

/// Retains the native typed value and original bytes; JSON is only a presentation.
pub struct Decoded {
    raw: Vec<u8>,
    schema_id: String,
    value: apache_avro::types::Value,
}

impl Decoder {
    /// Parse a self-contained writer schema. Reader-schema resolution is not implicit.
    pub fn new(writer_schema: &str) -> Result<Self> {
        ensure!(
            writer_schema.len() <= MAX_SCHEMA_BYTES,
            "Avro schema exceeds 256 KiB"
        );
        let schema = avro::schema(writer_schema)?;
        Ok(Self {
            schema_id: format!("avro:sha256:{}", fingerprint(writer_schema.as_bytes())),
            schema,
        })
    }

    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }

    /// Decode one raw payload without guessing framing or substituting another format.
    /// On error the caller still owns the unchanged input.
    pub fn decode(&self, raw: &[u8]) -> Result<Decoded> {
        ensure!(
            raw.len() <= MAX_PAYLOAD_BYTES,
            "Message exceeds 64 KiB decode limit; inspect raw bytes"
        );
        let value = avro::decode(&self.schema, raw)?;
        Ok(Decoded {
            raw: raw.to_vec(),
            schema_id: self.schema_id.clone(),
            value,
        })
    }
}

fn fingerprint(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

impl Decoded {
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }
    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }
    pub fn value(&self) -> &apache_avro::types::Value {
        &self.value
    }

    /// Bounded, unformatted JSON presentation. Call outside rendering.
    /// Errors here do not discard the decoded value or original bytes.
    pub fn json(&self) -> Result<String> {
        let json = serde_json::Value::try_from(self.value.clone())?;
        bounds::json(&json)
    }
}
