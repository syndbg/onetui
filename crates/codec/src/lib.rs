#![doc = include_str!("../README.md")]
mod avro;
mod bounds;
mod protobuf;

use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};

pub const MAX_SCHEMA_BYTES: usize = 256 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_DEPTH: usize = 32;
pub const MAX_NODES: usize = 4096;
pub const MAX_OUTPUT_BYTES: usize = 1024 * 1024;

pub struct Decoder {
    schema_id: String,
    inner: Codec,
}

enum Codec {
    Protobuf(prost_reflect::MessageDescriptor),
    Avro(apache_avro::Schema),
}

/// Native typed values retain bytes, unknown fields, enum numbers and logical types.
/// JSON is only a presentation; it does not replace these values or the wire bytes.
pub enum TypedValue {
    Protobuf(prost_reflect::DynamicMessage),
    Avro(apache_avro::types::Value),
}

pub struct Decoded {
    raw: Vec<u8>,
    schema_id: String,
    value: TypedValue,
}

impl Decoder {
    /// Load a binary FileDescriptorSet including imports and select an exact full name.
    pub fn protobuf(descriptor_set: &[u8], message_name: &str) -> Result<Self> {
        ensure!(
            descriptor_set.len() <= MAX_SCHEMA_BYTES,
            "Descriptor set exceeds 256 KiB"
        );
        ensure!(
            !message_name.is_empty() && message_name.len() <= 1024,
            "Invalid Protobuf message name"
        );
        let descriptor = protobuf::schema(descriptor_set, message_name)?;
        Ok(Self {
            schema_id: format!(
                "protobuf:sha256:{}:{message_name}",
                fingerprint(descriptor_set)
            ),
            inner: Codec::Protobuf(descriptor),
        })
    }

    /// Parse a self-contained writer schema. Reader-schema resolution is not implicit.
    pub fn avro(writer_schema: &str) -> Result<Self> {
        ensure!(
            writer_schema.len() <= MAX_SCHEMA_BYTES,
            "Avro schema exceeds 256 KiB"
        );
        let schema = avro::schema(writer_schema)?;
        Ok(Self {
            schema_id: format!("avro:sha256:{}", fingerprint(writer_schema.as_bytes())),
            inner: Codec::Avro(schema),
        })
    }

    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }

    /// Interpret one raw payload. On error the caller still owns the unchanged input.
    /// This method never guesses framing or substitutes another codec.
    pub fn decode(&self, raw: &[u8]) -> Result<Decoded> {
        ensure!(
            raw.len() <= MAX_PAYLOAD_BYTES,
            "Message exceeds 64 KiB decode limit; inspect raw bytes"
        );
        let value = match &self.inner {
            Codec::Protobuf(descriptor) => TypedValue::Protobuf(protobuf::decode(descriptor, raw)?),
            Codec::Avro(schema) => TypedValue::Avro(avro::decode(schema, raw)?),
        };
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
    pub fn value(&self) -> &TypedValue {
        &self.value
    }

    /// Bounded, unformatted JSON presentation. Call outside rendering.
    /// Errors here do not discard the decoded value or original bytes.
    pub fn json(&self) -> Result<String> {
        match &self.value {
            TypedValue::Protobuf(value) => {
                protobuf::check_json(value)?;
                bounds::json(value)
            }
            TypedValue::Avro(value) => {
                let json = serde_json::Value::try_from(value.clone())?;
                bounds::json(&json)
            }
        }
    }
}
