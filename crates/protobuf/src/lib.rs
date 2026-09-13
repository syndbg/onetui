#![doc = include_str!("../README.md")]
mod bounds;
mod inspect;
mod protobuf;
mod source;

pub use source::SourceSchema;

use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};

pub const MAX_SCHEMA_BYTES: usize = 256 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_DEPTH: usize = 32;
pub const MAX_NODES: usize = 4096;
pub const MAX_OUTPUT_BYTES: usize = 1024 * 1024;

pub struct Decoder {
    schema_id: String,
    descriptor: prost_reflect::MessageDescriptor,
}

/// Retains the native typed value and original bytes; JSON is only a presentation.
pub struct Decoded {
    raw: Vec<u8>,
    schema_id: String,
    value: prost_reflect::DynamicMessage,
}

impl Decoder {
    /// Load a binary FileDescriptorSet including imports and select an exact full name.
    pub fn new(descriptor_set: &[u8], message_name: &str) -> Result<Self> {
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
            descriptor,
        })
    }

    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }

    pub fn message_name(&self) -> &str {
        self.descriptor.full_name()
    }

    /// Decode one raw payload without guessing framing or substituting another format.
    /// On error the caller still owns the unchanged input.
    pub fn decode(&self, raw: &[u8]) -> Result<Decoded> {
        ensure!(
            raw.len() <= MAX_PAYLOAD_BYTES,
            "Message exceeds 64 KiB decode limit; inspect raw bytes"
        );
        let value = protobuf::decode(&self.descriptor, raw)?;
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
    pub fn value(&self) -> &prost_reflect::DynamicMessage {
        &self.value
    }

    /// Bounded, unformatted JSON presentation. Call outside rendering.
    /// Errors here do not discard the decoded value or original bytes.
    pub fn json(&self) -> Result<String> {
        protobuf::check_json(&self.value)?;
        bounds::json(&self.value)
    }

    /// Typed inspection, including unknown field wire types and re-encoded bytes.
    pub fn native(&self) -> Result<String> {
        bounds::json(&inspect::Message(&self.value))
    }
}
