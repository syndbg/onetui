#![doc = include_str!("../README.md")]
use std::fmt::Write as _;
mod avro;
mod bounds;
mod inspect;
mod reader;
pub use reader::ReaderSchema;

use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};

pub const MAX_SCHEMA_BYTES: usize = 256 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_DEPTH: usize = 32;
pub const MAX_NODES: usize = 4096;
pub const MAX_OUTPUT_BYTES: usize = 1024 * 1024;
pub const MAX_ERROR_BYTES: usize = 512;

pub struct Decoder {
    schema_id: String,
    schema: apache_avro::Schema,
    references: Vec<apache_avro::Schema>,
    reader: Option<ReaderSchema>,
}

/// Retains the native typed value and original bytes; JSON is only a presentation.
pub struct Decoded {
    raw: Vec<u8>,
    schema_id: String,
    reader_schema_id: Option<String>,
    value: apache_avro::types::Value,
    resolved: Option<Result<apache_avro::types::Value, String>>,
}

impl Decoder {
    /// Parse a self-contained writer schema. Reader-schema resolution is not implicit.
    ///
    /// # Errors
    ///
    /// Returns an error when the text is not a valid Avro schema.
    pub fn new(writer_schema: &str) -> Result<Self> {
        ensure!(
            writer_schema.len() <= MAX_SCHEMA_BYTES,
            "Avro schema exceeds 256 KiB"
        );
        let schema = avro::schema(writer_schema)?;
        Ok(Self {
            schema_id: format!("avro:sha256:{}", fingerprint(writer_schema.as_bytes())),
            schema,
            references: Vec::new(),
            reader: None,
        })
    }

    /// Resolve named writer dependencies without substituting a reader schema.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer schema or a reference is invalid, or a named dependency is missing.
    ///
    /// # Panics
    ///
    /// Panics if the parser returns no schema for the writer text, which cannot happen after a successful parse.
    pub fn with_references(writer_schema: &str, references: &[&str]) -> Result<Self> {
        if references.is_empty() {
            return Self::new(writer_schema);
        }
        ensure!(references.len() <= 32, "Avro schema references exceed 32");
        ensure!(
            writer_schema.len() + references.iter().map(|s| s.len()).sum::<usize>()
                <= MAX_SCHEMA_BYTES,
            "Avro schema bundle exceeds 256 KiB"
        );
        let mut texts = references.to_vec();
        texts.push(writer_schema);
        let mut budget = bounds::Budget::default();
        for text in &texts {
            bounds::schema_json(&serde_json::from_str(text)?, &mut budget, 0)?;
        }
        let mut schemas = apache_avro::Schema::parse_list(&texts)?;
        apache_avro::schema::ResolvedSchema::new_with_schemata(schemas.iter().collect())?;
        let schema = schemas.pop().unwrap();
        let mut identity = Sha256::new();
        for text in texts {
            identity.update((text.len() as u64).to_be_bytes());
            identity.update(text.as_bytes());
        }
        Ok(Self {
            schema_id: format!(
                "avro:sha256:{}",
                identity
                    .finalize()
                    .iter()
                    .fold(String::new(), |mut hex, b| {
                        let _ = write!(hex, "{b:02x}");
                        hex
                    })
            ),
            schema,
            references: schemas,
            reader: None,
        })
    }

    #[must_use]
    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }

    #[must_use]
    pub fn with_reader(mut self, reader: ReaderSchema) -> Self {
        self.reader = Some(reader);
        self
    }

    pub fn reader_schema_id(&self) -> Option<&str> {
        self.reader.as_ref().map(ReaderSchema::schema_id)
    }

    /// Decode one raw payload without guessing framing or substituting another format.
    /// On error the caller still owns the unchanged input.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload does not match the writer schema or exceeds a decoding bound.
    pub fn decode(&self, raw: &[u8]) -> Result<Decoded> {
        ensure!(
            raw.len() <= MAX_PAYLOAD_BYTES,
            "Message exceeds 64 KiB decode limit; inspect raw bytes"
        );
        let value = avro::decode(&self.schema, &self.references, raw)?;
        let resolved = self.reader.as_ref().map(|reader| {
            reader.resolve(&value).map_err(|error| {
                // Large schema diagnostics must not crowd writer inspection out of the page.
                let mut text = format!("{error:#}");
                if text.len() > MAX_ERROR_BYTES {
                    text.truncate(text.floor_char_boundary(MAX_ERROR_BYTES - 3));
                    text.push_str("...");
                }
                text
            })
        });
        Ok(Decoded {
            raw: raw.to_vec(),
            schema_id: self.schema_id.clone(),
            reader_schema_id: self.reader_schema_id().map(str::to_owned),
            value,
            resolved,
        })
    }
}

fn fingerprint(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

impl Decoded {
    #[must_use]
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }
    #[must_use]
    pub fn schema_id(&self) -> &str {
        &self.schema_id
    }
    #[must_use]
    pub fn reader_schema_id(&self) -> Option<&str> {
        self.reader_schema_id.as_deref()
    }
    #[must_use]
    pub const fn value(&self) -> &apache_avro::types::Value {
        &self.value
    }

    /// Bounded, unformatted JSON presentation. Call outside rendering.
    /// Errors here do not discard the decoded value or original bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the JSON presentation exceeds its bound.
    pub fn json(&self) -> Result<String> {
        let value = match &self.resolved {
            Some(Ok(value)) => value,
            Some(Err(error)) => anyhow::bail!("{error}"),
            None => &self.value,
        };
        let json = serde_json::Value::try_from(value.clone())?;
        bounds::json(&json)
    }

    /// Typed inspection, including union branches and logical types. Not wire bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the inspection output exceeds its bound.
    pub fn native(&self) -> Result<String> {
        if let Some(resolved) = &self.resolved {
            #[derive(serde::Serialize)]
            struct Inspection<'a> {
                writer: inspect::Native<'a>,
                reader: Option<inspect::Native<'a>>,
                reader_error: Option<&'a str>,
            }
            return bounds::json(&Inspection {
                writer: inspect::Native(&self.value),
                reader: resolved.as_ref().ok().map(inspect::Native),
                reader_error: resolved.as_ref().err().map(String::as_str),
            });
        }
        bounds::json(&inspect::Native(&self.value))
    }
}
