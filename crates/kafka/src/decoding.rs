use anyhow::{Result, ensure};
use onetui_core::{Column, PAGE_BYTES, Page, Value};
use serde::Deserialize;
use std::{fs::OpenOptions, io::Read, path::Path};

const SCHEMA_BYTES: usize = 256 * 1024;
const PREVIEW_BYTES: usize = 64 * 1024;
const ERROR_BYTES: usize = 512;

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Field {
    Key,
    Value,
}

impl Field {
    fn name(self) -> &'static str {
        match self {
            Self::Key => "key",
            Self::Value => "value",
        }
    }
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Format {
    Avro,
    Protobuf,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Framing {
    Raw,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub topic: String,
    pub field: Field,
    pub format: Format,
    pub framing: Framing,
    pub schema_file: String,
    pub message_name: Option<String>,
}

pub(crate) fn validate(bindings: &[Binding]) -> Result<()> {
    ensure!(
        bindings.len() <= 32,
        "Kafka supports at most 32 decoder bindings"
    );
    for (index, binding) in bindings.iter().enumerate() {
        ensure!(
            crate::browse::valid_topic(&binding.topic),
            "Invalid decoder topic"
        );
        ensure!(
            binding.schema_file.len() <= 4096
                && !binding.schema_file.chars().any(char::is_control)
                && Path::new(&binding.schema_file).is_absolute(),
            "Decoder schema_file must be an absolute path of at most 4096 bytes without controls"
        );
        ensure!(
            !bindings[..index]
                .iter()
                .any(|previous| previous.topic == binding.topic && previous.field == binding.field),
            "Duplicate Kafka decoder binding for topic and field"
        );
        match binding.format {
            Format::Avro => ensure!(
                binding.message_name.is_none(),
                "Avro binding does not accept message_name"
            ),
            Format::Protobuf => ensure!(
                binding
                    .message_name
                    .as_ref()
                    .is_some_and(|name| !name.is_empty()
                        && name.len() <= 1024
                        && !name.chars().any(char::is_control)),
                "Protobuf binding requires message_name of 1..1024 bytes without controls"
            ),
        }
    }
    Ok(())
}

enum Decoder {
    Avro(onetui_avro::Decoder),
    Protobuf(onetui_protobuf::Decoder),
}

impl Decoder {
    fn load(binding: &Binding) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // A FIFO substituted at this path must not hold the native worker indefinitely.
            options.custom_flags(nix::libc::O_NONBLOCK);
        }
        let file = options.open(&binding.schema_file)?;
        ensure!(
            file.metadata()?.is_file(),
            "Decoder schema_file must be a regular file"
        );
        let mut bytes = Vec::new();
        file.take(SCHEMA_BYTES as u64 + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() <= SCHEMA_BYTES,
            "Decoder schema exceeds 256 KiB"
        );
        match binding.framing {
            Framing::Raw => match binding.format {
                Format::Avro => Ok(Self::Avro(onetui_avro::Decoder::new(std::str::from_utf8(
                    &bytes,
                )?)?)),
                Format::Protobuf => Ok(Self::Protobuf(onetui_protobuf::Decoder::new(
                    &bytes,
                    binding.message_name.as_deref().unwrap(),
                )?)),
            },
        }
    }

    fn schema_id(&self) -> &str {
        match self {
            Self::Avro(decoder) => decoder.schema_id(),
            Self::Protobuf(decoder) => decoder.schema_id(),
        }
    }

    fn json(&self, raw: &[u8]) -> Result<String> {
        match self {
            Self::Avro(decoder) => decoder.decode(raw)?.json(),
            Self::Protobuf(decoder) => decoder.decode(raw)?.json(),
        }
    }
}

struct Entry {
    binding: Binding,
    loaded: Option<Result<Decoder, String>>,
}

impl Entry {
    fn load(&mut self) -> &Result<Decoder, String> {
        self.loaded.get_or_insert_with(|| {
            Decoder::load(&self.binding).map_err(|error| format!("{error:#}"))
        })
    }
}

pub(crate) struct Bindings(Vec<Entry>);

impl Bindings {
    pub fn new(bindings: Vec<Binding>) -> Self {
        Self(
            bindings
                .into_iter()
                .map(|binding| Entry {
                    binding,
                    loaded: None,
                })
                .collect(),
        )
    }

    pub fn check(&mut self, remaining: impl Fn() -> Result<()>) -> Result<()> {
        for entry in &mut self.0 {
            remaining()?;
            entry
                .load()
                .as_ref()
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            remaining()?;
        }
        Ok(())
    }

    pub fn raw_page_limit(&self, topic: &str) -> usize {
        // Reserve column/notice bytes even when a large raw record leaves no preview budget.
        if self.0.iter().any(|entry| entry.binding.topic == topic) {
            PAGE_BYTES - 2048
        } else {
            PAGE_BYTES
        }
    }

    pub fn project(
        &mut self,
        topic: &str,
        page: &mut Page,
        remaining: impl Fn() -> Result<()>,
    ) -> Result<()> {
        let mut selected = Vec::new();
        let mut columns = Vec::new();
        let mut reserved = 0;
        for entry in self
            .0
            .iter_mut()
            .filter(|entry| entry.binding.topic == topic)
        {
            let name = entry.binding.field.name();
            let Some(index) = page.columns.iter().position(|column| column.name == name) else {
                continue;
            };
            remaining()?;
            let decoder = entry.load();
            remaining()?;
            let identity = decoder.as_ref().ok().map(|d| d.schema_id().to_owned());
            reserved += page.rows.len() * (identity.as_ref().map_or(0, String::len) + ERROR_BYTES);
            for (suffix, datatype) in [
                ("decoded", "JSON projection (not wire bytes)"),
                ("schema", "schema identity"),
                ("decode_error", "text"),
            ] {
                columns.push(Column {
                    name: format!("{name}_{suffix}"),
                    datatype: datatype.into(),
                });
            }
            selected.push((index, decoder, identity));
        }
        if selected.is_empty() {
            return Ok(());
        }
        reserved += columns
            .iter()
            .map(|c| c.name.len() + c.datatype.len())
            .sum::<usize>();
        let available = PAGE_BYTES.saturating_sub(page.bytes());
        if reserved > available {
            // Do not drop records or advance their cursor just to make room for a projection.
            let notice = "Decoder previews omitted: page byte limit; raw fields retained";
            let added = columns
                .iter()
                .map(|c| c.name.len() + c.datatype.len())
                .sum::<usize>();
            ensure!(added <= available, "Missing decoder column budget");
            page.notice = notice
                .chars()
                .take(available - added + page.notice.len())
                .collect();
            for row in &mut page.rows {
                row.cells.extend(std::iter::repeat_n(None, columns.len()));
            }
            page.columns.extend(columns);
            return Ok(());
        }
        let mut available = available - reserved;
        page.columns.extend(columns);
        for row in &mut page.rows {
            for (index, decoder, identity) in &selected {
                remaining()?;
                let mut error = None;
                let decoded = if let Some(value) = &row.cells[*index] {
                    let result = if available == 0 {
                        Err(anyhow::anyhow!(
                            "Decoded preview exceeds remaining page budget"
                        ))
                    } else {
                        decoder
                            .as_ref()
                            .map_err(|error| anyhow::anyhow!("{error}"))
                            .and_then(|decoder| decoder.json(value.bytes()))
                    };
                    match result {
                        Ok(json) if json.len() <= PREVIEW_BYTES.min(available) => {
                            available -= json.len();
                            Some(Value::Json(json))
                        }
                        Ok(_) => {
                            error = Some(
                                "Decoded preview exceeds 64 KiB or remaining page budget".into(),
                            );
                            None
                        }
                        Err(cause) => {
                            let mut message = format!("{cause:#}");
                            if message.len() > ERROR_BYTES {
                                let end = message.floor_char_boundary(ERROR_BYTES - 3);
                                message.truncate(end);
                                message.push_str("...");
                            }
                            error = Some(Value::Text(message));
                            None
                        }
                    }
                } else {
                    None
                };
                row.cells
                    .extend([decoded, identity.clone().map(Value::Text), error]);
            }
        }
        remaining()?;
        ensure!(
            page.bytes() <= PAGE_BYTES,
            "Decoded Kafka page exceeds 1 MiB"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests_avro;
#[cfg(test)]
mod tests_protobuf;
