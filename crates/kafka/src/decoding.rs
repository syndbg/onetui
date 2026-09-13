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

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Framing {
    Raw,
    Confluent,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub topic: String,
    pub field: Field,
    pub format: Format,
    pub framing: Framing,
    pub schema_file: Option<String>,
    pub message_name: Option<String>,
    pub registry: Option<crate::registry::Config>,
    pub catalog: Option<crate::catalog::Config>,
    pub buf: Option<crate::buf::Config>,
}

pub(crate) fn validate_path(path: &str) -> Result<()> {
    ensure!(
        path.len() <= 4096 && !path.chars().any(char::is_control) && Path::new(path).is_absolute(),
        "Decoder path must be absolute, at most 4096 bytes, without controls"
    );
    Ok(())
}

pub(crate) fn read_file(path: &str, limit: usize) -> Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // A FIFO substituted at this path must not hold the native worker indefinitely.
        options.custom_flags(nix::libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    ensure!(
        file.metadata()?.is_file(),
        "Decoder path must be a regular file"
    );
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= limit, "Decoder file exceeds {limit} bytes");
    Ok(bytes)
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
        match binding.framing {
            Framing::Raw => {
                ensure!(
                    [
                        binding.schema_file.is_some(),
                        binding.catalog.is_some(),
                        binding.buf.is_some()
                    ]
                    .into_iter()
                    .filter(|present| *present)
                    .count()
                        == 1,
                    "Raw binding requires exactly one of schema_file, catalog or buf"
                );
                if let Some(path) = &binding.schema_file {
                    validate_path(path)?;
                }
                if let Some(catalog) = &binding.catalog {
                    catalog.validate(binding.format)?;
                }
                if let Some(buf) = &binding.buf {
                    ensure!(binding.format == Format::Protobuf, "Buf requires Protobuf");
                    buf.validate()?;
                }
                ensure!(
                    binding.registry.is_none(),
                    "Raw binding does not accept registry"
                );
            }
            Framing::Confluent => {
                ensure!(
                    binding.schema_file.is_none()
                        && binding.catalog.is_none()
                        && binding.buf.is_none(),
                    "Registry binding does not accept schema_file, catalog or buf"
                );
                binding
                    .registry
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Confluent binding requires registry"))?
                    .validate()?;
            }
        }
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
            Format::Protobuf if binding.framing == Framing::Raw => ensure!(
                binding
                    .message_name
                    .as_ref()
                    .is_some_and(|name| !name.is_empty()
                        && name.len() <= 1024
                        && !name.chars().any(char::is_control)),
                "Protobuf binding requires message_name of 1..1024 bytes without controls"
            ),
            Format::Protobuf => ensure!(
                binding.message_name.is_none(),
                "Confluent Protobuf selects its message through envelope indexes; omit message_name"
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
    fn load(binding: &mut Binding, remaining: &impl Fn() -> Result<()>) -> Result<Self> {
        remaining()?;
        let bundle = if let Some(buf) = &mut binding.buf {
            crate::catalog::Bundle {
                schema: buf.load(remaining)?,
                references: Vec::new(),
            }
        } else if let Some(catalog) = &binding.catalog {
            catalog.load(binding.format, remaining)?
        } else {
            crate::catalog::Bundle {
                schema: read_file(
                    binding
                        .schema_file
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("Raw binding requires schema_file"))?,
                    SCHEMA_BYTES,
                )?,
                references: Vec::new(),
            }
        };
        remaining()?;
        match binding.framing {
            Framing::Raw => match binding.format {
                Format::Avro => Ok(Self::Avro(onetui_avro::Decoder::with_references(
                    std::str::from_utf8(&bundle.schema)?,
                    &bundle
                        .references
                        .iter()
                        .map(|s| std::str::from_utf8(s))
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                )?)),
                Format::Protobuf => Ok(Self::Protobuf(onetui_protobuf::Decoder::new(
                    &bundle.schema,
                    binding.message_name.as_deref().unwrap(),
                )?)),
            },
            Framing::Confluent => {
                anyhow::bail!("Registry binding requires per-message schema resolution")
            }
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
    loaded: Option<Result<Loaded, String>>,
    registry: Option<Result<crate::registry::Registry, String>>,
}

struct Loaded {
    decoder: Decoder,
    identity: String,
}

impl Entry {
    fn buf_identity(&self) -> Option<String> {
        self.binding.buf.as_ref().map(|buf| {
            format!(
                "{}:message={}",
                buf.identity(),
                self.binding.message_name.as_deref().unwrap_or_default()
            )
        })
    }

    fn registry(&mut self) -> Result<&mut crate::registry::Registry> {
        self.registry
            .get_or_insert_with(|| {
                crate::registry::Registry::new(
                    self.binding.registry.clone().unwrap(),
                    self.binding.format,
                )
                .map_err(|e| format!("{e:#}"))
            })
            .as_mut()
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    fn preview(
        &mut self,
        raw: &[u8],
        remaining: &impl Fn() -> Result<()>,
    ) -> (Option<String>, Result<String>) {
        if self.binding.registry.is_some() {
            return match self.registry() {
                Ok(registry) => registry.preview(raw, remaining),
                Err(error) => (None, Err(error)),
            };
        }
        let decoder = self.load(remaining);
        let identity = decoder.as_ref().ok().map(|d| d.identity.clone());
        let result = decoder
            .as_ref()
            .map_err(|error| anyhow::anyhow!("{error}"))
            .and_then(|d| d.decoder.json(raw));
        (identity.or_else(|| self.buf_identity()), result)
    }
    fn load(&mut self, remaining: &impl Fn() -> Result<()>) -> &Result<Loaded, String> {
        self.loaded.get_or_insert_with(|| {
            (|| {
                let decoder = Decoder::load(&mut self.binding, remaining)?;
                remaining()?;
                let identity = if let Some(buf) = &self.binding.buf {
                    format!("{}:{}", buf.identity(), decoder.schema_id())
                } else if let Some(catalog) = &self.binding.catalog {
                    format!("{}:{}", catalog.identity(), decoder.schema_id())
                } else {
                    decoder.schema_id().to_owned()
                };
                Ok(Loaded { decoder, identity })
            })()
            .map_err(|error: anyhow::Error| match &self.binding.buf {
                Some(buf) => buf.diagnostic(error),
                None => format!("{error:#}"),
            })
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
                    registry: None,
                })
                .collect(),
        )
    }

    pub fn check(&mut self, remaining: impl Fn() -> Result<()>) -> Result<()> {
        for entry in &mut self.0 {
            remaining()?;
            if entry.binding.registry.is_some() {
                entry.registry()?.check(&remaining)?;
            } else {
                entry
                    .load(&remaining)
                    .as_ref()
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
            }
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
            let identity = if entry.binding.registry.is_some() {
                None
            } else {
                entry
                    .load(&remaining)
                    .as_ref()
                    .ok()
                    .map(|d| d.identity.clone())
                    .or_else(|| entry.buf_identity())
            };
            remaining()?;
            let identity_bytes = if entry.binding.registry.is_some() {
                1700
            } else {
                identity.as_ref().map_or(0, String::len)
            };
            reserved += page.rows.len() * (identity_bytes + ERROR_BYTES);
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
            selected.push((index, entry, identity));
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
            for (index, entry, identity) in &mut selected {
                remaining()?;
                let mut error = None;
                let mut schema_identity = identity.clone();
                let decoded = if let Some(value) = &row.cells[*index] {
                    let result = if available == 0 {
                        Err(anyhow::anyhow!(
                            "Decoded preview exceeds remaining page budget"
                        ))
                    } else {
                        let (resolved, result) = entry.preview(value.bytes(), &remaining);
                        schema_identity = resolved;
                        result
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
                    .extend([decoded, schema_identity.map(Value::Text), error]);
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
