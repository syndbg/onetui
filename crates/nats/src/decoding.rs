use anyhow::{Result, anyhow, ensure};
use onetui_core::{Column, PAGE_BYTES, Page, Value};
use serde::Deserialize;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

const SCHEMA_BYTES: usize = 256 * 1024;
const PREVIEW_BYTES: usize = 64 * 1024;
// Cancelled filesystem/parser work retains its permit until it exits.
static WORKERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

pub(crate) use onetui_schema_source::Format;
use onetui_schema_source::{Preview, registry::Registry};

#[derive(Clone, Copy, Default, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Framing {
    #[default]
    Raw,
    Confluent,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub subject: String,
    pub format: Format,
    #[serde(default)]
    pub framing: Framing,
    pub schema_file: Option<String>,
    pub catalog: Option<onetui_schema_source::Config>,
    pub registry: Option<onetui_schema_source::registry::Config>,
    pub buf: Option<onetui_schema_source::buf::Config>,
    pub reader_schema_file: Option<String>,
    pub message_name: Option<String>,
}

pub(crate) fn validate(bindings: &[Binding]) -> Result<()> {
    ensure!(
        bindings.len() <= 32,
        "NATS supports at most 32 decoder bindings"
    );
    for (index, binding) in bindings.iter().enumerate() {
        crate::core_subscription::validate_subject(&binding.subject)?;
        ensure!(
            !binding.subject.contains(['*', '>']),
            "NATS decoder subject must be exact"
        );
        ensure!(
            !bindings[..index]
                .iter()
                .any(|b| b.subject == binding.subject),
            "Duplicate NATS decoder subject"
        );
        match binding.framing {
            Framing::Raw => ensure!(
                binding.registry.is_none()
                    && [
                        binding.schema_file.is_some(),
                        binding.catalog.is_some(),
                        binding.buf.is_some()
                    ]
                    .into_iter()
                    .filter(|present| *present)
                    .count()
                        == 1,
                "Raw NATS decoder requires exactly one of schema_file, catalog or buf; no registry"
            ),
            Framing::Confluent => {
                ensure!(
                    binding.schema_file.is_none()
                        && binding.catalog.is_none()
                        && binding.buf.is_none(),
                    "Confluent NATS decoder does not accept schema_file, catalog or buf"
                );
                binding
                    .registry
                    .as_ref()
                    .ok_or_else(|| anyhow!("Confluent NATS decoder requires registry"))?
                    .validate()?;
            }
        }
        if let Some(buf) = &binding.buf {
            ensure!(binding.format == Format::Protobuf, "Buf requires Protobuf");
            buf.validate()?;
        }
        if let Some(catalog) = &binding.catalog {
            catalog.validate(binding.format)?;
        }
        for path in binding
            .schema_file
            .iter()
            .chain(binding.reader_schema_file.iter())
        {
            ensure!(
                std::path::Path::new(path).is_absolute()
                    && path.len() <= 4096
                    && !path.chars().any(char::is_control),
                "NATS decoder path must be absolute, at most 4096 bytes, without controls"
            );
        }
        match binding.format {
            Format::Avro => ensure!(
                binding.message_name.is_none(),
                "Avro does not accept message_name"
            ),
            Format::Protobuf => {
                ensure!(
                    binding.reader_schema_file.is_none(),
                    "reader_schema_file requires Avro"
                );
                if binding.framing == Framing::Raw {
                    ensure!(
                        binding.message_name.as_ref().is_some_and(|n| !n.is_empty()
                            && n.len() <= 1024
                            && !n.chars().any(char::is_control)),
                        "Protobuf requires message_name of 1..1024 bytes without controls"
                    );
                } else {
                    ensure!(
                        binding.message_name.is_none(),
                        "Confluent Protobuf selects its message through envelope indexes; omit message_name"
                    );
                }
            }
        }
    }
    Ok(())
}

fn read_file(path: &str) -> Result<Vec<u8>> {
    onetui_schema_source::read_file(path, SCHEMA_BYTES)
}

enum Decoder {
    Avro(onetui_avro::Decoder),
    Protobuf(onetui_protobuf::Decoder),
}

impl Decoder {
    fn load(binding: &mut Binding, remaining: &impl Fn() -> Result<()>) -> Result<Self> {
        remaining()?;
        let bundle = if let Some(buf) = &mut binding.buf {
            onetui_schema_source::Bundle {
                schema: buf.load(remaining)?,
                references: Vec::new(),
            }
        } else if let Some(catalog) = &binding.catalog {
            catalog.load(binding.format, remaining)?
        } else {
            onetui_schema_source::Bundle {
                schema: read_file(
                    binding
                        .schema_file
                        .as_deref()
                        .ok_or_else(|| anyhow!("NATS decoder requires schema_file or catalog"))?,
                )?,
                references: Vec::new(),
            }
        };
        remaining()?;
        match binding.format {
            Format::Avro => {
                let mut decoder = onetui_avro::Decoder::with_references(
                    std::str::from_utf8(&bundle.schema)?,
                    &bundle
                        .references
                        .iter()
                        .map(|s| std::str::from_utf8(s))
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                )?;
                if let Some(path) = &binding.reader_schema_file {
                    remaining()?;
                    decoder = decoder.with_reader(onetui_avro::ReaderSchema::new(
                        std::str::from_utf8(&read_file(path)?)?,
                    )?);
                }
                Ok(Self::Avro(decoder))
            }
            Format::Protobuf => Ok(Self::Protobuf(onetui_protobuf::Decoder::new(
                &bundle.schema,
                binding.message_name.as_deref().unwrap(),
            )?)),
        }
    }
    fn identity(&self) -> String {
        match self {
            Self::Avro(d) => match d.reader_schema_id() {
                Some(reader) => format!("{}&reader={reader}", d.schema_id()),
                None => d.schema_id().into(),
            },
            Self::Protobuf(d) => d.schema_id().into(),
        }
    }
    fn preview(&self, raw: &[u8]) -> Result<Preview> {
        match self {
            Self::Avro(d) => Preview::avro(d, raw),
            Self::Protobuf(d) => Preview::protobuf(d, raw),
        }
    }
}

struct Entry {
    binding: Binding,
    decoder: Option<Result<Decoder, String>>,
    registry: Option<Result<Registry, String>>,
}

impl Entry {
    fn load(&mut self, remaining: &impl Fn() -> Result<()>) -> Result<&Decoder, &str> {
        self.decoder
            .get_or_insert_with(|| {
                Decoder::load(&mut self.binding, remaining).map_err(|e| short(&format!("{e:#}")))
            })
            .as_ref()
            .map_err(String::as_str)
    }

    fn registry(&mut self, remaining: &impl Fn() -> Result<()>) -> Result<&mut Registry> {
        self.registry
            .get_or_insert_with(|| {
                (|| {
                    remaining()?;
                    let registry =
                        Registry::new(self.binding.registry.clone().unwrap(), self.binding.format)?;
                    let reader = self
                        .binding
                        .reader_schema_file
                        .as_deref()
                        .map(|path| {
                            remaining()?;
                            onetui_avro::ReaderSchema::new(std::str::from_utf8(&read_file(path)?)?)
                        })
                        .transpose()?;
                    remaining()?;
                    Ok(registry.with_reader(reader))
                })()
                .map_err(|e: anyhow::Error| short(&format!("{e:#}")))
            })
            .as_mut()
            .map_err(|error| anyhow!("{error}"))
    }

    fn check(&mut self, remaining: &impl Fn() -> Result<()>) -> Result<()> {
        if self.binding.registry.is_some() {
            self.registry(remaining)?.check(remaining)
        } else {
            self.load(remaining)
                .map(|_| ())
                .map_err(|error| anyhow!("{error}"))
        }
    }

    fn preview(
        &mut self,
        raw: &[u8],
        remaining: &impl Fn() -> Result<()>,
    ) -> (Option<String>, Result<Preview>) {
        if self.binding.registry.is_some() {
            return match self.registry(remaining) {
                Ok(registry) => registry.preview(raw, remaining),
                Err(error) => (None, Err(error)),
            };
        }
        let loaded = self.load(remaining);
        let fingerprint = loaded.as_ref().ok().map(|decoder| decoder.identity());
        let preview = loaded
            .map_err(|error| anyhow!("{error}"))
            .and_then(|decoder| decoder.preview(raw));
        let source = self
            .binding
            .buf
            .as_ref()
            .map(|buf| buf.identity())
            .or_else(|| {
                self.binding
                    .catalog
                    .as_ref()
                    .map(|catalog| catalog.identity())
            });
        let identity = match (source, fingerprint) {
            (Some(source), Some(fingerprint)) => Some(format!("{source}:{fingerprint}")),
            (source, fingerprint) => source.or(fingerprint),
        };
        (identity, preview)
    }
}

pub(crate) struct Cache(Mutex<Vec<Entry>>);

struct Cancel(Arc<AtomicBool>);
impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl Cache {
    pub fn new(bindings: Vec<Binding>) -> Arc<Self> {
        Arc::new(Self(Mutex::new(
            bindings
                .into_iter()
                .map(|binding| Entry {
                    binding,
                    decoder: None,
                    registry: None,
                })
                .collect(),
        )))
    }

    pub async fn apply(
        self: &Arc<Self>,
        mut page: Page,
        check: bool,
        deadline: tokio::time::Instant,
    ) -> Result<Page> {
        let permit = WORKERS.acquire().await?;
        let cache = self.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _guard = Cancel(cancelled.clone());
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let remaining = || -> Result<()> {
                ensure!(
                    !cancelled.load(Ordering::Relaxed) && tokio::time::Instant::now() < deadline,
                    "NATS decoding cancelled or timed out"
                );
                Ok(())
            };
            let mut entries = cache
                .0
                .lock()
                .map_err(|_| anyhow!("NATS decoder cache unavailable"))?;
            remaining()?;
            if check {
                for entry in entries.iter_mut() {
                    remaining()?;
                    entry.check(&remaining)?;
                }
                remaining()?;
                return Ok(page);
            }
            let Some(subject) = page.columns.iter().position(|c| c.name == "subject") else {
                return Ok(page);
            };
            let Some(data) = page.columns.iter().position(|c| c.name == "data") else {
                return Ok(page);
            };
            // Quiet or unrelated live batches must keep the same columns as decoded batches.
            for (name, datatype) in [
                ("data_decoded", "JSON projection (not wire bytes)"),
                ("data_schema", "schema identity"),
                ("data_decode_error", "text"),
                ("data_native", "typed JSON inspection (not wire bytes)"),
                ("data_native_error", "text"),
            ] {
                page.columns.push(Column {
                    name: name.into(),
                    datatype: datatype.into(),
                });
            }
            // Raw data is authoritative. Exhausting preview space never removes a message.
            let available = PAGE_BYTES.saturating_sub(page.bytes() * 2 + 4096);
            let reserved = page.rows.len() * 4096;
            if available < reserved {
                for row in &mut page.rows {
                    row.cells.extend([None, None, None, None, None]);
                }
                page.notice
                    .push_str("; decoder previews omitted: display budget; raw fields retained");
                return Ok(page);
            }
            let mut budget = (available - reserved) / 4;
            for row in &mut page.rows {
                remaining()?;
                let mut extra = vec![None; 5];
                if let Some(entry) = entries.iter_mut().find(|e| {
                    row.cells[subject].as_ref().and_then(Value::text) == Some(&e.binding.subject)
                }) && let Some(raw) = &row.cells[data]
                {
                    let (identity, preview) = entry.preview(raw.bytes(), &remaining);
                    extra[1] = identity.map(Value::from);
                    match preview {
                        Ok(Preview { json, native }) => {
                            for (value, output, error) in [(json, 0, 2), (native, 3, 4)] {
                                match value {
                                    Ok(text)
                                        if text.len() <= PREVIEW_BYTES && text.len() <= budget =>
                                    {
                                        budget -= text.len();
                                        extra[output] = Some(Value::Json(text));
                                    }
                                    Ok(_) => extra[error] = Some(
                                        "Decoded preview exceeds display budget; inspect raw bytes"
                                            .into(),
                                    ),
                                    Err(e) => extra[error] = Some(short(&format!("{e:#}")).into()),
                                }
                            }
                        }
                        Err(error) => extra[2] = Some(short(&format!("{error:#}")).into()),
                    }
                }
                row.cells.extend(extra);
            }
            remaining()?;
            ensure!(
                page.bytes() <= PAGE_BYTES,
                "NATS decoded page exceeds 1 MiB"
            );
            Ok(page)
        })
        .await?
    }
}

fn short(value: &str) -> String {
    let value = onetui_core::diagnostic(anyhow!("{value}"), &[]).to_string();
    if value.len() <= 512 {
        value
    } else {
        format!("{}...", &value[..value.floor_char_boundary(509)])
    }
}

#[cfg(test)]
mod remote_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn catalog_config_is_offline_strict_and_excludes_other_sources() {
        let source = "subject='demo.event'\nformat='avro'\ncatalog={directory='/not-read-during-validation',schema='event',references=['child']}";
        let binding: Binding = toml::from_str(source).unwrap();
        validate(std::slice::from_ref(&binding)).unwrap();
        assert!(
            validate(&[Binding {
                schema_file: Some("/event.avsc".into()),
                ..binding.clone()
            }])
            .is_err()
        );
        assert!(
            validate(&[Binding {
                catalog: None,
                ..binding.clone()
            }])
            .is_err()
        );
        assert!(
            validate(&[Binding {
                format: Format::Protobuf,
                message_name: Some("demo.Event".into()),
                ..binding.clone()
            }])
            .is_err()
        );
        for invalid in [
            source.replace("schema='event'", "schema='../event'"),
            source.replace("schema='event'", "schema='event',unexpected=true"),
            source.replace("/not-read-during-validation", "relative"),
            source.replace("references=['child']", "references=['child','child']"),
        ] {
            assert!(
                toml::from_str::<Binding>(&invalid)
                    .map_or(true, |binding| validate(&[binding]).is_err())
            );
        }
    }

    #[tokio::test]
    async fn limits_lazy_files_cancellation_and_raw_budget_are_preserved() {
        let mut schema = tempfile::NamedTempFile::new().unwrap();
        schema.write_all(b"\"bytes\"").unwrap();
        let binding = Binding {
            subject: "demo.bytes".into(),
            format: Format::Avro,
            framing: Framing::Raw,
            schema_file: Some(schema.path().to_str().unwrap().into()),
            catalog: None,
            registry: None,
            buf: None,
            reader_schema_file: None,
            message_name: None,
        };
        validate(std::slice::from_ref(&binding)).unwrap();
        assert!(validate(&[binding.clone(), binding.clone()]).is_err());
        assert!(
            validate(&[Binding {
                subject: "demo.*".into(),
                ..binding.clone()
            }])
            .is_err()
        );
        assert!(
            validate(&[Binding {
                schema_file: Some("relative.avsc".into()),
                ..binding.clone()
            }])
            .is_err()
        );
        let cache = Cache::new(vec![binding]);
        let page = Page {
            columns: vec![
                Column {
                    name: "subject".into(),
                    datatype: "text".into(),
                },
                Column {
                    name: "data".into(),
                    datatype: "bytes".into(),
                },
            ],
            rows: vec![onetui_core::Row {
                cells: vec![
                    Some("demo.bytes".into()),
                    Some(Value::Bytes(vec![0; (PAGE_BYTES - 8192) / 2])),
                ],
                target: None,
            }],
            ..Page::default()
        };
        let raw = page.rows[0].cells[1].clone();
        let page = cache
            .apply(
                page,
                false,
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert_eq!(page.rows[0].cells[1], raw);
        assert!(page.notice.contains("previews omitted"));
        assert!(page.bytes() <= PAGE_BYTES);
        assert!(
            cache
                .apply(Page::default(), true, tokio::time::Instant::now())
                .await
                .is_err()
        );
        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            let fifo = dir.path().join("schema");
            nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::S_IRUSR).unwrap();
            assert!(read_file(fifo.to_str().unwrap()).is_err());
        }
        let mut huge = tempfile::NamedTempFile::new().unwrap();
        huge.write_all(&vec![0; SCHEMA_BYTES + 1]).unwrap();
        assert!(read_file(huge.path().to_str().unwrap()).is_err());
    }
}
