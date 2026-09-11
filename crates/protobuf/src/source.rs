use anyhow::{Result, anyhow, ensure};
use protox::file::{File, FileResolver, GoogleFileResolver};
use std::collections::HashMap;

use crate::{Decoder, MAX_DEPTH, MAX_NODES, MAX_SCHEMA_BYTES, fingerprint};

const ROOT: &str = "__onetui_root.proto";

/// Compiled registry sources. Imports resolve only from the supplied bundle or
/// embedded Google well-known types, never the filesystem or network.
pub struct SourceSchema {
    root: prost_reflect::FileDescriptor,
    fingerprint: String,
}

struct Sources(HashMap<String, String>);

impl FileResolver for Sources {
    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        match self.0.get(name) {
            Some(source) => File::from_source(name, source),
            None => GoogleFileResolver::new().open_file(name),
        }
    }
}

impl SourceSchema {
    pub fn compile(root: &str, imports: &[(&str, &str)]) -> Result<Self> {
        ensure!(imports.len() <= 32, "Protobuf imports exceed 32");
        let mut bytes = root.len();
        let mut sources = HashMap::new();
        preflight(root)?;
        sources.insert(ROOT.to_owned(), root.to_owned());
        for &(name, source) in imports {
            ensure!(
                !name.is_empty()
                    && name.len() <= 1024
                    && !name.chars().any(char::is_control)
                    && !name.contains(['\\', ':'])
                    && name.split('/').all(|part| !matches!(part, "" | "." | ".."))
                    && !name.starts_with("google/protobuf/"),
                "Invalid or reserved Protobuf import name"
            );
            bytes = bytes
                .checked_add(source.len())
                .ok_or_else(|| anyhow!("Protobuf source size overflow"))?;
            ensure!(
                bytes <= MAX_SCHEMA_BYTES,
                "Protobuf source bundle exceeds 256 KiB"
            );
            preflight(source)?;
            ensure!(
                sources.insert(name.to_owned(), source.to_owned()).is_none(),
                "Duplicate Protobuf import name"
            );
        }
        let mut compiler = protox::Compiler::with_file_resolver(Sources(sources));
        compiler
            .include_imports(true)
            .include_source_info(false)
            .open_file(ROOT)?;
        let descriptors = compiler.encode_file_descriptor_set();
        ensure!(
            descriptors.len() <= MAX_SCHEMA_BYTES,
            "Compiled descriptor set exceeds 256 KiB"
        );
        let root = compiler
            .descriptor_pool()
            .get_file_by_name(ROOT)
            .ok_or_else(|| anyhow!("Compiled Protobuf root missing"))?;
        // Apply the same descriptor budgets as raw descriptor-file bindings.
        let first = root
            .messages()
            .next()
            .ok_or_else(|| anyhow!("Protobuf root has no messages"))?;
        crate::protobuf::schema(&descriptors, first.full_name())?;
        Ok(Self {
            root,
            fingerprint: fingerprint(&descriptors),
        })
    }

    /// Select a root-to-nested declaration index path from a Confluent envelope.
    pub fn decoder(&self, indexes: &[usize]) -> Result<Decoder> {
        ensure!(
            !indexes.is_empty() && indexes.len() <= MAX_DEPTH,
            "Invalid Protobuf message index depth"
        );
        let mut descriptor = self
            .root
            .messages()
            .nth(indexes[0])
            .ok_or_else(|| anyhow!("Protobuf message index out of range"))?;
        for &index in &indexes[1..] {
            let child = descriptor
                .child_messages()
                .nth(index)
                .ok_or_else(|| anyhow!("Protobuf nested message index out of range"))?;
            descriptor = child;
            ensure!(
                !descriptor.is_map_entry(),
                "Protobuf message index selects a synthetic map entry"
            );
        }
        ensure!(
            descriptor.full_name().len() <= 1024,
            "Protobuf message name exceeds 1024 bytes"
        );
        Ok(Decoder {
            schema_id: format!(
                "protobuf:sha256:{}:{}",
                self.fingerprint,
                descriptor.full_name()
            ),
            descriptor,
        })
    }
}

// protox's parser has no nesting budget. Bound delimiters before calling it,
// excluding quoted strings and comments so data inside options cannot fake depth.
fn preflight(source: &str) -> Result<()> {
    ensure!(
        source.len() <= MAX_SCHEMA_BYTES,
        "Protobuf source bundle exceeds 256 KiB"
    );
    let input = source.as_bytes();
    let (mut i, mut depth, mut nodes) = (0, 0usize, 0usize);
    while i < input.len() {
        match input[i] {
            b'"' | b'\'' => {
                let quote = input[i];
                i += 1;
                while i < input.len() {
                    if input[i] == b'\\' {
                        i += 2;
                    } else if input[i] == quote {
                        i += 1;
                        break;
                    } else {
                        i += 1;
                    }
                }
                continue;
            }
            b'/' if input.get(i + 1) == Some(&b'/') => {
                while i < input.len() && input[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if input.get(i + 1) == Some(&b'*') => {
                i += 2;
                let mut comments = 1;
                while i + 1 < input.len() && comments > 0 {
                    if &input[i..i + 2] == b"/*" {
                        comments += 1;
                        i += 2;
                    } else if &input[i..i + 2] == b"*/" {
                        comments -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                ensure!(comments == 0, "Unterminated Protobuf comment");
                continue;
            }
            b'{' | b'[' | b'(' => {
                depth += 1;
                nodes += 1;
            }
            b'}' | b']' | b')' => {
                depth = depth.saturating_sub(1);
            }
            b';' => {
                nodes += 1;
            }
            _ => {}
        }
        ensure!(depth <= MAX_DEPTH, "Protobuf source nesting exceeds 32");
        ensure!(
            nodes <= MAX_NODES,
            "Protobuf source declarations exceed 4096"
        );
        i += 1;
    }
    Ok(())
}
