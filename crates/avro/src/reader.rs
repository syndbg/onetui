use crate::bounds::Budget;
use anyhow::{Result, anyhow, ensure};
use apache_avro::{
    Schema,
    schema::{NamesRef, ResolvedSchema},
    types::Value,
};

/// An explicit, self-contained reader schema. Named types must be defined in this document.
#[derive(Clone)]
pub struct ReaderSchema {
    schema: std::sync::Arc<Schema>,
    identity: String,
}

impl ReaderSchema {
    pub fn new(text: &str) -> Result<Self> {
        ensure!(
            text.len() <= crate::MAX_SCHEMA_BYTES,
            "Avro reader schema exceeds 256 KiB"
        );
        Ok(Self {
            schema: std::sync::Arc::new(crate::avro::schema(text)?),
            identity: format!("avro:sha256:{}", crate::fingerprint(text.as_bytes())),
        })
    }

    pub fn schema_id(&self) -> &str {
        &self.identity
    }

    pub(crate) fn resolve(&self, value: &Value) -> Result<Value> {
        let names = ResolvedSchema::try_from(self.schema.as_ref())?;
        guard(
            value,
            &self.schema,
            names.get_names(),
            None,
            &mut Budget::default(),
            0,
        )?;
        Ok(value.clone().resolve(&self.schema)?)
    }
}

// Reader defaults can multiply a tiny writer value or recursively fill missing
// fields. Bound expansion before native resolution, which still owns semantics.
fn guard(
    value: &Value,
    schema: &Schema,
    names: &NamesRef<'_>,
    namespace: Option<&str>,
    budget: &mut Budget,
    depth: usize,
) -> Result<()> {
    budget.node(depth)?;
    if let Value::Union(_, value) = value {
        return guard(value, schema, names, namespace, budget, depth + 1);
    }
    match schema {
        Schema::Ref { name } => {
            let name = name.fully_qualified_name(namespace);
            let schema = names
                .get(name.as_ref())
                .ok_or_else(|| anyhow!("Unresolved reader schema reference"))?;
            guard(value, schema, names, name.namespace(), budget, depth + 1)?;
        }
        Schema::Union(union) => {
            // Charge every candidate: native resolution selects the compatible branch.
            for schema in union.variants() {
                guard(value, schema, names, namespace, budget, depth + 1)?;
            }
        }
        Schema::Record(record) if matches!(value, Value::Record(_) | Value::Map(_)) => {
            let name = record.name.fully_qualified_name(namespace);
            budget.collection(record.fields.len())?;
            for field in &record.fields {
                budget.name(&field.name)?;
                let supplied = match value {
                    Value::Record(fields) => fields
                        .iter()
                        .find(|(n, _)| n == &field.name)
                        .map(|(_, v)| v),
                    Value::Map(fields) => fields.get(&field.name),
                    _ => unreachable!(),
                };
                if let Some(value) = supplied {
                    guard(
                        value,
                        &field.schema,
                        names,
                        name.namespace(),
                        budget,
                        depth + 1,
                    )?;
                } else if let Some(default) = &field.default {
                    crate::bounds::schema_json(default, budget, depth + 1)?;
                    charge_strings(default, budget)?;
                    let value = Value::try_from(default.clone())?;
                    guard(
                        &value,
                        &field.schema,
                        names,
                        name.namespace(),
                        budget,
                        depth + 1,
                    )?;
                }
            }
        }
        Schema::Array(array) => {
            if let Value::Array(values) = value {
                budget.collection(values.len())?;
                for value in values {
                    guard(value, &array.items, names, namespace, budget, depth + 1)?;
                }
            }
        }
        Schema::Map(map) => {
            if let Value::Map(values) = value {
                budget.collection(values.len())?;
                for (key, value) in values {
                    budget.name(key)?;
                    guard(value, &map.types, names, namespace, budget, depth + 1)?;
                }
            }
        }
        Schema::BigDecimal => anyhow::bail!("Avro big-decimal is not supported; inspect raw bytes"),
        Schema::Enum(enumeration) => {
            if let Some(symbol) = enumeration.symbols.iter().max_by_key(|s| s.len()) {
                budget.name(symbol)?;
            }
        }
        _ => match value {
            Value::String(value) => budget.name(value)?,
            Value::Bytes(value) | Value::Fixed(_, value) => {
                ensure!(
                    value.len() <= crate::MAX_OUTPUT_BYTES,
                    "Reader value exceeds 1 MiB"
                );
            }
            _ => {}
        },
    }
    Ok(())
}

fn charge_strings(value: &serde_json::Value, budget: &mut Budget) -> Result<()> {
    match value {
        serde_json::Value::String(s) => budget.name(s)?,
        serde_json::Value::Array(values) => {
            for value in values {
                charge_strings(value, budget)?;
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                charge_strings(value, budget)?;
            }
        }
        _ => {}
    }
    Ok(())
}
