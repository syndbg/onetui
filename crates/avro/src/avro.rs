use crate::bounds::{Budget, take};
use anyhow::{Result, anyhow, ensure};
use apache_avro::{
    Schema,
    schema::{InnerDecimalSchema, NamesRef, ResolvedSchema, UuidSchema},
};

pub(crate) fn schema(text: &str) -> Result<Schema> {
    let json: serde_json::Value = serde_json::from_str(text)?;
    crate::bounds::schema_json(&json, &mut Budget::default(), 0)?;
    let schema = Schema::parse(&json)?;
    ResolvedSchema::try_from(&schema)?;
    Ok(schema)
}

pub(crate) fn decode(
    schema: &Schema,
    references: &[Schema],
    bytes: &[u8],
) -> Result<apache_avro::types::Value> {
    let resolved = ResolvedSchema::new_with_schemata(
        references.iter().chain(std::iter::once(schema)).collect(),
    )?;
    let mut input = bytes;
    scan(
        schema,
        resolved.get_names(),
        None,
        &mut input,
        &mut Budget::default(),
        0,
    )?;
    ensure!(input.is_empty(), "Trailing bytes after Avro datum");
    let mut input = bytes;
    let value = apache_avro::reader::datum::GenericDatumReader::builder(schema)
        .resolved_writer_schemata(resolved)
        .build()?
        .read_value(&mut input)?;
    ensure!(input.is_empty(), "Trailing bytes after Avro datum");
    Ok(value)
}

fn long(input: &mut &[u8]) -> Result<i64> {
    let mut n = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = take(input, 1)?[0];
        ensure!(shift != 63 || byte <= 1, "Avro long overflow");
        n |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((n >> 1) as i64 ^ -((n & 1) as i64));
        }
    }
    anyhow::bail!("Avro long overflow")
}

fn bytes(input: &mut &[u8]) -> Result<()> {
    let len = usize::try_from(long(input)?)?;
    take(input, len)?;
    Ok(())
}

// A tiny Avro datum can request millions of zero-width values. Scan structure
// without allocating them; native decoding remains responsible for semantics.
fn scan(
    schema: &Schema,
    names: &NamesRef<'_>,
    namespace: Option<&str>,
    input: &mut &[u8],
    budget: &mut Budget,
    depth: usize,
) -> Result<()> {
    budget.node(depth)?;
    match schema {
        Schema::Null => {}
        Schema::Boolean => {
            take(input, 1)?;
        }
        Schema::Int
        | Schema::Long
        | Schema::Date
        | Schema::TimeMillis
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => {
            long(input)?;
        }
        Schema::Float => {
            take(input, 4)?;
        }
        Schema::Double => {
            take(input, 8)?;
        }
        Schema::Bytes | Schema::String => bytes(input)?,
        Schema::Fixed(fixed) | Schema::Duration(fixed) => {
            take(input, fixed.size)?;
        }
        Schema::Decimal(decimal) => match &decimal.inner {
            InnerDecimalSchema::Bytes => bytes(input)?,
            InnerDecimalSchema::Fixed(fixed) => {
                take(input, fixed.size)?;
            }
        },
        // BigDecimal permits an arbitrary scale which can expand during conversion.
        Schema::BigDecimal => anyhow::bail!("Avro big-decimal is not supported; inspect raw bytes"),
        Schema::Uuid(uuid) => match uuid {
            UuidSchema::String | UuidSchema::Bytes => bytes(input)?,
            UuidSchema::Fixed(fixed) => {
                take(input, fixed.size)?;
            }
        },
        Schema::Enum(enumeration) => {
            let index = usize::try_from(long(input)?)?;
            let symbol = enumeration
                .symbols
                .get(index)
                .ok_or_else(|| anyhow!("Invalid Avro enum index"))?;
            budget.name(symbol)?;
        }
        Schema::Union(union) => {
            let index = usize::try_from(long(input)?)?;
            let variant = union
                .variants()
                .get(index)
                .ok_or_else(|| anyhow!("Invalid Avro union index"))?;
            scan(variant, names, namespace, input, budget, depth + 1)?;
        }
        Schema::Record(record) => {
            budget.collection(record.fields.len())?;
            let name = record.name.fully_qualified_name(namespace);
            for field in &record.fields {
                budget.name(&field.name)?;
                scan(
                    &field.schema,
                    names,
                    name.namespace(),
                    input,
                    budget,
                    depth + 1,
                )?;
            }
        }
        Schema::Ref { name } => {
            let name = name.fully_qualified_name(namespace);
            let resolved = names
                .get(name.as_ref())
                .ok_or_else(|| anyhow!("Unresolved Avro schema reference"))?;
            scan(resolved, names, name.namespace(), input, budget, depth + 1)?;
        }
        Schema::Array(_) | Schema::Map(_) => loop {
            let count = long(input)?;
            if count == 0 {
                break;
            }
            let block_bytes = if count < 0 {
                Some(usize::try_from(long(input)?)?)
            } else {
                None
            };
            let count = usize::try_from(
                count
                    .checked_abs()
                    .ok_or_else(|| anyhow!("Avro count overflow"))?,
            )?;
            budget.collection(count)?;
            let before = input.len();
            for _ in 0..count {
                let child = match schema {
                    Schema::Array(array) => &array.items,
                    Schema::Map(map) => {
                        budget.node(depth + 1)?;
                        bytes(input)?;
                        &map.types
                    }
                    _ => unreachable!(),
                };
                scan(child, names, namespace, input, budget, depth + 1)?;
            }
            ensure!(
                block_bytes.is_none_or(|size| size == before - input.len()),
                "Invalid Avro block byte size"
            );
        },
    }
    Ok(())
}
