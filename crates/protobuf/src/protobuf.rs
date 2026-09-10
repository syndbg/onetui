use anyhow::{Result, anyhow, ensure};
use prost::encoding::{WireType, decode_key, decode_varint};
use prost_reflect::{
    DescriptorPool, DynamicMessage, Kind, MessageDescriptor, ReflectMessage, Value,
};

use crate::bounds::{Budget, take};

pub(crate) fn schema(bytes: &[u8], name: &str) -> Result<MessageDescriptor> {
    let descriptor = DescriptorPool::global()
        .get_message_by_name("google.protobuf.FileDescriptorSet")
        .ok_or_else(|| anyhow!("Built-in descriptor schema unavailable"))?;
    scan(
        Some(&descriptor),
        &mut &bytes[..],
        &mut Budget::default(),
        0,
        None,
    )?;
    DescriptorPool::decode(bytes)?
        .get_message_by_name(name)
        .ok_or_else(|| anyhow!("Protobuf message not found in descriptor set"))
}

pub(crate) fn decode(descriptor: &MessageDescriptor, bytes: &[u8]) -> Result<DynamicMessage> {
    scan(
        Some(descriptor),
        &mut &bytes[..],
        &mut Budget::default(),
        0,
        None,
    )?;
    Ok(DynamicMessage::decode(descriptor.clone(), bytes)?)
}

// Count wire occurrences, including overwritten fields, before native allocation.
// Unknown length-delimited values stay opaque; only declared messages recurse.
fn scan(
    descriptor: Option<&MessageDescriptor>,
    input: &mut &[u8],
    budget: &mut Budget,
    depth: usize,
    group: Option<u32>,
) -> Result<()> {
    budget.node(depth)?;
    while !input.is_empty() {
        let (number, wire) = decode_key(input)?;
        if wire == WireType::EndGroup {
            ensure!(group == Some(number), "Unexpected Protobuf end-group");
            return Ok(());
        }
        budget.node(depth)?;
        let field = descriptor.and_then(|d| {
            d.get_field(number)
                .map(|f| (f.kind(), f.is_list()))
                .or_else(|| d.get_extension(number).map(|f| (f.kind(), f.is_list())))
        });
        match wire {
            WireType::Varint => {
                decode_varint(input)?;
            }
            WireType::SixtyFourBit => {
                take(input, 8)?;
            }
            WireType::ThirtyTwoBit => {
                take(input, 4)?;
            }
            WireType::LengthDelimited => {
                let len = usize::try_from(decode_varint(input)?)?;
                let mut body = take(input, len)?;
                match field {
                    Some((Kind::Message(child), _)) => {
                        scan(Some(&child), &mut body, budget, depth + 1, None)?
                    }
                    Some((kind, true)) if !matches!(kind, Kind::String | Kind::Bytes) => {
                        while !body.is_empty() {
                            budget.node(depth + 1)?;
                            match kind {
                                Kind::Double | Kind::Fixed64 | Kind::Sfixed64 => {
                                    take(&mut body, 8)?;
                                }
                                Kind::Float | Kind::Fixed32 | Kind::Sfixed32 => {
                                    take(&mut body, 4)?;
                                }
                                _ => {
                                    decode_varint(&mut body)?;
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            WireType::StartGroup => {
                let child = match field {
                    Some((Kind::Message(child), _)) => Some(child),
                    _ => None,
                };
                scan(child.as_ref(), input, budget, depth + 1, Some(number))?;
            }
            WireType::EndGroup => unreachable!(),
        }
    }
    ensure!(group.is_none(), "Truncated Protobuf group");
    Ok(())
}

pub(crate) fn check_json(message: &DynamicMessage) -> Result<()> {
    // prost-reflect's Any JSON mapping recursively decodes embedded bytes.
    // Those bytes have not passed our schema-aware budget scan.
    ensure!(
        message.descriptor().full_name() != "google.protobuf.Any",
        "Protobuf Any JSON unpacking is not supported; inspect typed fields or raw bytes"
    );
    for (_, value) in message.fields() {
        check_value(value)?;
    }
    for (_, value) in message.extensions() {
        check_value(value)?;
    }
    Ok(())
}

fn check_value(value: &Value) -> Result<()> {
    match value {
        Value::Message(message) => check_json(message)?,
        Value::List(values) => {
            for value in values {
                check_value(value)?;
            }
        }
        Value::Map(values) => {
            for value in values.values() {
                check_value(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}
