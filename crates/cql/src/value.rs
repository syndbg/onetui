use onetui_core::Value;
use scylla::frame::response::result::{CollectionType, ColumnType};
use scylla::value::{CqlDuration, CqlValue};
use std::fmt::Write as _;

/// Render one cell. Scalars become text, collections and structures become JSON so the
/// existing pretty-printer and filters can work on them, and `blob` stays bytes so the
/// display format decides between hex and text.
///
/// Nothing is rounded: `decimal` and `varint` are arbitrary-precision on the wire, so both
/// are converted exactly rather than through a float. Temporal values render in UTC the
/// way cqlsh shows them, computed with integer arithmetic over their full range.
pub fn cell(value: Option<&CqlValue>) -> Option<Value> {
    Some(match value? {
        // An empty value is a legacy distinction from null; showing it as empty text
        // rather than None keeps it distinguishable in the table.
        CqlValue::Empty => Value::Text(String::new()),
        CqlValue::Blob(bytes) => Value::Bytes(bytes.clone()),
        CqlValue::Ascii(text) | CqlValue::Text(text) => Value::Text(text.clone()),
        CqlValue::Boolean(flag) => Value::Text(flag.to_string()),
        CqlValue::Int(number) => Value::Text(number.to_string()),
        CqlValue::BigInt(number) => Value::Text(number.to_string()),
        CqlValue::SmallInt(number) => Value::Text(number.to_string()),
        CqlValue::TinyInt(number) => Value::Text(number.to_string()),
        CqlValue::Counter(counter) => Value::Text(counter.0.to_string()),
        CqlValue::Float(number) => Value::Text(number.to_string()),
        CqlValue::Double(number) => Value::Text(number.to_string()),
        CqlValue::Varint(varint) => Value::Text(integer(varint.as_signed_bytes_be_slice())),
        CqlValue::Decimal(decimal) => {
            let (digits, scale) = decimal.as_signed_be_bytes_slice_and_exponent();
            Value::Text(decimal_text(digits, scale))
        }
        CqlValue::Uuid(uuid) => Value::Text(uuid.to_string()),
        CqlValue::Timeuuid(uuid) => Value::Text(uuid.to_string()),
        CqlValue::Inet(address) => Value::Text(address.to_string()),
        CqlValue::Timestamp(stamp) => Value::Text(timestamp_text(stamp.0)),
        // A date counts days from 2^31 days before the epoch.
        CqlValue::Date(date) => Value::Text(date_text(i64::from(date.0) - (1 << 31))),
        CqlValue::Time(time) => Value::Text(time_text(time.0)),
        CqlValue::Duration(duration) => Value::Text(duration_text(duration)),
        structure @ (CqlValue::List(_)
        | CqlValue::Set(_)
        | CqlValue::Vector(_)
        | CqlValue::Tuple(_)
        | CqlValue::Map(_)
        | CqlValue::UserDefinedType { .. }) => Value::Json(structured(structure).to_string()),
        // CqlValue is non-exhaustive: a driver release can add a type. Showing the
        // driver's own rendering keeps a new type readable instead of unreachable.
        other => Value::Text(format!("{other:?}")),
    })
}

/// JSON for a structure. A map is a list of pairs because CQL keys need not be strings.
fn structured(value: &CqlValue) -> serde_json::Value {
    use serde_json::{Value as Json, json};
    match value {
        CqlValue::List(items) | CqlValue::Set(items) | CqlValue::Vector(items) => {
            items.iter().map(|item| json_of(Some(item))).collect()
        }
        // A tuple keeps its nulls, which are part of its type.
        CqlValue::Tuple(items) => items.iter().map(|item| json_of(item.as_ref())).collect(),
        CqlValue::Map(pairs) => pairs
            .iter()
            .map(|(key, value)| json!({"key": json_of(Some(key)), "value": json_of(Some(value))}))
            .collect(),
        CqlValue::UserDefinedType {
            keyspace,
            name,
            fields,
        } => json!({
            "type": format!("{keyspace}.{name}"),
            "fields": fields
                .iter()
                .map(|(field, value)| (field.clone(), json_of(value.as_ref())))
                .collect::<serde_json::Map<_, _>>(),
        }),
        _ => Json::Null,
    }
}

/// Nested rendering. A nested blob cannot stay bytes inside JSON, so it becomes hex,
/// which the display formats also use for unreadable bytes.
fn json_of(value: Option<&CqlValue>) -> serde_json::Value {
    match value {
        None => serde_json::Value::Null,
        Some(CqlValue::Blob(bytes)) => serde_json::Value::String(hex(bytes)),
        Some(
            structure @ (CqlValue::List(_)
            | CqlValue::Set(_)
            | CqlValue::Vector(_)
            | CqlValue::Tuple(_)
            | CqlValue::Map(_)
            | CqlValue::UserDefinedType { .. }),
        ) => structured(structure),
        Some(scalar) => match cell(Some(scalar)) {
            Some(Value::Text(text)) => serde_json::Value::String(text),
            _ => serde_json::Value::Null,
        },
    }
}

/// A column type in CQL syntax, as `DESCRIBE` writes it.
pub fn type_name(typ: &ColumnType) -> String {
    let frozen = |frozen: bool, inner: String| {
        if frozen {
            format!("frozen<{inner}>")
        } else {
            inner
        }
    };
    match typ {
        // Native variant names are the CQL names in camel case: BigInt is bigint.
        ColumnType::Native(native) => format!("{native:?}").to_lowercase(),
        ColumnType::Collection {
            frozen: is_frozen,
            typ,
        } => frozen(
            *is_frozen,
            match typ {
                CollectionType::List(item) => format!("list<{}>", type_name(item)),
                CollectionType::Set(item) => format!("set<{}>", type_name(item)),
                CollectionType::Map(key, value) => {
                    format!("map<{}, {}>", type_name(key), type_name(value))
                }
                other => format!("{other:?}"),
            },
        ),
        ColumnType::Tuple(items) => format!(
            "tuple<{}>",
            items.iter().map(type_name).collect::<Vec<_>>().join(", ")
        ),
        ColumnType::UserDefinedType {
            frozen: is_frozen,
            definition,
        } => frozen(*is_frozen, definition.name.to_string()),
        ColumnType::Vector { typ, dimensions } => {
            format!("vector<{}, {dimensions}>", type_name(typ))
        }
        other => format!("{other:?}"),
    }
}

/// Proleptic Gregorian (year, month, day) from days since 1970-01-01. Howard Hinnant's
/// `civil_from_days`: exact integer arithmetic, valid for every CQL date and timestamp.
fn civil(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(month <= 2), month, day)
}

fn date_text(days: i64) -> String {
    let (year, month, day) = civil(days);
    format!("{year:04}-{month:02}-{day:02}")
}

fn timestamp_text(millis: i64) -> String {
    let (days, rest) = (millis.div_euclid(86_400_000), millis.rem_euclid(86_400_000));
    format!(
        "{} {:02}:{:02}:{:02}.{:03}Z",
        date_text(days),
        rest / 3_600_000,
        rest / 60_000 % 60,
        rest / 1000 % 60,
        rest % 1000
    )
}

fn time_text(nanos: i64) -> String {
    format!(
        "{:02}:{:02}:{:02}.{:09}",
        nanos / 3_600_000_000_000,
        nanos / 60_000_000_000 % 60,
        nanos / 1_000_000_000 % 60,
        nanos % 1_000_000_000
    )
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(2 + bytes.len() * 2);
    text.push_str("0x");
    for byte in bytes {
        let _ = write!(text, "{byte:02x}");
    }
    text
}

/// A CQL duration carries months, days and nanoseconds separately, because neither
/// converts to the other without a calendar. Keep all three.
fn duration_text(duration: &CqlDuration) -> String {
    format!(
        "{}mo {}d {}ns",
        duration.months, duration.days, duration.nanoseconds
    )
}

/// Exact base-10 text for a two's-complement big-endian integer of any width.
fn integer(bytes: &[u8]) -> String {
    let Some(&first) = bytes.first() else {
        return "0".into();
    };
    let negative = first & 0x80 != 0;
    // Negate in place for a negative value so the digit loop only handles magnitudes.
    let mut magnitude: Vec<u8> = if negative {
        let mut inverted: Vec<u8> = bytes.iter().map(|byte| !byte).collect();
        for byte in inverted.iter_mut().rev() {
            *byte = byte.wrapping_add(1);
            if *byte != 0 {
                break;
            }
        }
        inverted
    } else {
        bytes.to_vec()
    };
    let mut digits = Vec::new();
    // Repeated division by 10 over the byte array; each pass yields one decimal digit.
    while magnitude.iter().any(|&byte| byte != 0) {
        let mut remainder = 0u16;
        for byte in &mut magnitude {
            let current = remainder << 8 | u16::from(*byte);
            *byte = (current / 10) as u8;
            remainder = current % 10;
        }
        digits.push(b'0' + remainder as u8);
    }
    if digits.is_empty() {
        return "0".into();
    }
    if negative {
        digits.push(b'-');
    }
    digits.reverse();
    String::from_utf8(digits).expect("ASCII digits")
}

/// A decimal is an arbitrary-precision integer and a base-10 scale. Place the point by
/// text so no precision is lost to a float.
fn decimal_text(digits: &[u8], scale: i32) -> String {
    let text = integer(digits);
    if scale == 0 {
        return text;
    }
    let (sign, magnitude) = text
        .strip_prefix('-')
        .map_or(("", text.as_str()), |rest| ("-", rest));
    // A negative scale means trailing zeros rather than a fractional part.
    if scale < 0 {
        let zeros = scale.unsigned_abs() as usize;
        return format!("{sign}{magnitude}{}", "0".repeat(zeros));
    }
    let scale = scale as usize;
    if scale >= magnitude.len() {
        let padding = "0".repeat(scale - magnitude.len());
        format!("{sign}0.{padding}{magnitude}")
    } else {
        let point = magnitude.len() - scale;
        format!("{sign}{}.{}", &magnitude[..point], &magnitude[point..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::value::{Counter, CqlDate, CqlDecimal, CqlTime, CqlTimestamp, CqlVarint};

    fn text(value: &CqlValue) -> String {
        match cell(Some(value)) {
            Some(Value::Text(text)) => text,
            other => panic!("expected text, got {other:?}"),
        }
    }

    fn rendered_json(value: &CqlValue) -> serde_json::Value {
        match cell(Some(value)) {
            Some(Value::Json(text)) => serde_json::from_str(&text).expect("valid JSON"),
            other => panic!("expected JSON, got {other:?}"),
        }
    }

    #[test]
    fn column_types_read_as_cql_syntax() {
        use scylla::frame::response::result::NativeType;
        let native = |typ| ColumnType::Native(typ);
        assert_eq!(type_name(&native(NativeType::BigInt)), "bigint");
        assert_eq!(type_name(&native(NativeType::Timeuuid)), "timeuuid");
        assert_eq!(
            type_name(&ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(
                    Box::new(native(NativeType::Text)),
                    Box::new(native(NativeType::Int))
                ),
            }),
            "map<text, int>"
        );
        assert_eq!(
            type_name(&ColumnType::Collection {
                frozen: true,
                typ: CollectionType::List(Box::new(native(NativeType::Uuid))),
            }),
            "frozen<list<uuid>>"
        );
        assert_eq!(
            type_name(&ColumnType::Tuple(vec![
                native(NativeType::Int),
                native(NativeType::Blob)
            ])),
            "tuple<int, blob>"
        );
    }

    #[test]
    fn null_and_empty_stay_distinguishable() {
        assert!(cell(None).is_none());
        // CQL's empty value is not null, so it must not render as one.
        assert_eq!(
            cell(Some(&CqlValue::Empty)),
            Some(Value::Text(String::new()))
        );
    }

    #[test]
    fn scalars_keep_their_exact_value() {
        assert_eq!(text(&CqlValue::Int(-7)), "-7");
        assert_eq!(text(&CqlValue::BigInt(i64::MIN)), i64::MIN.to_string());
        assert_eq!(text(&CqlValue::TinyInt(-128)), "-128");
        assert_eq!(text(&CqlValue::Boolean(true)), "true");
        assert_eq!(text(&CqlValue::Counter(Counter(42))), "42");
        assert_eq!(text(&CqlValue::Text("hi".into())), "hi");
        // A blob stays bytes so the display format chooses hex or text.
        assert_eq!(
            cell(Some(&CqlValue::Blob(vec![0, 255]))),
            Some(Value::Bytes(vec![0, 255]))
        );
        // Temporal values render in UTC like cqlsh, including before the epoch.
        assert_eq!(
            text(&CqlValue::Timestamp(CqlTimestamp(1_700_000_000_000))),
            "2023-11-14 22:13:20.000Z"
        );
        assert_eq!(
            text(&CqlValue::Timestamp(CqlTimestamp(-1))),
            "1969-12-31 23:59:59.999Z"
        );
        assert_eq!(text(&CqlValue::Date(CqlDate(1 << 31))), "1970-01-01");
        // A leap day, which a naive days-per-year conversion gets wrong.
        assert_eq!(
            text(&CqlValue::Date(CqlDate((1 << 31) + 11_016))),
            "2000-02-29"
        );
        // The extremes of both types stay in range.
        assert_eq!(text(&CqlValue::Date(CqlDate(0))), "-5877641-06-23");
        text(&CqlValue::Timestamp(CqlTimestamp(i64::MIN)));
        text(&CqlValue::Timestamp(CqlTimestamp(i64::MAX)));
        assert_eq!(
            text(&CqlValue::Time(CqlTime(86_399_999_999_999))),
            "23:59:59.999999999"
        );
        assert_eq!(
            text(&CqlValue::Duration(CqlDuration {
                months: 1,
                days: 2,
                nanoseconds: 3,
            })),
            "1mo 2d 3ns"
        );
    }

    #[test]
    fn arbitrary_precision_numbers_survive_exactly() {
        // Wider than i64, so a float or an i64 cast would lose digits. 10^21 is
        // 0x3635c9adc5dea00000, one bit past what u64 can hold.
        let big = CqlVarint::from_signed_bytes_be(vec![
            0x36, 0x35, 0xC9, 0xAD, 0xC5, 0xDE, 0xA0, 0x00, 0x00,
        ]);
        assert_eq!(text(&CqlValue::Varint(big)), "1000000000000000000000");
        assert_eq!(
            text(&CqlValue::Varint(CqlVarint::from_signed_bytes_be(vec![
                0x00
            ]))),
            "0"
        );
        // Two's-complement negatives, including the all-ones edge.
        assert_eq!(
            text(&CqlValue::Varint(CqlVarint::from_signed_bytes_be(vec![
                0xFF
            ]))),
            "-1"
        );
        assert_eq!(
            text(&CqlValue::Varint(CqlVarint::from_signed_bytes_be(vec![
                0x80, 0x00
            ]))),
            "-32768"
        );

        // scale > 0 places a fractional point; the integer is unbounded.
        let decimal = CqlDecimal::from_signed_be_bytes_and_exponent(vec![0x04, 0xD2], 2);
        assert_eq!(text(&CqlValue::Decimal(decimal)), "12.34");
        // A scale wider than the digits pads with leading zeros.
        let small = CqlDecimal::from_signed_be_bytes_and_exponent(vec![0x01], 4);
        assert_eq!(text(&CqlValue::Decimal(small)), "0.0001");
        // A negative scale means trailing zeros, not a fraction.
        let scaled = CqlDecimal::from_signed_be_bytes_and_exponent(vec![0x05], -3);
        assert_eq!(text(&CqlValue::Decimal(scaled)), "5000");
        // Sign stays outside the decimal point.
        let negative = CqlDecimal::from_signed_be_bytes_and_exponent(vec![0xFB, 0x2E], 2);
        assert_eq!(text(&CqlValue::Decimal(negative)), "-12.34");
    }

    #[test]
    fn structures_render_as_inspectable_json() {
        assert_eq!(
            rendered_json(&CqlValue::List(vec![
                CqlValue::Int(1),
                CqlValue::Text("two".into())
            ])),
            serde_json::json!(["1", "two"])
        );
        // A tuple keeps its nulls, which are part of its type.
        assert_eq!(
            rendered_json(&CqlValue::Tuple(vec![Some(CqlValue::Int(1)), None,])),
            serde_json::json!(["1", null])
        );
        // A map is a list of pairs: CQL keys are not restricted to strings.
        assert_eq!(
            rendered_json(&CqlValue::Map(vec![(
                CqlValue::Int(1),
                CqlValue::Text("one".into())
            )])),
            serde_json::json!([{"key": "1", "value": "one"}])
        );
        assert_eq!(
            rendered_json(&CqlValue::UserDefinedType {
                keyspace: "ks".into(),
                name: "address".into(),
                fields: vec![
                    ("city".into(), Some(CqlValue::Text("Sofia".into()))),
                    ("zip".into(), None),
                ],
            }),
            serde_json::json!({"type": "ks.address", "fields": {"city": "Sofia", "zip": null}})
        );
        // Nesting stays structured rather than becoming an escaped string.
        assert_eq!(
            rendered_json(&CqlValue::List(vec![CqlValue::List(vec![CqlValue::Int(
                1
            )])])),
            serde_json::json!([["1"]])
        );
        // A nested blob cannot stay bytes inside JSON, so it becomes hex.
        assert_eq!(
            rendered_json(&CqlValue::List(vec![CqlValue::Blob(vec![0xAB, 0xCD])])),
            serde_json::json!(["0xabcd"])
        );
    }
}
