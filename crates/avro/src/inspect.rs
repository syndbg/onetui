use apache_avro::types::Value;
use serde::{Serialize, Serializer, ser::SerializeMap};

pub(crate) struct Native<'a>(pub &'a Value);

impl Serialize for Native<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        macro_rules! scalar {
            ($kind:expr, $value:expr) => {{
                map.serialize_entry("type", $kind)?;
                map.serialize_entry("value", $value)?;
            }};
        }
        match self.0 {
            Value::Null => scalar!("null", &()),
            Value::Boolean(v) => scalar!("boolean", v),
            Value::Int(v) => scalar!("int", v),
            Value::Long(v) => scalar!("long", v),
            Value::Float(v) if v.is_finite() => scalar!("float", v),
            Value::Float(v) => scalar!("float", &v.to_string()),
            Value::Double(v) if v.is_finite() => scalar!("double", v),
            Value::Double(v) => scalar!("double", &v.to_string()),
            Value::Bytes(v) => scalar!("bytes", v),
            Value::String(v) => scalar!("string", v),
            Value::Fixed(size, v) => {
                scalar!("fixed", v);
                map.serialize_entry("size", size)?;
            }
            Value::Enum(index, symbol) => {
                scalar!("enum", symbol);
                map.serialize_entry("index", index)?;
            }
            Value::Union(index, v) => {
                scalar!("union", &Native(v));
                map.serialize_entry("branch", index)?;
            }
            Value::Array(v) => scalar!("array", &v.iter().map(Native).collect::<Vec<_>>()),
            Value::Map(v) => scalar!(
                "map",
                &v.iter()
                    .map(|(k, v)| (k, Native(v)))
                    .collect::<std::collections::BTreeMap<_, _>>()
            ),
            Value::Record(v) => {
                map.serialize_entry("type", "record")?;
                map.serialize_entry(
                    "fields",
                    &v.iter().map(|(k, v)| (k, Native(v))).collect::<Vec<_>>(),
                )?;
            }
            Value::Date(v) => scalar!("date", v),
            Value::Decimal(v) => scalar!(
                "decimal",
                &Vec::<u8>::try_from(v).map_err(serde::ser::Error::custom)?
            ),
            Value::BigDecimal(_) => {
                return Err(serde::ser::Error::custom(
                    "Avro big-decimal is not supported",
                ));
            }
            Value::TimeMillis(v) => scalar!("time-millis", v),
            Value::TimeMicros(v) => scalar!("time-micros", v),
            Value::TimestampMillis(v) => scalar!("timestamp-millis", v),
            Value::TimestampMicros(v) => scalar!("timestamp-micros", v),
            Value::TimestampNanos(v) => scalar!("timestamp-nanos", v),
            Value::LocalTimestampMillis(v) => scalar!("local-timestamp-millis", v),
            Value::LocalTimestampMicros(v) => scalar!("local-timestamp-micros", v),
            Value::LocalTimestampNanos(v) => scalar!("local-timestamp-nanos", v),
            Value::Duration(v) => {
                map.serialize_entry("type", "duration")?;
                map.serialize_entry("months", &u32::from(v.months()))?;
                map.serialize_entry("days", &u32::from(v.days()))?;
                map.serialize_entry("millis", &u32::from(v.millis()))?;
            }
            Value::Uuid(v) => scalar!("uuid", &v.to_string()),
        }
        map.end()
    }
}
