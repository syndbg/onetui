use prost_reflect::{DynamicMessage, MapKey, ReflectMessage, Value};
use serde::{
    Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};

pub(crate) struct Message<'a>(pub &'a DynamicMessage);

impl Serialize for Message<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("type", self.0.descriptor().full_name())?;
        map.serialize_entry("fields", &Fields(self.0))?;
        map.serialize_entry("unknown_fields", &Unknown(self.0))?;
        map.end()
    }
}

struct Fields<'a>(&'a DynamicMessage);
impl Serialize for Fields<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(None)?;
        for (field, value) in self.0.fields() {
            seq.serialize_element(&Field {
                name: field.name(),
                number: field.number(),
                datatype: format!("{:?}", field.kind()),
                extension: false,
                value: Native(value),
            })?;
        }
        for (field, value) in self.0.extensions() {
            seq.serialize_element(&Field {
                name: field.full_name(),
                number: field.number(),
                datatype: format!("{:?}", field.kind()),
                extension: true,
                value: Native(value),
            })?;
        }
        seq.end()
    }
}

#[derive(Serialize)]
struct Field<'a> {
    name: &'a str,
    number: u32,
    datatype: String,
    extension: bool,
    value: Native<'a>,
}

struct Unknown<'a>(&'a DynamicMessage);
impl Serialize for Unknown<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(None)?;
        for field in self.0.unknown_fields() {
            let mut bytes = Vec::new();
            field.encode(&mut bytes);
            // These are re-encoded field bytes. The original record remains authoritative.
            seq.serialize_element(&serde_json::json!({
                "number": field.number(), "wire_type": format!("{:?}", field.wire_type()),
                "encoded": bytes,
            }))?;
        }
        seq.end()
    }
}

struct Native<'a>(&'a Value);
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
            Value::Bool(v) => scalar!("bool", v),
            Value::I32(v) => scalar!("i32", v),
            Value::I64(v) => scalar!("i64", v),
            Value::U32(v) => scalar!("u32", v),
            Value::U64(v) => scalar!("u64", v),
            Value::F32(v) if v.is_finite() => scalar!("f32", v),
            Value::F32(v) => scalar!("f32", &v.to_string()),
            Value::F64(v) if v.is_finite() => scalar!("f64", v),
            Value::F64(v) => scalar!("f64", &v.to_string()),
            Value::String(v) => scalar!("string", v),
            Value::Bytes(v) => scalar!("bytes", &v.as_ref()),
            Value::EnumNumber(v) => scalar!("enum_number", v),
            Value::Message(v) => scalar!("message", &Message(v)),
            Value::List(v) => scalar!("list", &v.iter().map(Native).collect::<Vec<_>>()),
            Value::Map(v) => {
                let mut entries = v.iter().collect::<Vec<_>>();
                entries.sort_by_key(|(key, _)| *key);
                scalar!(
                    "map",
                    &entries
                        .iter()
                        .map(|(k, v)| (Key(k), Native(v)))
                        .collect::<Vec<_>>()
                );
            }
        }
        map.end()
    }
}

struct Key<'a>(&'a MapKey);
impl Serialize for Key<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            MapKey::Bool(v) => Native(&Value::Bool(*v)).serialize(serializer),
            MapKey::I32(v) => Native(&Value::I32(*v)).serialize(serializer),
            MapKey::I64(v) => Native(&Value::I64(*v)).serialize(serializer),
            MapKey::U32(v) => Native(&Value::U32(*v)).serialize(serializer),
            MapKey::U64(v) => Native(&Value::U64(*v)).serialize(serializer),
            MapKey::String(v) => Native(&Value::String(v.clone())).serialize(serializer),
        }
    }
}
