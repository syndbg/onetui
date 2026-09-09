use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Value {
    Text(String),
    Json(String),
    Bytes(Vec<u8>),
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Text(value.into())
    }
}

impl Value {
    pub fn text(&self) -> Option<&str> {
        match self {
            Self::Text(s) | Self::Json(s) => Some(s),
            Self::Bytes(_) => None,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        match self {
            Self::Text(s) | Self::Json(s) => s.as_bytes(),
            Self::Bytes(b) => b,
        }
    }

    pub fn provenance(&self) -> &'static str {
        match self {
            Self::Text(_) => "UTF-8 text",
            Self::Json(_) => "serialized JSON (not wire bytes)",
            Self::Bytes(_) => "binary content",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueFormat {
    #[default]
    Auto,
    Text,
    Json,
    Hex,
    Binary,
}

#[derive(Serialize)]
pub struct FormatDescriptor {
    pub id: ValueFormat,
    pub name: &'static str,
    pub description: &'static str,
}

pub const FORMATS: &[FormatDescriptor] = &[
    FormatDescriptor {
        id: ValueFormat::Auto,
        name: "auto",
        description: "JSON objects/arrays or declared JSON; text otherwise; opaque bytes use hex",
    },
    FormatDescriptor {
        id: ValueFormat::Text,
        name: "text",
        description: "Terminal-safe text; byte input requires valid UTF-8",
    },
    FormatDescriptor {
        id: ValueFormat::Json,
        name: "json",
        description: "Complete valid JSON; preserves keys, numbers and string escapes",
    },
    FormatDescriptor {
        id: ValueFormat::Hex,
        name: "hex",
        description: "Two hexadecimal digits per byte with byte offsets; null has no bytes",
    },
    FormatDescriptor {
        id: ValueFormat::Binary,
        name: "binary",
        description: "Eight binary digits per byte, most significant bit first; null has no bytes",
    },
];

impl ValueFormat {
    pub fn name(self) -> &'static str {
        FORMATS
            .iter()
            .find(|f| f.id == self)
            .expect("registered format")
            .name
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnicodeDisplay {
    #[default]
    Literal,
    Escaped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DisplayOptions {
    pub format: ValueFormat,
    pub pretty_print: bool,
    pub highlight: bool,
    pub word_wrap: bool,
    pub unicode: UnicodeDisplay,
}

impl Default for DisplayOptions {
    fn default() -> Self {
        Self {
            format: ValueFormat::Auto,
            pretty_print: true,
            highlight: true,
            word_wrap: true,
            unicode: UnicodeDisplay::Literal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_and_text_remain_distinct_and_options_are_strict() {
        let bytes = Value::Bytes(vec![0, 255, 65]);
        assert_eq!(bytes.bytes(), [0, 255, 65]);
        assert_eq!(bytes.text(), None);
        assert_ne!(Value::Bytes(vec![]), Value::from(""));
        assert_eq!(Value::from("41").bytes(), b"41");
        assert_eq!(
            toml::from_str::<DisplayOptions>("").unwrap(),
            DisplayOptions::default()
        );
        for format in FORMATS {
            let options: DisplayOptions =
                toml::from_str(&format!("format = {:?}", format.name)).unwrap();
            assert_eq!(options.format, format.id);
        }
        for invalid in [
            "format='HEX'",
            "format=''",
            "unicode='auto'",
            "highlight='off'",
            "pretty_print=1",
            "word_wrap=[]",
            "unknown=true",
        ] {
            assert!(toml::from_str::<DisplayOptions>(invalid).is_err());
        }
    }
}
