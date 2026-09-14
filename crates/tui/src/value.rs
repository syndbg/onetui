use std::fmt::Write;

use onetui_core::value::{DisplayOptions, UnicodeDisplay, ValueFormat};
use onetui_core::{PAGE_BYTES, Value};
use unicode_segmentation::UnicodeSegmentation;

pub const BYTE_CHUNK: usize = 256;
pub const TEXT_CHUNK: usize = 4096;
pub const JSON_DEPTH: usize = 64;

pub fn preview_prefix(source: &str, budget: usize) -> (String, bool) {
    let mut lines = 1;
    let mut end = 0;
    let mut limited = false;
    for (offset, g) in source.grapheme_indices(true) {
        if g == "\n" {
            if lines == 3 {
                break;
            }
            lines += 1;
        }
        if offset + g.len() > budget.saturating_sub('…'.len_utf8()) {
            limited = true;
            break;
        }
        end = offset + g.len();
    }
    let mut preview = source[..end].to_owned();
    if end < source.len() && budget >= '…'.len_utf8() {
        preview.push('…');
    }
    (preview, limited)
}

pub struct Prepared {
    pub format: ValueFormat,
    pub text: String,
    pub notice: &'static str,
}

fn unsafe_char(c: char) -> bool {
    c.is_control()
        || matches!(c, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{2028}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
}

pub fn text(value: &str, unicode: UnicodeDisplay, lines: bool) -> Result<String, &'static str> {
    let mut out = String::new();
    for c in value.chars() {
        if lines && c == '\n' {
            out.push(c);
        } else if unsafe_char(c)
            || (unicode == UnicodeDisplay::Escaped && (!c.is_ascii() || c == '\\'))
        {
            out.extend(c.escape_default());
        } else {
            out.push(c);
        }
        if out.len() > PAGE_BYTES {
            return Err("Text formatting exceeds 1 MiB; use hex/binary");
        }
    }
    Ok(out)
}

pub fn projection(value: &Value) -> Result<String, &'static str> {
    match value {
        Value::Text(s) | Value::Json(s) => text(s, UnicodeDisplay::Literal, false),
        Value::Bytes(bytes) => {
            if bytes.len() > (PAGE_BYTES - 2) / 2 {
                return Err("Byte preview exceeds 1 MiB");
            }
            let mut out = String::from("\\x");
            for byte in bytes {
                write!(out, "{byte:02x}").unwrap();
            }
            Ok(out)
        }
    }
}

fn json(source: &str, options: DisplayOptions, compact: bool) -> Result<String, &'static str> {
    let pretty = options.pretty_print && !compact;
    let mut out = String::new();
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    let mut previous = ' ';
    for c in source.chars() {
        if quoted {
            if escaped {
                out.push(c);
                escaped = false;
            } else if c == '\\' {
                out.push(c);
                escaped = true;
            } else if c == '"' {
                out.push(c);
                quoted = false;
            } else if unsafe_char(c)
                || (options.unicode == UnicodeDisplay::Escaped && !c.is_ascii())
            {
                for unit in c.encode_utf16(&mut [0; 2]) {
                    write!(out, "\\u{unit:04x}").unwrap();
                }
            } else {
                out.push(c);
            }
        } else if c.is_ascii_whitespace() {
            if !pretty && !compact {
                if matches!(c, ' ' | '\n') {
                    out.push(c);
                } else {
                    out.extend(c.escape_default());
                }
            }
            if out.len() > PAGE_BYTES {
                return Err("JSON formatting exceeds 1 MiB; use text/hex/binary");
            }
            continue;
        } else {
            let close = matches!(c, '}' | ']');
            if close {
                depth = depth
                    .checked_sub(1)
                    .ok_or("Invalid JSON; use text/hex/binary")?;
            }
            if pretty
                && ((close && !matches!(previous, '{' | '['))
                    || (!close && matches!(previous, '{' | '[')))
            {
                out.push('\n');
                out.extend(std::iter::repeat_n(' ', depth * 2));
            }
            out.push(c);
            match c {
                '{' | '[' => {
                    depth += 1;
                    if depth > JSON_DEPTH {
                        return Err("JSON nesting exceeds 64; use text/hex/binary");
                    }
                }
                '"' => quoted = true,
                ',' if pretty => {
                    out.push('\n');
                    out.extend(std::iter::repeat_n(' ', depth * 2));
                }
                ':' if pretty => out.push(' '),
                _ => {}
            }
            previous = c;
        }
        if out.len() > PAGE_BYTES {
            return Err("JSON formatting exceeds 1 MiB; use text/hex/binary");
        }
    }
    // Validate without materializing a map or floating point numbers. The bounded scan above
    // checks nesting before RawValue's validator sees adversarial input.
    serde_json::from_str::<&serde_json::value::RawValue>(source)
        .map_err(|_| "Invalid JSON; use text/hex/binary")?;
    Ok(out)
}

pub fn prepare(value: Option<&Value>, options: DisplayOptions, declared_json: bool) -> Prepared {
    prepare_value(value, options, declared_json, false)
}

pub fn prepare_table(
    value: Option<&Value>,
    options: DisplayOptions,
    declared_json: bool,
) -> Prepared {
    prepare_value(value, options, declared_json, true)
}

fn prepare_value(
    value: Option<&Value>,
    options: DisplayOptions,
    declared_json: bool,
    compact: bool,
) -> Prepared {
    let Some(value) = value else {
        return Prepared {
            format: ValueFormat::Text,
            text: String::new(),
            notice: "SQL NULL; no bytes",
        };
    };
    let source = std::str::from_utf8(value.bytes());
    let format = if options.format == ValueFormat::Auto {
        if source.is_err() {
            ValueFormat::Hex
        } else if declared_json
            || matches!(value, Value::Json(_))
            || source.is_ok_and(|s| matches!(s.trim_start().chars().next(), Some('{' | '[')))
        {
            ValueFormat::Json
        } else {
            ValueFormat::Text
        }
    } else {
        options.format
    };
    let result = match format {
        ValueFormat::Hex | ValueFormat::Binary => {
            return Prepared {
                format,
                text: String::new(),
                notice: "Pretty print does not apply",
            };
        }
        ValueFormat::Text => source
            .map_err(|_| "Invalid UTF-8; use hex/binary")
            .and_then(|s| text(s, options.unicode, true)),
        ValueFormat::Json => source
            .map_err(|_| "Invalid UTF-8; use hex/binary")
            .and_then(|s| json(s, options, compact)),
        ValueFormat::Auto => unreachable!("auto format was resolved above"),
    };
    match result {
        Ok(text)
            if text
                .graphemes(true)
                .all(|g| g.chars().count() <= TEXT_CHUNK) =>
        {
            Prepared {
                format,
                text,
                notice: if format == ValueFormat::Text {
                    "Pretty print does not apply"
                } else {
                    ""
                },
            }
        }
        Ok(_) => Prepared {
            format: ValueFormat::Hex,
            text: String::new(),
            notice: "Grapheme exceeds 4096 characters; showing hex",
        },
        Err(error) => {
            if options.format == ValueFormat::Auto
                && !declared_json
                && !matches!(value, Value::Json(_))
                && let Ok(source) = source
                && let Ok(text) = text(source, options.unicode, true)
                && text
                    .graphemes(true)
                    .all(|g| g.chars().count() <= TEXT_CHUNK)
            {
                return Prepared {
                    format: ValueFormat::Text,
                    text,
                    notice: error,
                };
            }
            Prepared {
                format: ValueFormat::Hex,
                text: String::new(),
                notice: error,
            }
        }
    }
}

pub fn byte_chunk(bytes: &[u8], format: ValueFormat, chunk: usize) -> String {
    let start = chunk.saturating_mul(BYTE_CHUNK).min(bytes.len());
    let end = (start + BYTE_CHUNK).min(bytes.len());
    let width = if format == ValueFormat::Binary { 8 } else { 16 };
    let mut out = String::new();
    for (line, bytes) in bytes[start..end].chunks(width).enumerate() {
        if line > 0 {
            out.push('\n');
        }
        write!(out, "{:08x}: ", start + line * width).unwrap();
        for (i, byte) in bytes.iter().enumerate() {
            if i > 0 {
                out.push(' ');
            }
            if format == ValueFormat::Binary {
                write!(out, "{byte:08b}").unwrap();
            } else {
                write!(out, "{byte:02x}").unwrap();
            }
        }
    }
    out
}

pub fn chunk_ranges(text: &str) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut chars = 0;
    for (offset, grapheme) in text.grapheme_indices(true) {
        let count = grapheme.chars().count();
        if chars > 0 && chars + count > TEXT_CHUNK {
            ranges.push(start..offset);
            start = offset;
            chars = 0;
        }
        chars += count;
    }
    ranges.push(start..text.len());
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_table_json_preserves_lexemes_and_ignores_pretty_print() {
        let compact = r#"{"z":1.000000000000000001,"z":-0,"n":1e999,"s":"a \" b\\c\n","unicode":"界🌊","empty":[]}"#;
        let source = format!("{{\r\n\t{}", &compact[1..]);
        for value in [
            Value::Json(source.clone()),
            Value::Text(source.clone()),
            Value::Bytes(source.clone().into_bytes()),
        ] {
            for pretty_print in [false, true] {
                let options = DisplayOptions {
                    pretty_print,
                    ..DisplayOptions::default()
                };
                let table = prepare_table(Some(&value), options, false);
                assert_eq!(table.format, ValueFormat::Json);
                assert_eq!(table.text, compact);
                assert_eq!(value.bytes(), source.as_bytes());
                assert!(prepare(Some(&value), options, false).text.contains('\n'));
            }
        }
        let bad = Value::Text("{not JSON\t".into());
        assert_eq!(
            prepare_table(Some(&bad), DisplayOptions::default(), false).format,
            ValueFormat::Text
        );
        let deep = Value::Json(format!("{}0{}", "[".repeat(65), "]".repeat(65)));
        let rejected = prepare_table(Some(&deep), DisplayOptions::default(), false);
        assert_eq!(rejected.format, ValueFormat::Hex);
        assert!(rejected.notice.contains("64"));
    }

    #[test]
    fn preview_cache_limits_lines_and_bytes_not_characters() {
        let long = format!("{}END", "界e\u{301}".repeat(200));
        assert_eq!(preview_prefix(&long, PAGE_BYTES), (long.clone(), false));
        assert_eq!(
            preview_prefix("one\ntwo\nthree\nfour", PAGE_BYTES),
            ("one\ntwo\nthree…".into(), false)
        );
        for budget in 0..40 {
            let (preview, limited) = preview_prefix(&long, budget);
            assert!(limited);
            assert!(preview.len() <= budget);
            assert!(long.starts_with(preview.trim_end_matches('…')));
            assert!(!preview.ends_with("e…"), "must not split a grapheme");
        }
    }

    #[test]
    fn json_formatting_is_lossless_bounded_and_independent() {
        let raw =
            r#"{"z":1.000000000000000001,"z":-0,"n":1e999,"s":"a\"b\\c\n","empty":[],"obj":{}}"#;
        let options = DisplayOptions::default();
        let pretty = json(raw, options, false).unwrap();
        assert!(pretty.contains("\n  \"z\": 1.000000000000000001,\n  \"z\": -0,"));
        assert!(pretty.contains("1e999"));
        assert!(pretty.contains(r#""s": "a\"b\\c\n""#));
        assert!(pretty.contains("\"empty\": []"));
        assert_eq!(
            json(
                raw,
                DisplayOptions {
                    pretty_print: false,
                    ..options
                },
                false,
            )
            .unwrap(),
            raw
        );
        for bad in ["{oops}", "[1,]", "{} trailing", "{\"a\":\"\n\"}"] {
            assert!(json(bad, options, false).is_err());
        }
        assert!(
            json(
                &format!("{}0{}", "[".repeat(65), "]".repeat(65)),
                options,
                false
            )
            .is_err()
        );
        assert!(
            json(
                &format!("[{}]", "0,".repeat(PAGE_BYTES / 2)),
                options,
                false
            )
            .is_err()
        );
        assert_eq!(
            json(
                "\"🌊\"",
                DisplayOptions {
                    unicode: UnicodeDisplay::Escaped,
                    ..options
                },
                false,
            )
            .unwrap(),
            "\"\\ud83c\\udf0a\""
        );
        let safe = json(
            "{\r\n\t\"x\": 1}",
            DisplayOptions {
                pretty_print: false,
                ..options
            },
            false,
        )
        .unwrap();
        assert!(safe.contains("\\r\n\\t"));
        assert!(!safe.chars().any(|c| c.is_control() && c != '\n'));
    }

    #[test]
    fn auto_decodes_readable_bytes_without_changing_the_source() {
        let options = DisplayOptions::default();
        for (source, format, expected) in [
            ("demo", ValueFormat::Text, "demo"),
            ("", ValueFormat::Text, ""),
            ("42", ValueFormat::Text, "42"),
            ("{incomplete", ValueFormat::Text, "{incomplete"),
            ("София 🌊\x1b", ValueFormat::Text, "София 🌊\\u{1b}"),
            ("{\"x\":1}", ValueFormat::Json, "{\n  \"x\": 1\n}"),
            ("[1]", ValueFormat::Json, "[\n  1\n]"),
        ] {
            let value = Value::Bytes(source.as_bytes().to_vec());
            let prepared = prepare(Some(&value), options, false);
            assert_eq!(prepared.format, format, "{source:?}");
            assert_eq!(prepared.text, expected, "{source:?}");
            assert_eq!(value.bytes(), source.as_bytes());
            for format in [ValueFormat::Hex, ValueFormat::Binary] {
                assert_eq!(
                    prepare(Some(&value), DisplayOptions { format, ..options }, false).format,
                    format
                );
            }
        }
        let json = Value::Bytes(br#"{"x":1}"#.to_vec());
        assert_eq!(
            prepare(
                Some(&json),
                DisplayOptions {
                    pretty_print: false,
                    ..options
                },
                false
            )
            .text,
            r#"{"x":1}"#
        );
        assert_eq!(prepare(None, options, false).notice, "SQL NULL; no bytes");
        assert_ne!(
            prepare(Some(&Value::Bytes(vec![])), options, false).notice,
            "SQL NULL; no bytes"
        );
    }

    #[test]
    fn all_bytes_and_unicode_chunks_are_preserved_without_terminal_controls() {
        let bytes: Vec<u8> = (0..=255).collect();
        assert!(byte_chunk(&bytes, ValueFormat::Hex, 0).ends_with("fe ff"));
        assert!(
            byte_chunk(&[0, 255, 65], ValueFormat::Binary, 0)
                .ends_with("00000000 11111111 01000001")
        );
        assert_eq!(
            prepare(Some(&Value::Bytes(bytes)), DisplayOptions::default(), false).format,
            ValueFormat::Hex
        );
        let invalid = prepare(
            Some(&Value::Bytes(vec![255])),
            DisplayOptions {
                format: ValueFormat::Text,
                ..DisplayOptions::default()
            },
            false,
        );
        assert!(invalid.notice.contains("Invalid UTF-8"));
        let escaped = text(
            "🌊\\u{1f30a}\x1b]52;bad\u{202e}",
            UnicodeDisplay::Escaped,
            true,
        )
        .unwrap();
        assert!(escaped.starts_with("\\u{1f30a}\\\\u{1f30a}"));
        assert!(!escaped.chars().any(char::is_control));
        let source = "a👩‍💻e\u{301}東京".repeat(2000);
        let ranges = chunk_ranges(&source);
        assert_eq!(
            ranges
                .iter()
                .map(|r| &source[r.clone()])
                .collect::<String>(),
            source
        );
        assert!(
            ranges
                .iter()
                .all(|r| source.is_char_boundary(r.start) && source.is_char_boundary(r.end))
        );
    }
}
