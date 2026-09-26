use anyhow::{Result, anyhow, ensure};
use onetui_core::{Resource, Row};

pub(crate) enum Statement<'a> {
    Consume {
        stream: &'a str,
        replay: crate::replay::Replay,
    },
    Produce {
        stream: &'a str,
        subject: &'a str,
        headers: Vec<(&'a str, &'a str)>,
        payload: Vec<u8>,
    },
}

/// Header line that selects how the payload text is decoded. It is consumed here and
/// never sent, unlike every other header line. Reserved rather than reusing
/// `Content-Encoding`, which is an ordinary header a publisher may need to send as-is.
const ENCODING: &str = "onetui-encoding";

pub(crate) fn parse(text: &str) -> Result<Statement<'_>> {
    let (line, rest) = text.split_once('\n').unwrap_or((text, ""));
    let mut parts = line.split_whitespace();
    let verb = parts
        .next()
        .ok_or_else(|| anyhow!("Enter CONSUME stream or PRODUCE stream subject"))?;
    let stream = parts
        .next()
        .ok_or_else(|| anyhow!("Name a stream on the first line"))?;
    match verb {
        "CONSUME" => {
            // The verb line carries the whole read: the subject it addresses and the
            // range to return.
            let replay = crate::replay::Replay::parse(&mut parts)?;
            ensure!(
                body(rest)?.trim().is_empty(),
                "CONSUME takes no body; put the subject and range on the first line"
            );
            Ok(Statement::Consume { stream, replay })
        }
        "PRODUCE" => {
            let subject = parts
                .next()
                .ok_or_else(|| anyhow!("Enter PRODUCE stream subject on the first line"))?;
            ensure!(
                parts.next().is_none(),
                "Enter PRODUCE stream subject on the first line"
            );
            // Header lines sit between the verb line and the blank line, as they do on
            // the wire. Without any, the blank line still separates verb from payload.
            let mut headers = Vec::new();
            let mut encoding = None;
            let mut remainder = rest;
            while let Some((line, rest)) = remainder.split_once('\n')
                && !line.is_empty()
            {
                let (name, value) = line.split_once(':').ok_or_else(|| {
                    anyhow!(
                        "Header lines are Name: value, or leave a blank line before the payload"
                    )
                })?;
                let name = name.trim();
                let value = value.trim();
                ensure!(
                    !name.is_empty() && name.bytes().all(|b| b.is_ascii_graphic() && b != b':'),
                    "Invalid NATS header name {name}"
                );
                ensure!(
                    !value.bytes().any(|b| b.is_ascii_control()),
                    "NATS header values must not contain control characters"
                );
                if name.eq_ignore_ascii_case(ENCODING) {
                    ensure!(encoding.is_none(), "Repeated {ENCODING} header");
                    encoding = Some(value);
                } else {
                    headers.push((name, value));
                }
                remainder = rest;
            }
            let payload = decode(body(remainder)?, encoding)?;
            Ok(Statement::Produce {
                stream,
                subject,
                headers,
                payload,
            })
        }
        other => Err(anyhow!("Unknown NATS verb {other}; use CONSUME or PRODUCE")),
    }
}

/// The payload is whatever follows the blank line, taken literally.
fn body(rest: &str) -> Result<&str> {
    if rest.is_empty() {
        return Ok("");
    }
    rest.strip_prefix('\n')
        .ok_or_else(|| anyhow!("Leave a blank line before the body"))
}

/// Absent encoding sends the text as UTF-8, which is what most publishes want.
fn decode(payload: &str, encoding: Option<&str>) -> Result<Vec<u8>> {
    let Some(encoding) = encoding else {
        return Ok(payload.as_bytes().to_vec());
    };
    // Whitespace lets a long encoded payload wrap across lines.
    let compact: String = payload.split_whitespace().collect();
    match encoding.to_ascii_lowercase().as_str() {
        "utf-8" | "utf8" | "none" => Ok(payload.as_bytes().to_vec()),
        "base64" => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(&compact)
                .map_err(|error| anyhow!("Invalid base64 payload: {error}"))
        }
        "hex" => {
            ensure!(
                compact.len().is_multiple_of(2),
                "Hex payload must have an even number of digits"
            );
            (0..compact.len())
                .step_by(2)
                .map(|i| {
                    u8::from_str_radix(&compact[i..i + 2], 16)
                        .map_err(|error| anyhow!("Invalid hex payload: {error}"))
                })
                .collect()
        }
        other => Err(anyhow!(
            "Unknown {ENCODING} {other}; use base64, hex or utf-8"
        )),
    }
}

pub(crate) fn watermark(resource: &Resource, row: Option<&Row>) -> String {
    let stream = resource
        .path
        .first()
        .map(String::as_str)
        .unwrap_or("stream");
    let subject = row
        .and_then(|row| row.cells.get(1))
        .and_then(Option::as_ref)
        .and_then(onetui_core::Value::text)
        .unwrap_or("subject");
    format!("PRODUCE {stream} {subject}\n\nhello")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_keep_the_body_and_require_one_target_line() {
        // CONSUME names its subject and range on the verb line, like PRODUCE names its
        // subject there. Range parsing itself is covered in replay.rs.
        let Statement::Consume { stream, replay } =
            parse("CONSUME EVENTS orders.* seq 1..500").unwrap()
        else {
            panic!("expected consume")
        };
        assert_eq!(stream, "EVENTS");
        assert_eq!(replay.subject, "orders.*");
        assert_eq!(
            (replay.start_sequence, replay.end_sequence),
            (Some(1), Some(500))
        );
        // A body is gone; CONSUME is one line.
        assert!(parse("CONSUME EVENTS\n\n{\"subject\":\"orders.*\"}").is_err());
        // A payload is taken literally, newlines and all.
        let Statement::Produce {
            stream,
            subject,
            headers,
            payload,
        } = parse("PRODUCE EVENTS orders.created\n\nfirst\nsecond").unwrap()
        else {
            panic!("expected produce")
        };
        assert_eq!((stream, subject), ("EVENTS", "orders.created"));
        assert!(headers.is_empty());
        assert_eq!(payload, b"first\nsecond");

        for text in [
            "CONSUME",
            "CONSUME EVENTS orders.* seq 1..500 extra",
            "PRODUCE EVENTS",
            "PRODUCE EVENTS orders.created extra",
            "PRODUCE EVENTS orders.created\npayload",
            "REPLAY EVENTS",
        ] {
            assert!(parse(text).is_err(), "{text}");
        }
    }

    #[test]
    fn header_lines_precede_the_payload_and_encoding_is_consumed() {
        let Statement::Produce {
            headers, payload, ..
        } = parse(concat!(
            "PRODUCE EVENTS orders.created\n",
            "src: onetui\n",
            "Nats-Msg-Id:  order-1  \n",
            "\n",
            "hello"
        ))
        .unwrap()
        else {
            panic!("expected produce")
        };
        // Names and values are trimmed; order is preserved.
        assert_eq!(headers, vec![("src", "onetui"), ("Nats-Msg-Id", "order-1")]);
        assert_eq!(payload, b"hello");

        // The encoding header selects the decoding and is never sent as a header.
        for (text, expected) in [
            (
                "Onetui-Encoding: base64

aGVsbG8=",
                b"hello".to_vec(),
            ),
            (
                "onetui-encoding: hex

68656c6c6f",
                b"hello".to_vec(),
            ),
            // Encoded payloads may wrap across lines.
            (
                "Onetui-Encoding: base64

aGVs
bG8=",
                b"hello".to_vec(),
            ),
            (
                "Onetui-Encoding: utf-8

hello",
                b"hello".to_vec(),
            ),
            // Binary that is not valid UTF-8 is exactly what encoding is for.
            (
                "Onetui-Encoding: hex

ff00",
                vec![255, 0],
            ),
        ] {
            let source = format!("PRODUCE EVENTS orders.created\n{text}");
            let Statement::Produce {
                headers, payload, ..
            } = parse(&source).unwrap()
            else {
                panic!("expected produce")
            };
            assert!(headers.is_empty(), "{text}: encoding is not a header");
            assert_eq!(payload, expected, "{text}");
        }

        // An empty payload stays empty rather than becoming a missing body.
        let Statement::Produce { payload, .. } =
            parse("PRODUCE EVENTS orders.created\n\n").unwrap()
        else {
            panic!("expected produce")
        };
        assert!(payload.is_empty());

        for text in [
            "PRODUCE EVENTS orders.created\nnot-a-header\n\nhi",
            "PRODUCE EVENTS orders.created\n: empty-name\n\nhi",
            "PRODUCE EVENTS orders.created\nOnetui-Encoding: base64\n\n!!!!",
            "PRODUCE EVENTS orders.created\nOnetui-Encoding: hex\n\nabc",
            "PRODUCE EVENTS orders.created\nOnetui-Encoding: rot13\n\nhi",
            "PRODUCE EVENTS orders.created\nOnetui-Encoding: hex\nOnetui-Encoding: base64\n\n00",
        ] {
            assert!(parse(text).is_err(), "{text}");
        }
    }
}
