use anyhow::{Result, bail, ensure};
use aws_sdk_dynamodb::{primitives::Blob, types::AttributeValue};
use base64::Engine;
use serde_json::Value;
use std::collections::HashMap;

pub(crate) fn item(value: &Value) -> Result<HashMap<String, AttributeValue>> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("DynamoDB item must be an attribute map"))?;
    object
        .iter()
        .map(|(name, value)| Ok((name.clone(), attribute(value, 0)?)))
        .collect()
}

fn attribute(value: &Value, depth: usize) -> Result<AttributeValue> {
    ensure!(depth <= 32, "DynamoDB attribute nesting exceeds 32");
    let object = value
        .as_object()
        .filter(|v| v.len() == 1)
        .ok_or_else(|| anyhow::anyhow!("DynamoDB attributes require exactly one type tag"))?;
    let (tag, value) = object.iter().next().unwrap();
    let string = || {
        value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("DynamoDB {tag} requires a string"))
    };
    let strings = || -> Result<Vec<String>> {
        value
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("DynamoDB {tag} requires an array"))?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow::anyhow!("DynamoDB set requires strings"))
            })
            .collect()
    };
    Ok(match tag.as_str() {
        "S" => AttributeValue::S(string()?.into()),
        "N" => AttributeValue::N(string()?.into()),
        "B" => AttributeValue::B(Blob::new(
            base64::engine::general_purpose::STANDARD.decode(string()?)?,
        )),
        "BOOL" => AttributeValue::Bool(
            value
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("BOOL requires boolean"))?,
        ),
        "NULL" => {
            ensure!(value == &Value::Bool(true), "NULL requires true");
            AttributeValue::Null(true)
        }
        "SS" => AttributeValue::Ss(strings()?),
        "NS" => AttributeValue::Ns(strings()?),
        "BS" => AttributeValue::Bs(
            strings()?
                .iter()
                .map(|s| {
                    base64::engine::general_purpose::STANDARD
                        .decode(s)
                        .map(Blob::new)
                })
                .collect::<std::result::Result<_, _>>()?,
        ),
        "L" => AttributeValue::L(
            value
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("L requires an array"))?
                .iter()
                .map(|v| attribute(v, depth + 1))
                .collect::<Result<_>>()?,
        ),
        "M" => AttributeValue::M(
            value
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("M requires an object"))?
                .iter()
                .map(|(k, v)| Ok((k.clone(), attribute(v, depth + 1)?)))
                .collect::<Result<_>>()?,
        ),
        _ => bail!("Unknown DynamoDB attribute tag: {tag}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn types_decimal_precision_binary_and_nesting_are_preserved() {
        let value = serde_json::json!({"number":{"N":"99999999999999999999999999999999999999"},"bytes":{"B":"AP+A"},"nested":{"M":{"values":{"L":[{"NULL":true},{"BOOL":false},{"SS":["a","b"]},{"NS":["1.00","2"]},{"BS":["AA=="]}]}}}});
        let parsed = item(&value).unwrap();
        assert_eq!(
            parsed["number"].as_n().unwrap(),
            "99999999999999999999999999999999999999"
        );
        assert_eq!(parsed["bytes"].as_b().unwrap().as_ref(), [0, 255, 128]);
        for invalid in [
            serde_json::json!({"x":{"S":"x","N":"1"}}),
            serde_json::json!({"x":{"NULL":false}}),
            serde_json::json!({"x":{"B":"invalid"}}),
            serde_json::json!({"x":{"unknown":"x"}}),
        ] {
            assert!(item(&invalid).is_err());
        }
        let mut nested = serde_json::json!({"S":"end"});
        for _ in 0..34 {
            nested = serde_json::json!({"L":[nested]});
        }
        assert!(item(&serde_json::json!({"x":nested})).is_err());
    }
}
