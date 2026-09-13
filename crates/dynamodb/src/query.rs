use anyhow::{Result, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
pub(crate) enum Read {
    GetRecords {
        shard_id: String,
        sequence_number: String,
        #[serde(default)]
        after: bool,
        #[serde(default = "page_size")]
        limit: i32,
    },
    Scan {
        index: Option<String>,
        filter_expression: Option<String>,
        projection_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<Value>,
        #[serde(default)]
        consistent_read: bool,
        #[serde(default = "page_size")]
        limit: i32,
    },
    Query {
        index: Option<String>,
        key_condition_expression: String,
        filter_expression: Option<String>,
        projection_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<Value>,
        #[serde(default)]
        consistent_read: bool,
        #[serde(default = "forward")]
        scan_index_forward: bool,
        #[serde(default = "page_size")]
        limit: i32,
    },
    GetItem {
        key: Value,
        projection_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        #[serde(default)]
        consistent_read: bool,
    },
}

fn page_size() -> i32 {
    100
}
fn forward() -> bool {
    true
}

pub(crate) fn example(resource: &onetui_core::Resource, row: Option<&onetui_core::Row>) -> String {
    let first = row
        .and_then(|row| row.cells.first())
        .and_then(Option::as_ref)
        .and_then(onetui_core::Value::text);
    let value = if resource.id == "dynamodb.records" {
        serde_json::json!({"operation":"GetRecords", "shard_id":resource.path.get(1).map(String::as_str).unwrap_or("shard-id"), "sequence_number":first.unwrap_or("0"), "limit":100})
    } else if resource.id == "dynamodb.shards" {
        let shard: Value = first
            .and_then(|text| serde_json::from_str(text).ok())
            .unwrap_or(Value::Null);
        serde_json::json!({"operation":"GetRecords", "shard_id":shard["ShardId"].as_str().unwrap_or("shard-id"), "sequence_number":shard["SequenceNumberRange"]["StartingSequenceNumber"].as_str().unwrap_or("0"), "limit":100})
    } else if matches!(resource.id, "dynamodb.stream" | "dynamodb.stream_info") {
        serde_json::json!({"operation":"GetRecords", "shard_id":"shard-id", "sequence_number":"0", "limit":100})
    } else {
        serde_json::json!({"operation":"Scan", "limit":100})
    };
    serde_json::to_string_pretty(&value).expect("JSON query example")
}

impl Read {
    pub fn parse(text: &str) -> Result<Self> {
        ensure!(
            !text.is_empty() && text.len() <= onetui_core::provider::QUERY_BYTES,
            "DynamoDB query requires 1..16384 bytes"
        );
        let query: Self = serde_json::from_str(text)?;
        match &query {
            Self::GetRecords {
                shard_id,
                sequence_number,
                limit,
                ..
            } => {
                ensure!(
                    !shard_id.is_empty()
                        && shard_id.len() <= 240
                        && !shard_id.chars().any(char::is_control),
                    "Invalid DynamoDB Streams shard_id"
                );
                ensure!(
                    !sequence_number.is_empty()
                        && sequence_number.len() <= 40
                        && sequence_number.bytes().all(|c| c.is_ascii_digit()),
                    "DynamoDB Streams sequence_number requires 1..40 decimal digits"
                );
                ensure!(
                    (1..=100).contains(limit),
                    "DynamoDB Streams limit must be 1..100 records"
                );
            }
            Self::Scan {
                limit,
                expression_attribute_values,
                ..
            }
            | Self::Query {
                limit,
                expression_attribute_values,
                ..
            } => {
                ensure!(
                    (1..=100).contains(limit),
                    "DynamoDB limit must be 1..100 evaluated items"
                );
                if let Some(values) = expression_attribute_values {
                    crate::attributes::item(values)?;
                }
            }
            Self::GetItem { key, .. } => {
                ensure!(
                    !crate::attributes::item(key)?.is_empty(),
                    "GetItem requires a nonempty key"
                );
            }
        }
        Ok(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stream_examples_use_selected_shards_and_preserve_decimal_sequences() {
        use onetui_core::{Resource, Row};
        let row = Row {
            cells: vec![Some("000123456789012345678901234567890".into())],
            target: None,
        };
        let text = example(
            &Resource::new("dynamodb.records", vec!["arn".into(), "shard-1".into()]),
            Some(&row),
        );
        assert!(
            matches!(Read::parse(&text).unwrap(), Read::GetRecords { shard_id, sequence_number, after: false, limit: 100 } if shard_id == "shard-1" && sequence_number == "000123456789012345678901234567890")
        );
        let row = Row { cells: vec![Some(onetui_core::Value::Json(r#"{"ShardId":"shard-2","SequenceNumberRange":{"StartingSequenceNumber":"000456"}}"#.into()))], target: None };
        let text = example(
            &Resource::new("dynamodb.shards", vec!["arn".into()]),
            Some(&row),
        );
        assert!(
            matches!(Read::parse(&text).unwrap(), Read::GetRecords { shard_id, sequence_number, .. } if shard_id == "shard-2" && sequence_number == "000456")
        );
        assert!(matches!(
            Read::parse(&example(
                &Resource::new("dynamodb.table", vec!["demo".into()]),
                None
            ))
            .unwrap(),
            Read::Scan { .. }
        ));
    }
    #[test]
    fn only_known_read_operations_and_options_are_accepted() {
        assert!(Read::parse(r#"{"operation":"Scan"}"#).is_ok());
        assert!(
            Read::parse(
                r#"{"operation":"GetRecords","shard_id":"shard-1","sequence_number":"000123"}"#
            )
            .is_ok()
        );
        assert!(Read::parse(r#"{"operation":"Query","key_condition_expression":"pk=:p","expression_attribute_values":{":p":{"S":"x"}}}"#).is_ok());
        for text in [
            r#"{"operation":"PutItem"}"#,
            r#"{"operation":"Scan","limit":0}"#,
            r#"{"operation":"Scan","limit":101}"#,
            r#"{"operation":"Scan","TableName":"other"}"#,
            r#"{"operation":"Query"}"#,
            r#"{"operation":"GetItem","key":{}}"#,
            r#"{"operation":"GetRecords","shard_id":"shard-1","sequence_number":"-1"}"#,
            r#"{"operation":"GetRecords","shard_id":"shard-1","sequence_number":"1","limit":101}"#,
            r#"{"operation":"GetRecords","shard_id":"","sequence_number":"1"}"#,
        ] {
            assert!(Read::parse(text).is_err(), "{text}");
        }
    }
}
