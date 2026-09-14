use anyhow::{Result, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
pub(crate) enum Read {
    ExecuteStatement {
        statement: String,
        parameters: Option<Vec<Value>>,
        #[serde(default)]
        consistent_read: bool,
        #[serde(default = "page_size")]
        limit: i32,
    },
    BatchExecuteStatement {
        statements: Vec<crate::partiql::Statement>,
    },
    ExecuteTransaction {
        statements: Vec<crate::partiql::Statement>,
    },
    SearchVectors {
        index: String,
        search_vector: Vec<serde_json::Number>,
        top_k: i32,
        search_condition_expression: Option<String>,
        projection_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<Value>,
    },
    BatchGetItem {
        keys: Vec<Value>,
        projection_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        #[serde(default)]
        consistent_read: bool,
    },
    TransactGetItems {
        items: Vec<Get>,
    },
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

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Get {
    pub key: Value,
    pub projection_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
}

pub(crate) fn validate_keys(keys: &[Value]) -> Result<()> {
    ensure!(
        (1..=100).contains(&keys.len()),
        "DynamoDB requires 1..100 keys"
    );
    for key in keys {
        ensure!(
            !crate::attributes::item(key)?.is_empty(),
            "DynamoDB keys must be nonempty attribute maps"
        );
    }
    Ok(())
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
    } else if matches!(resource.id, "dynamodb.shards" | "dynamodb.shard_details") {
        let start = row
            .and_then(|row| row.cells.get(2))
            .and_then(Option::as_ref)
            .and_then(onetui_core::Value::text);
        serde_json::json!({"operation":"GetRecords", "shard_id":first.unwrap_or("shard-id"), "sequence_number":start.unwrap_or("0"), "limit":100})
    } else if matches!(resource.id, "dynamodb.stream" | "dynamodb.stream_info") {
        serde_json::json!({"operation":"GetRecords", "shard_id":"shard-id", "sequence_number":"0", "limit":100})
    } else {
        serde_json::json!({"operation":"Scan", "limit":100})
    };
    serde_json::to_string_pretty(&value).expect("JSON query example")
}

impl Read {
    pub(crate) fn validate_scope(&self, table: &str) -> Result<()> {
        let check = |statement: &str| -> Result<()> {
            ensure!(
                crate::partiql::source(statement)? == table,
                "PartiQL must read the selected table"
            );
            Ok(())
        };
        match self {
            Self::ExecuteStatement { statement, .. } => check(statement)?,
            Self::BatchExecuteStatement { statements }
            | Self::ExecuteTransaction { statements } => {
                for statement in statements {
                    check(&statement.statement)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub fn parse(text: &str) -> Result<Self> {
        ensure!(
            !text.is_empty() && text.len() <= onetui_core::provider::QUERY_BYTES,
            "DynamoDB query requires 1..16384 bytes"
        );
        let query: Self = serde_json::from_str(text)?;
        match &query {
            Self::ExecuteStatement {
                statement,
                parameters,
                limit,
                ..
            } => {
                crate::partiql::source(statement)?;
                crate::partiql::parameters(parameters.as_deref())?;
                ensure!(
                    (1..=100).contains(limit),
                    "ExecuteStatement limit must be 1..100 evaluated items"
                );
            }
            Self::BatchExecuteStatement { statements }
            | Self::ExecuteTransaction { statements } => {
                let transaction = matches!(&query, Self::ExecuteTransaction { .. });
                let max = if transaction { 100 } else { 25 };
                ensure!(
                    (1..=max).contains(&statements.len()),
                    "PartiQL requires 1..{max} statements"
                );
                for statement in statements {
                    crate::partiql::source(&statement.statement)?;
                    crate::partiql::parameters(statement.parameters.as_deref())?;
                    ensure!(
                        !transaction || statement.consistent_read.is_none(),
                        "ExecuteTransaction does not accept consistent_read"
                    );
                }
            }
            Self::SearchVectors {
                index,
                search_vector,
                top_k,
                expression_attribute_values,
                ..
            } => {
                ensure!(
                    (3..=255).contains(&index.len())
                        && index
                            .bytes()
                            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c)),
                    "Invalid DynamoDB vector index name"
                );
                ensure!(
                    (1..=4096).contains(&search_vector.len()),
                    "SearchVectors requires 1..4096 dimensions"
                );
                ensure!(
                    search_vector
                        .iter()
                        .all(|n| n.to_string().parse::<f32>().is_ok_and(f32::is_finite)),
                    "SearchVectors dimensions must fit finite 32-bit floats"
                );
                ensure!(
                    (1..=100).contains(top_k),
                    "SearchVectors top_k must be 1..100"
                );
                if let Some(values) = expression_attribute_values {
                    crate::attributes::item(values)?;
                }
            }
            Self::BatchGetItem { keys, .. } => validate_keys(keys)?,
            Self::TransactGetItems { items } => {
                ensure!(
                    (1..=100).contains(&items.len()),
                    "TransactGetItems requires 1..100 items"
                );
                for item in items {
                    validate_keys(std::slice::from_ref(&item.key))?;
                }
            }
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
    fn vector_inputs_obey_native_dimensions_and_bounds() {
        for (dimensions, valid) in [(0, false), (1, true), (4096, true), (4097, false)] {
            let query = serde_json::json!({"operation":"SearchVectors","index":"embedding","search_vector":vec![0;dimensions],"top_k":100});
            assert_eq!(
                Read::parse(&query.to_string()).is_ok(),
                valid,
                "{dimensions}"
            );
        }
        let valid = serde_json::json!({"operation":"SearchVectors","index":"embedding","search_vector":[0.1],"top_k":1});
        for (field, value) in [
            ("index", serde_json::json!("ab")),
            ("index", serde_json::json!("not an index")),
            ("index", serde_json::json!("x".repeat(256))),
            ("top_k", serde_json::json!(0)),
            ("top_k", serde_json::json!(101)),
            ("search_vector", serde_json::json!([1e39])),
            ("search_vector", serde_json::json!(["0.1"])),
            ("search_vector", serde_json::json!([null])),
            (
                "expression_attribute_values",
                serde_json::json!({":v":{"B":"invalid"}}),
            ),
            ("table", serde_json::json!("other")),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            assert!(Read::parse(&invalid.to_string()).is_err(), "{invalid}");
        }
    }

    #[test]
    fn multi_item_reads_reject_invalid_counts_keys_and_table_overrides() {
        let key = serde_json::json!({"pk":{"S":"demo"}});
        for operation in ["BatchGetItem", "TransactGetItems"] {
            let field = if operation == "BatchGetItem" {
                "keys"
            } else {
                "items"
            };
            let item = if operation == "BatchGetItem" {
                key.clone()
            } else {
                serde_json::json!({"key":key})
            };
            for count in [0, 1, 100, 101] {
                let text =
                    serde_json::json!({"operation":operation,field:vec![item.clone();count]})
                        .to_string();
                assert_eq!(
                    Read::parse(&text).is_ok(),
                    (1..=100).contains(&count),
                    "{operation} {count}"
                );
            }
        }
        for text in [
            r#"{"operation":"BatchGetItem","keys":[{}]}"#,
            r#"{"operation":"BatchGetItem","keys":[{"pk":{"B":"invalid"}}]}"#,
            r#"{"operation":"BatchGetItem","keys":[{"pk":{"S":"x"}}],"table":"other"}"#,
            r#"{"operation":"TransactGetItems","items":[{"key":{}}]}"#,
            r#"{"operation":"TransactGetItems","items":[{"key":{"pk":{"S":"x"}},"table":"other"}]}"#,
            r#"{"operation":"TransactGetItems","items":[{"Put":{"Item":{"pk":{"S":"x"}}}}]}"#,
        ] {
            assert!(Read::parse(text).is_err(), "{text}");
        }
    }

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
        let row = Row {
            cells: vec![Some("shard-2".into()), None, Some("000456".into()), None],
            target: None,
        };
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
