use anyhow::{Result, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
pub(crate) enum Read {
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

impl Read {
    pub fn parse(text: &str) -> Result<Self> {
        ensure!(
            !text.is_empty() && text.len() <= onetui_core::provider::QUERY_BYTES,
            "DynamoDB query requires 1..16384 bytes"
        );
        let query: Self = serde_json::from_str(text)?;
        match &query {
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
    fn only_known_read_operations_and_options_are_accepted() {
        assert!(Read::parse(r#"{"operation":"Scan"}"#).is_ok());
        assert!(Read::parse(r#"{"operation":"Query","key_condition_expression":"pk=:p","expression_attribute_values":{":p":{"S":"x"}}}"#).is_ok());
        for text in [
            r#"{"operation":"PutItem"}"#,
            r#"{"operation":"Scan","limit":0}"#,
            r#"{"operation":"Scan","limit":101}"#,
            r#"{"operation":"Scan","TableName":"other"}"#,
            r#"{"operation":"Query"}"#,
            r#"{"operation":"GetItem","key":{}}"#,
        ] {
            assert!(Read::parse(text).is_err(), "{text}");
        }
    }
}
