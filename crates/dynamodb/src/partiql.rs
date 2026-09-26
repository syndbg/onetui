use anyhow::{Result, ensure};
use aws_sdk_dynamodb::types::AttributeValue;
use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Statement {
    pub statement: String,
    pub parameters: Option<Vec<Value>>,
    pub consistent_read: Option<bool>,
}

pub(crate) fn parameters(values: Option<&[Value]>) -> Result<Option<Vec<AttributeValue>>> {
    values
        .map(|values| {
            ensure!(
                !values.is_empty(),
                "PartiQL parameters must be nonempty when supplied"
            );
            values
                .iter()
                .map(|v| crate::attributes::attribute(v, 0))
                .collect()
        })
        .transpose()
}

pub(crate) fn page(body: Value, limit: usize) -> Result<(onetui_core::Page, Option<Value>)> {
    if let Some(items) = body.get("Items") {
        ensure!(
            items.as_array().is_some_and(|items| items.len() <= limit),
            "PartiQL response exceeds the requested item limit; narrow the predicate (DynamoDB Local ignores Limit)"
        );
    }
    let next = body.get("NextToken").filter(|v| !v.is_null()).cloned();
    if let Some(token) = &next {
        ensure!(
            token
                .as_str()
                .is_some_and(|s| !s.is_empty() && s.len() <= 32768),
            "Invalid PartiQL NextToken"
        );
    }
    let last_key = body
        .get("LastEvaluatedKey")
        .filter(|v| !v.is_null() && v.as_object().is_none_or(|v| !v.is_empty()));
    ensure!(
        next.is_some() || last_key.is_none(),
        "PartiQL returned LastEvaluatedKey without NextToken; cannot resume this response: {}",
        last_key.unwrap_or(&Value::Null)
    );
    if body.get("Items").is_none() {
        let (mut page, _) = crate::browse::metadata("dynamodb.query", body)?;
        page.notice = "DynamoDB statement completed; native response shown".into();
        return Ok((page, next));
    }
    let details = serde_json::json!({"Count":body.get("Count"),"ScannedCount":body.get("ScannedCount"),"ConsumedCapacity":body.get("ConsumedCapacity")});
    let (mut page, _) = crate::browse::items(body)?;
    page.notice = format!("{details} | DynamoDB statement completed");
    Ok((page, next))
}

#[cfg(test)]
mod tests {
    #[test]
    fn statements_parameters_and_operation_limits_are_checked_offline() {
        use crate::query::Read;
        use serde_json::json;
        for (operation, field, max) in [
            ("BatchExecuteStatement", "statements", 25),
            ("ExecuteTransaction", "statements", 100),
        ] {
            for count in [0, 1, max, max + 1] {
                let text = json!({"operation":operation,field:vec![json!({"statement":"SELECT * FROM demo WHERE pk=?","parameters":[{"S":"a"}]} );count]}).to_string();
                assert_eq!(
                    Read::parse(&text).is_ok(),
                    (1..=max).contains(&count),
                    "{operation} {count}"
                );
            }
        }
        for query in [
            json!({"operation":"ExecuteStatement","statement":"SELECT * FROM demo","limit":0}),
            json!({"operation":"ExecuteStatement","statement":"SELECT * FROM demo","limit":101}),
            json!({"operation":"ExecuteStatement","statement":"SELECT * FROM demo WHERE pk=?","parameters":[]}),
            json!({"operation":"ExecuteStatement","statement":"SELECT * FROM demo WHERE pk=?","parameters":[{"B":"invalid"}]}),
            json!({"operation":"ExecuteTransaction","statements":[{"statement":"SELECT * FROM demo WHERE pk=?","consistent_read":false}]}),
        ] {
            assert!(Read::parse(&query.to_string()).is_err(), "{query}");
        }
        for statement in [
            "INSERT INTO demo VALUE {'pk':'a'}",
            "DELETE FROM other WHERE pk='a'",
            "server parses this",
        ] {
            let query = json!({"operation":"ExecuteStatement","statement":statement});
            assert!(Read::parse(&query.to_string()).is_ok());
        }
    }
}
