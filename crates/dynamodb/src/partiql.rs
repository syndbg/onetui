use anyhow::{Result, anyhow, ensure};
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
    let (page, _) = crate::browse::items(body)?;
    Ok((page, next))
}

enum Token<'a> {
    Word(&'a str),
    Identifier(String),
    Symbol(u8),
    Literal,
}

impl Token<'_> {
    fn keyword(&self, word: &str) -> bool {
        matches!(self, Self::Word(value) if value.eq_ignore_ascii_case(word))
    }
    fn name(&self) -> Option<&str> {
        match self {
            Self::Word(value) => Some(value),
            Self::Identifier(value) => Some(value),
            _ => None,
        }
    }
}

// This checks operation and table scope, not PartiQL expressions. The service
// validates the remaining syntax; DynamoDB SELECT has no user-defined functions.
pub(crate) fn source(statement: &str) -> Result<String> {
    ensure!(
        (1..=8192).contains(&statement.len()),
        "PartiQL statements require 1..8192 bytes"
    );
    ensure!(
        !statement
            .chars()
            .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t')),
        "PartiQL statement contains a control character"
    );
    let bytes = statement.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i += 2;
                while i < bytes.len() && !matches!(bytes[i], b'\n' | b'\r') {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let end = statement[i + 2..]
                    .find("*/")
                    .ok_or_else(|| anyhow!("Unterminated PartiQL comment"))?;
                ensure!(
                    !statement[i + 2..i + 2 + end].contains("/*"),
                    "Nested PartiQL comments are not accepted"
                );
                i += end + 4;
            }
            quote @ (b'\'' | b'"') => {
                i += 1;
                let start = i;
                loop {
                    ensure!(i < bytes.len(), "Unterminated PartiQL quoted value");
                    ensure!(
                        bytes[i] != b'\\' || bytes.get(i + 1) != Some(&quote),
                        "Use doubled quotes or typed parameters instead of backslash-escaped quotes"
                    );
                    if bytes[i] == quote {
                        if bytes.get(i + 1) == Some(&quote) {
                            i += 2;
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                tokens.push(if quote == b'"' {
                    Token::Identifier(statement[start..i].replace("\"\"", "\""))
                } else {
                    Token::Literal
                });
                i += 1;
            }
            b'`' => anyhow::bail!("Use typed PartiQL parameters instead of backtick literals"),
            c if c.is_ascii_alphanumeric() || c == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                tokens.push(Token::Word(&statement[start..i]));
            }
            c => {
                tokens.push(Token::Symbol(c));
                i += 1;
            }
        }
    }
    ensure!(
        tokens.first().is_some_and(|t| t.keyword("SELECT")),
        "Only PartiQL SELECT statements are allowed"
    );
    let mut from = None;
    for (i, token) in tokens.iter().enumerate().skip(1) {
        ensure!(
            ![
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "UPSERT",
                "REPLACE",
                "CREATE",
                "DROP",
                "ALTER",
                "INTO",
                "JOIN",
                "UNION",
                "INTERSECT",
                "EXCEPT",
                "EXEC",
                "EXECUTE"
            ]
            .iter()
            .any(|word| token.keyword(word)),
            "PartiQL requires one read-only SELECT from the selected table"
        );
        ensure!(
            !matches!(token, Token::Symbol(b';')) || i == tokens.len() - 1,
            "PartiQL requires one statement"
        );
        if token.keyword("FROM") {
            ensure!(
                from.replace(i).is_none(),
                "PartiQL requires one table source"
            );
        }
    }
    let from = from.ok_or_else(|| anyhow!("PartiQL SELECT requires FROM"))?;
    let table = tokens
        .get(from + 1)
        .and_then(Token::name)
        .ok_or_else(|| anyhow!("PartiQL FROM requires a table name"))?;
    let mut next = from + 2;
    if matches!(tokens.get(next), Some(Token::Symbol(b'.'))) {
        ensure!(
            tokens.get(next + 1).and_then(Token::name).is_some(),
            "PartiQL index name is missing"
        );
        next += 2;
    }
    ensure!(
        tokens.get(next).is_none_or(|t| t.keyword("WHERE")
            || t.keyword("ORDER")
            || matches!(t, Token::Symbol(b';'))),
        "PartiQL FROM must name only the selected table and optional index"
    );
    Ok(table.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_guard_tracks_quotes_comments_and_table_scope() {
        for statement in [
            "SELECT * FROM demo",
            "-- before\n select pk, sk FROM \"demo\".\"by_status\" WHERE pk=? ORDER BY sk DESC; -- after",
            "/* before */ SELECT \"delete\", {'from':'update;'} FROM demo WHERE pk='it''s София'",
            "SELECT size(payload) FROM demo WHERE pk IN ['a','b']",
        ] {
            assert_eq!(source(statement).unwrap(), "demo", "{statement}");
        }
        for statement in [
            "DELETE FROM demo",
            "UPDATE demo SET x=1",
            "INSERT INTO demo VALUE {}",
            "SELECT * FROM demo; DELETE FROM demo",
            "SELECT * INTO other FROM demo",
            "SELECT * FROM demo, other",
            "SELECT * FROM other JOIN demo ON true",
            "SELECT * FROM demo UNION SELECT * FROM other",
            "SELECT * FROM (SELECT * FROM demo)",
            "SELECT * FROM demo WHERE pk='unfinished",
            "/* unfinished SELECT * FROM demo",
            "/* outer /* inner */ SELECT ' */ DELETE FROM other; -- ' FROM demo",
            "SELECT 'backslash\\' FROM demo WHERE x='' FROM other'",
            "SELECT `value` FROM demo",
            "SELECT * FROM demo\0",
        ] {
            assert!(source(statement).is_err(), "{statement}");
        }
        let query = crate::query::Read::parse(
            r#"{"operation":"ExecuteStatement","statement":"SELECT * FROM other"}"#,
        )
        .unwrap();
        assert!(query.validate_scope("demo").is_err());
    }

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
            json!({"operation":"ExecuteStatement","statement":format!("SELECT * FROM demo --{}", "x".repeat(8192))}),
            json!({"operation":"ExecuteTransaction","statements":[{"statement":"SELECT * FROM demo WHERE pk=?","consistent_read":false}]}),
        ] {
            assert!(Read::parse(&query.to_string()).is_err(), "{query}");
        }
    }
}
