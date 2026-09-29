use std::ops::Range;

use onetui_core::provider::{Body, Syntax};
use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Location, Token, Tokenizer, Whitespace};

/// What a byte of editor text is, for coloring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Plain,
    Keyword,
    String,
    Number,
    Comment,
    /// A quoted identifier, a JSON key or a header name.
    Name,
}

/// Words every SQL-like language shares. `sqlparser` classifies far more words as
/// keywords (`NAME`, `VALUE`, `DATA`), which would color ordinary column names, so it
/// only splits the text and this list decides what reads as a keyword.
const SQL_KEYWORDS: &[&str] = &[
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "AS",
    "ASC",
    "BEGIN",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "COMMIT",
    "CREATE",
    "CROSS",
    "DEFAULT",
    "DELETE",
    "DESC",
    "DISTINCT",
    "DROP",
    "ELSE",
    "END",
    "EXISTS",
    "EXPLAIN",
    "FALSE",
    "FETCH",
    "FOR",
    "FROM",
    "FULL",
    "FUNCTION",
    "GRANT",
    "GROUP",
    "HAVING",
    "IF",
    "ILIKE",
    "IN",
    "INDEX",
    "INNER",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "LEFT",
    "LIKE",
    "LIMIT",
    "MISSING",
    "NOT",
    "NULL",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "PRIMARY",
    "RETURNING",
    "REVOKE",
    "RIGHT",
    "ROLLBACK",
    "SCHEMA",
    "SELECT",
    "SET",
    "TABLE",
    "THEN",
    "TO",
    "TRUE",
    "TRUNCATE",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "USING",
    "VALUES",
    "VIEW",
    "WHEN",
    "WHERE",
    "WITH",
];

/// One kind per byte of `text`. The text is re-lexed on every draw. It is bounded by the
/// query size limit, so this costs microseconds and needs no incremental state.
pub fn kinds(syntax: Syntax, text: &str) -> Vec<Kind> {
    let mut kinds = vec![Kind::Plain; text.len()];
    match syntax {
        Syntax::Sql { keywords } => sql(text, 0, keywords, &mut kinds),
        Syntax::Json => json(text, 0, &mut kinds),
        Syntax::Verbs(verbs) => verb_statement(text, verbs, &mut kinds),
    }
    kinds
}

/// A verb line, header lines up to the first blank line, then the verb's body.
fn verb_statement(text: &str, verbs: &[(&str, Body)], kinds: &mut [Kind]) {
    let first = text.find('\n').unwrap_or(text.len());
    // The parsers split the verb line on whitespace, so leading spaces are allowed.
    let verb_start = text[..first]
        .find(|c: char| !c.is_whitespace())
        .unwrap_or(first);
    let verb_end = text[verb_start..first]
        .find(char::is_whitespace)
        .map_or(first, |i| verb_start + i);
    let body = verbs
        .iter()
        .find(|(verb, _)| *verb == &text[verb_start..verb_end])
        .map_or(Body::Raw, |&(_, body)| {
            kinds[verb_start..verb_end].fill(Kind::Keyword);
            body
        });
    let mut offset = first;
    while offset < text.len() {
        // `offset` sits on the newline that ends the previous line.
        let start = offset + 1;
        let end = text[start..].find('\n').map_or(text.len(), |i| start + i);
        let line = &text[start..end];
        if line.is_empty() {
            if body == Body::Json && end < text.len() {
                json(&text[end + 1..], end + 1, kinds);
            }
            return;
        }
        if let Some(colon) = line.find(':') {
            kinds[start..start + colon].fill(Kind::Name);
        }
        offset = end;
    }
}

/// SQL, CQL and `PartiQL` through `sqlparser`'s tokenizer. It stops at an unterminated
/// string or comment, which is the normal state while typing one: the tokens before it
/// are kept, and everything after the error reads as the open string or comment.
fn sql(text: &str, base: usize, keywords: &[&str], kinds: &mut [Kind]) {
    let lines = LineStarts::new(text);
    let mut tokens = Vec::new();
    let result =
        Tokenizer::new(&GenericDialect {}, text).tokenize_with_location_into_buf(&mut tokens);
    for token in &tokens {
        let kind = match &token.token {
            Token::Word(word) if word.quote_style.is_some() => Kind::Name,
            Token::Word(word) => {
                let upper = word.value.to_ascii_uppercase();
                if SQL_KEYWORDS.contains(&upper.as_str()) || keywords.contains(&upper.as_str()) {
                    Kind::Keyword
                } else {
                    continue;
                }
            }
            Token::Number(..) => Kind::Number,
            Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::NationalStringLiteral(_)
            | Token::EscapedStringLiteral(_)
            | Token::UnicodeStringLiteral(_)
            | Token::HexStringLiteral(_)
            | Token::DollarQuotedString(_) => Kind::String,
            Token::Whitespace(
                Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_),
            ) => Kind::Comment,
            _ => continue,
        };
        let range = lines.byte(token.span.start)..lines.byte(token.span.end);
        fill(kinds, base, range, kind);
    }
    if result.is_err() {
        // Whitespace is a token too, so the unfinished token starts where the last
        // complete one ended. The error's own location can point at the end of input.
        let start = tokens.last().map_or(0, |token| lines.byte(token.span.end));
        let rest = &text[start..];
        let kind = if rest.starts_with("/*") {
            Kind::Comment
        } else if rest.starts_with(['"', '`']) {
            Kind::Name
        } else {
            Kind::String
        };
        fill(kinds, base, start..text.len(), kind);
    }
}

/// JSON through `jsonc-parser`'s scanner. A string followed by `:` is a key. As with
/// SQL, an unterminated string colors the rest of the text as that string.
fn json(text: &str, base: usize, kinds: &mut [Kind]) {
    use jsonc_parser::tokens::Token as Json;
    let mut scanner = jsonc_parser::Scanner::new(text, &Default::default());
    let mut tokens: Vec<(Range<usize>, Kind, bool)> = Vec::new();
    loop {
        match scanner.scan() {
            Ok(Some(token)) => {
                let range = scanner.token_start()..scanner.token_end();
                let entry = match token {
                    Json::String(_) => Some((Kind::String, false)),
                    Json::Number(_) => Some((Kind::Number, false)),
                    Json::Boolean(_) | Json::Null => Some((Kind::Keyword, false)),
                    Json::CommentLine(_) | Json::CommentBlock(_) => Some((Kind::Comment, false)),
                    // A colon promotes the string before it to a key.
                    Json::Colon => Some((Kind::Plain, true)),
                    _ => None,
                };
                if let Some((kind, colon)) = entry {
                    if colon {
                        if let Some(last) = tokens.last_mut()
                            && last.1 == Kind::String
                        {
                            last.1 = Kind::Name;
                        }
                    } else {
                        tokens.push((range, kind, false));
                    }
                }
            }
            Ok(None) => break,
            Err(_) => {
                let start = scanner.token_start();
                let rest = &text[start..];
                if rest.starts_with('"') {
                    tokens.push((start..text.len(), Kind::String, false));
                } else if rest.starts_with("/*") {
                    tokens.push((start..text.len(), Kind::Comment, false));
                }
                break;
            }
        }
    }
    for (range, kind, _) in tokens {
        fill(kinds, base, range, kind);
    }
}

fn fill(kinds: &mut [Kind], base: usize, range: Range<usize>, kind: Kind) {
    let end = (base + range.end).min(kinds.len());
    if base + range.start < end {
        kinds[base + range.start..end].fill(kind);
    }
}

/// Converts `sqlparser`'s 1-based line and character column into byte offsets.
struct LineStarts<'a> {
    text: &'a str,
    starts: Vec<usize>,
}

impl<'a> LineStarts<'a> {
    fn new(text: &'a str) -> Self {
        let starts = std::iter::once(0)
            .chain(text.match_indices('\n').map(|(i, _)| i + 1))
            .collect();
        Self { text, starts }
    }

    fn byte(&self, location: Location) -> usize {
        let Some(&start) = usize::try_from(location.line)
            .ok()
            .and_then(|line| line.checked_sub(1))
            .and_then(|line| self.starts.get(line))
        else {
            return self.text.len();
        };
        let column = usize::try_from(location.column)
            .unwrap_or(usize::MAX)
            .saturating_sub(1);
        self.text[start..]
            .char_indices()
            .nth(column)
            .map_or(self.text.len(), |(i, _)| start + i)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The text of every span of `kind`, in order.
    fn spans(syntax: Syntax, text: &str, kind: Kind) -> Vec<String> {
        let kinds = kinds(syntax, text);
        let mut spans = Vec::new();
        let mut current = String::new();
        for (i, c) in text.char_indices() {
            if kinds[i] == kind {
                current.push(c);
            } else if !current.is_empty() {
                spans.push(std::mem::take(&mut current));
            }
        }
        if !current.is_empty() {
            spans.push(current);
        }
        spans
    }

    const SQL: Syntax = Syntax::Sql {
        keywords: &["ALLOW", "FILTERING"],
    };

    #[test]
    fn sql_colors_keywords_strings_numbers_comments_and_quoted_names() {
        let text =
            "select note FROM ks.\"Mixed\" WHERE n = 42 AND s = 'a b' ALLOW FILTERING -- why";
        assert_eq!(
            spans(SQL, text, Kind::Keyword),
            ["select", "FROM", "WHERE", "AND", "ALLOW", "FILTERING"]
        );
        // Ordinary names stay plain even when sqlparser knows them as keywords.
        assert!(!spans(SQL, text, Kind::Keyword).contains(&"note".into()));
        assert_eq!(spans(SQL, text, Kind::Name), ["\"Mixed\""]);
        assert_eq!(spans(SQL, text, Kind::Number), ["42"]);
        assert_eq!(spans(SQL, text, Kind::String), ["'a b'"]);
        assert_eq!(spans(SQL, text, Kind::Comment), ["-- why"]);
        // A provider keyword is not a keyword for another language.
        assert!(
            spans(Syntax::Sql { keywords: &[] }, text, Kind::Keyword)
                .iter()
                .all(|word| word != "ALLOW")
        );
    }

    #[test]
    fn sql_keeps_tokens_before_an_unterminated_string_or_comment() {
        let text = "SELECT * FROM t WHERE name = 'still typ";
        assert_eq!(spans(SQL, text, Kind::Keyword), ["SELECT", "FROM", "WHERE"]);
        assert_eq!(spans(SQL, text, Kind::String), ["'still typ"]);
        let text = "SELECT 1 /* open";
        assert_eq!(spans(SQL, text, Kind::Number), ["1"]);
        assert_eq!(spans(SQL, text, Kind::Comment), ["/* open"]);
    }

    #[test]
    fn sql_offsets_survive_multibyte_text_and_new_lines() {
        // Columns are characters, offsets are bytes: text after 'София' must line up.
        let text = "SELECT 'София' AS name\nFROM t WHERE x = 7";
        assert_eq!(spans(SQL, text, Kind::String), ["'София'"]);
        assert_eq!(
            spans(SQL, text, Kind::Keyword),
            ["SELECT", "AS", "FROM", "WHERE"]
        );
        assert_eq!(spans(SQL, text, Kind::Number), ["7"]);
    }

    #[test]
    fn json_separates_keys_from_values_and_tolerates_open_strings() {
        let text = r#"{"limit": 10, "with_payload": true, "filter": null, "city": "Sofia"}"#;
        assert_eq!(
            spans(Syntax::Json, text, Kind::Name),
            ["\"limit\"", "\"with_payload\"", "\"filter\"", "\"city\""]
        );
        assert_eq!(spans(Syntax::Json, text, Kind::String), ["\"Sofia\""]);
        assert_eq!(spans(Syntax::Json, text, Kind::Number), ["10"]);
        assert_eq!(spans(Syntax::Json, text, Kind::Keyword), ["true", "null"]);
        let text = r#"{"match": {"value": "Sof"#;
        assert_eq!(
            spans(Syntax::Json, text, Kind::Name),
            ["\"match\"", "\"value\""]
        );
        assert_eq!(spans(Syntax::Json, text, Kind::String), ["\"Sof"]);
    }

    const VERBS: Syntax = Syntax::Verbs(&[("DECLARE", Body::Json), ("PUBLISH", Body::Raw)]);

    #[test]
    fn verb_lines_color_the_verb_headers_and_json_bodies() {
        let text = "DECLARE queue / demo\n\n{\"durable\": true}";
        assert_eq!(spans(VERBS, text, Kind::Keyword), ["DECLARE", "true"]);
        assert_eq!(spans(VERBS, text, Kind::Name), ["\"durable\""]);
        // Arguments stay plain. They are names the server resolves.
        assert!(
            spans(VERBS, text, Kind::Plain)
                .iter()
                .any(|s| s.contains("queue / demo"))
        );

        // A raw body is bytes, never colored, even when it looks like JSON.
        let text = "PUBLISH / amq.default demo\nOnetui-Encoding: base64\n\n{\"a\": 1}";
        assert_eq!(spans(VERBS, text, Kind::Keyword), ["PUBLISH"]);
        assert_eq!(spans(VERBS, text, Kind::Name), ["Onetui-Encoding"]);
        assert!(spans(VERBS, text, Kind::Number).is_empty());

        // An unknown verb is plain and its body raw.
        let text = "FLUSH / demo\n\n{\"a\": 1}";
        assert!(spans(VERBS, text, Kind::Keyword).is_empty());
        assert!(spans(VERBS, text, Kind::Name).is_empty());
        // A verb only matches as a whole word.
        assert!(spans(VERBS, "DECLARED x", Kind::Keyword).is_empty());
        // The parsers accept leading spaces on the verb line, so the highlighter does too.
        assert_eq!(
            spans(VERBS, "  DECLARE queue / demo", Kind::Keyword),
            ["DECLARE"]
        );
    }

    #[test]
    fn unfinished_quoted_names_and_json_comments_keep_their_kind() {
        // An unterminated quoted identifier is a name, not a string.
        let text = "SELECT * FROM \"Mixed";
        assert_eq!(spans(SQL, text, Kind::Name), ["\"Mixed"]);
        assert!(spans(SQL, text, Kind::String).is_empty());
        let text = "{\"a\": 1 /* open";
        assert_eq!(spans(Syntax::Json, text, Kind::Number), ["1"]);
        assert_eq!(spans(Syntax::Json, text, Kind::Comment), ["/* open"]);
    }
}
