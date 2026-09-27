---
status: accepted
date: 2026-09-27
---

# ADR-0019: Color editor text by provider-declared syntax

## Decision

Each provider's `QueryDescriptor` declares a `Syntax` in [core](../../crates/core/src/provider.rs). The TUI colors the editor and the executed-query panel from it in [syntax.rs](../../crates/tui/src/syntax.rs). Three kinds of syntax exist:

| Syntax | Providers | Lexer |
| --- | --- | --- |
| `Sql { keywords }` | PostgreSQL SQL, CQL | `sqlparser` 0.63 tokenizer, `GenericDialect` |
| `Json` | DynamoDB operations | `jsonc-parser` 0.34 `Scanner` |
| `Verbs([(verb, Body)])` | Qdrant HTTP, Kafka, NATS, RabbitMQ | Own verb-line lexer. A `Json` body uses the JSON scanner, a `Raw` body stays plain |

Following [ADR-0005](0005-run-native-queries-through-providers.md), the provider owns its language: it declares which of the three syntaxes it uses and its extra keywords, and core stays free of lexers and Ratatui. The TUI re-lexes the whole text on every draw. Query text is capped at 16 KiB (`QUERY_BYTES`), so no incremental state is needed.

## Evidence

Four candidates were tried on the same four inputs:

- a CQL statement with a quoted name, `CONTAINS`, `USING TIMEOUT 5s` and a comment
- a half-typed SQL statement ending in an unterminated string
- a PartiQL statement with `<<1>>`, `[1, 2]`, `?` and `MISSING`
- a half-typed Qdrant filter body

| Candidate | Result | Transitive crates |
| --- | --- | --- |
| `sqlparser` tokenizer | Correct tokens for CQL and PartiQL, quoted identifiers marked by `quote_style`. An unterminated string returns an error and no token list. | 14 with defaults, `log` only with `default-features = false, features = ["std"]` |
| `jsonc-parser` `Scanner` | Exact tokens with byte ranges. It scans up to the unterminated string, then reports its position. | 1, no dependencies |
| `tree-sitter-sequel` + `tree-sitter-json` | Tolerates errors, but the output is wrong: `0` as a string, `tags CONTAINS 'a'` as one string, the unfinished string dropped, JSON values dropped | 19, plus a C build per grammar |
| `partiql-parser` 0.15.0-alpha.1 | Only `Parser::parse` is public. The lexer is private | 56, including LALRPOP |

`sqlparser` needs no patch. `Tokenizer::tokenize_with_location_into_buf` keeps every token it read before an error, which is all that coloring half-typed input needs.

## Rules the implementation keeps

- **Keywords come from a list, not from `sqlparser`.** Its keyword classification includes common column names (`NAME`, `VALUE`, `DATA`). A shared SQL list plus each provider's extras decides what is colored. CQL leaves out `KEY` and `TYPE` for the same reason.
- **Unfinished input keeps its color.** Whitespace is also a token, so an unfinished token starts where the last complete one ended. The error location can point at the end of the input instead. The rest of the text is colored as a comment (`/*`), a quoted name (`"` or `` ` ``) or a string. JSON follows the same rule from the scanner's `token_start`.
- **Payloads are bytes.** A `Raw` body is never colored, even when it looks like JSON, because the provider sends it literally.
- **The verb line matches its parser.** Leading whitespace is skipped, the verb matches only as a whole word, and an unknown verb and its body stay plain.
- **Colors reuse the palette's existing roles:** keyword `key_hint` bold, string `success`, number `warning`, comment `muted` italic, name `identifier`. All ten themes work without new tokens. The display `highlight` setting turns coloring off.

## Consequences

The TUI depends on `sqlparser` and `jsonc-parser`. Both are used as tokenizers only. Statements are never parsed on the client, as ADR-0005 requires.

PartiQL inside a DynamoDB `"statement"` JSON string stays one string color. Coloring it means running the SQL tokenizer over that string's range. This is out of scope here.

`syntax.rs` tests cover multibyte text before tokens, unterminated strings, comments and quoted names, JSON keys versus values, raw bodies, unknown and indented verbs. A render test checks the colors on screen and that the `highlight` setting turns them off. `docs/assets/demo/query.svg` shows the result.

## Alternatives

- **tree-sitter grammars.** They are built to tolerate errors, but a general SQL grammar misreads CQL. No CQL or PartiQL grammar exists, and each grammar adds a C build.
- **`partiql-parser`.** Its lexer is private and the release is alpha. `sqlparser` already tokenizes PartiQL text correctly.
- **`syntect`.** Not probed. It uses regex-based Sublime grammars with the same general-SQL gap. Its bundled grammars and theme model do not fit a palette with ten themes.
- **Hand-written SQL lexer.** Quoting rules, escapes, dollar-quoted strings and nested comments are already handled and tested in `sqlparser`. The only hand-written lexer is the verb line, which no library knows.
- **New syntax colors per theme.** More precise, but it needs ten theme edits and new snapshots. Existing roles can be replaced later without changing the provider contract.
