---
status: accepted
date: 2026-09-09
---

# ADR-0004: Preserve values and select display formats

The Auto byte-display rule was later superseded. The remaining decisions below still apply. The format table records the original choice.

## Decision

Keep retained values separate from their terminal representation. Let users select text, JSON, hexadecimal or binary-digit views through built-in enum dispatch. Pretty printing, data highlighting and word wrapping are independent settings. Word wrapping defaults to on throughout the app.

See [display usage](../ui.md#value-display-controls) for current controls and limits.

## Current behavior and reference

At decision time, OneTUI's [Row](../../crates/core/src/lib.rs) stored `Vec<Option<String>>`, escaped by connectors before caching. [Field detail](../../crates/tui/src/app.rs) split this text into 4,096-character chunks. That representation could not retain arbitrary bytes or distinguish original text from escape sequences introduced for display. The value contract now retains text, serialized JSON or bytes; TUI prepares separate safe projections.

The emoji in the reported payload is literal [fixture data](../../crates/qdrant/examples/seed_demo.rs), used alongside several writing systems to exercise Unicode rendering. It is not an icon added by the renderer. Non-ASCII text can be valid UTF-8; arbitrary binary data need not be. Neither validity nor an emoji establishes the value's format.

DataTUI was inspected locally at `~/work/datatui`, branch `develop`, commit `1b3a879c345eae002e53e518fc1ab58e29be12f7`:

- Its [selected-cell viewer](https://github.com/forensicmatt/datatui/blob/1b3a879c345eae002e53e518fc1ab58e29be12f7/src/components/datatable_container.rs#L2463) sits above the table. Optional auto-expansion enables Ratatui wrapping and bounds its height to leave room for the table.
- Its [value highlighting](https://github.com/forensicmatt/datatui/blob/1b3a879c345eae002e53e518fc1ab58e29be12f7/src/components/datatable_container.rs#L989) uses search matches and regex style rules. This is distinct from JSON syntax highlighting.
- Its [value conversion](https://github.com/forensicmatt/datatui/blob/1b3a879c345eae002e53e518fc1ab58e29be12f7/src/components/datatable.rs#L255) represents Polars binary values as debug-formatted strings inside JSON values. This inspected path is not a byte-preserving hex/binary viewer.

Reuse the separation between table navigation, value inspection and presentation controls. Keep OneTUI's existing panels and bounded connector reads; do not adopt DataTUI's DataFrame/query engine or its text conversion as the byte contract. These findings are source inspection, not a live DataTUI validation.

## Value and package boundaries

Core owns the value contract and serializable display options without Ratatui or database SDK dependencies. A value distinguishes null, text and bytes. Empty text, empty bytes and literal `NULL` remain distinct. Text carries its provenance: server-provided text or an application serialization of a structured SDK value. Connectors retain unsanitized values within the existing byte budgets; TUI alone creates terminal-safe display text. Metadata, errors and identifiers still require terminal sanitization.

PostgreSQL `bytea` must supply its binary content, not the bytes of a `\x...` text preview. Other PostgreSQL types can retain server-text output, explicitly labeled as such. Qdrant payload JSON is serialized from a structured protobuf/SDK response; its bytes are serialized JSON, not the original submitted document, storage representation or protobuf wire bytes. A display formatter cannot recover information already normalized by the server or SDK.

TUI owns formatting, display caches, viewport state and runtime selection. Theme supplies semantic colors. Keep these responsibilities in the existing packages; a separate formatting crate is unnecessary for one consumer. Selection follows [ADR-0002](0002-use-static-enum-dispatch-for-built-in-providers.md): built-in variants and exhaustive matches, without runtime plugins, trait objects or dispatch-related boxing.

The selector is independent of a datasource:

```rust
enum ValueFormat {
    Auto,
    Text,
    Json,
    Hex,
    Binary,
}
```

Adding a format means adding its enum variant, formatter, descriptor and package-owned tests. The descriptor supplies its name, purpose and input requirements to the picker, help and offline catalog. SQL types and resource names do not select renderer branches.

## Formats and fidelity

| Format | Behavior |
| --- | --- |
| `auto` | Use JSON for a declared JSON value or a complete valid JSON object/array in text; otherwise text. Opaque bytes default to hex even if they happen to be valid UTF-8. |
| `text` | Display text with terminal-safe escaping. For bytes, require strict UTF-8 decoding; invalid input reports that text is unavailable and offers hex/binary. Never replace invalid bytes with `�`, drop them or silently change encoding. |
| `json` | Require complete valid JSON, including scalars when explicitly selected. Invalid or incomplete input reports the problem without changing the retained value. Do not guess JSON from an opening brace alone. |
| `hex` | Show each byte as two hexadecimal digits, with byte offsets. For text, show its UTF-8 bytes and label their provenance. |
| `binary` | Show each byte as eight `0`/`1` digits, most significant bit first, with byte offsets. This is a byte view, not an integer conversion. |

For example, bytes `00 ff 41` display as `00000000 11111111 01000001` in binary mode. Their text view is unavailable because `ff` is not valid UTF-8. Text `41` has bytes `34 31`; it must not silently become the single byte `41`. Null has no byte representation.

Record tables use compact JSON so their bounded previews show values rather than indentation. Row/detail views honor pretty printing: two-space indentation when on, retained input whitespace when off, subject to terminal-control escaping. Both forms preserve key order, duplicate keys, number lexemes and string escapes from the retained JSON text. A `serde_json::Value` parse-and-reserialize round trip is not sufficient for that contract. Pretty printing does not recursively parse strings containing JSON, decode base64 or reinterpret arbitrary text. A format without a pretty-print operation shows that the setting does not apply.

Highlighting adds styles, never content. JSON syntax colors, byte grouping and any data match highlights obey the highlighting switch; selection, focus, errors and context key hints remain visible when it is off. Reuse semantic theme roles and Ratatui spans, not ANSI sequences inside the data. Serde JSON and Ratatui are already dependencies; use them before adding a generic pretty-printer or syntax-highlighting package. Any chosen formatter still has to preserve the retained representation.

Unicode display is an independent choice. `literal` preserves printable Unicode, including emoji already in the data; `escaped` renders non-ASCII code points as visible ASCII escapes such as `\u{1f30a}`. Both modes escape terminal controls and bidirectional overrides. Escape literal backslashes unambiguously in escaped text so a stored escape-like string cannot masquerade as an escaped character. JSON uses JSON-compatible `\uXXXX` escapes, including surrogate pairs where needed, rather than Rust-style escapes. Hex/binary modes always describe the retained value, never these display escapes. There is no emoji substitution, stripping or heuristic emoji detector.

Word wrapping changes visual lines only. With it on, all read-only text surfaces, including table-cell previews, details, help and status text, wrap within their allocated width. Keep row heights and layout bounded; expose content that exceeds a preview through detail. With it off, preserve logical newlines and allow horizontal scrolling in content views rather than losing access to clipped data. Wrapping uses terminal display widths and grapheme boundaries, not UTF-8 byte counts. JSON indentation and hex/binary byte groups survive wrapping. Command/filter editors remain single-line horizontally scrolling inputs; borders, column headings and compact context labels retain their layout constraints. These are structural exceptions, not independent panel-specific wrap defaults.

## Configuration and interaction

Use the optional display table in the existing configuration file:

```toml
[display]
format = "auto"
pretty_print = true
highlight = true
word_wrap = true
unicode = "literal"
```

| Setting | Type and accepted values | Default and scope |
| --- | --- | --- |
| `format` | String: `auto`, `text`, `json`, `hex`, `binary` | `auto`; startup value-view preference |
| `pretty_print` | Boolean | `true`; JSON indentation in row/detail views; record tables always use compact JSON |
| `highlight` | Boolean | `true`; data highlighting throughout the app, independent of the UI theme |
| `word_wrap` | Boolean | `true`; shared app-wide wrapping policy described above |
| `unicode` | String: `literal`, `escaped` | `literal`; Unicode rendering in data text views |

The optional table and omitted fields use these defaults. Names are exact and case-sensitive. Reject unknown fields, unknown/empty names and wrong types, including string booleans such as `"off"`, during config validation and headless checks. Values have no environment expansion, secret references or paths. Existing configurations remain valid; new display defaults change presentation only.

Keep existing file selection: explicit `--config PATH`, otherwise an absolute `$XDG_CONFIG_HOME/onetui/config.toml`, otherwise `$HOME/.config/onetui/config.toml`. Relative explicit paths resolve from the working directory. There is no additional display file, automatic home-file lookup or merge layer. A home file remains an explicit choice: `onetui --config "$HOME/onetui.toml"`.

Use `v` or `:display` as the in-app menu for formats and all switches. List available formats with reasons when a format cannot represent the selected value. Enter applies a choice; Esc closes the menu. Direct commands include `:display format hex`, `:display pretty-print off`, `:display highlight off`, `:display word-wrap off` and `:display unicode escaped`; command booleans accept `on` or `off`. A format choice overrides the current field detail until it closes; the other switches apply across the session and across datasource changes. Startup format preference remains in TOML. Table previews use auto format independently of the detail-format preference. Runtime choices override config in memory only and never write the file.

Context and `?` expose current display actions and effective settings. The input bar still appears only while typing. The value header identifies the effective format and byte provenance. The offline `onetui schema` catalog must describe implemented formats, settings, defaults and command usage from the same descriptors; do not advertise this proposal there before implementation.

## Limits and alternatives

Format retained data without fetching again, modifying provider continuation or changing connection lifetime. Page-local filter/sort keep their stable unformatted text projection; display choices must not change matching, row order or selection. Sanitize only the derived display projection, preserving retained values for other formats.

Count retained values and derived caches against bounded storage. Generate hex/binary views by byte ranges rather than allocating an entire expanded value: binary digits alone multiply output size by eight. JSON parsing, indentation, highlighting and wrapping need explicit work/depth/output limits before shipping. If formatting exceeds a limit, keep the value available in a bounded safe view and report why formatting stopped. Never silently truncate or advance backend paging. Cache formatting across unchanged frames; changing data or relevant settings invalidates it. Keep byte offsets separate from character and terminal-column positions.

Always pretty-printing hides the original presentation and removes user choice. Treating every cell as JSON loses non-JSON types. Treating every cell as UTF-8 loses arbitrary bytes. Lossy decoding conceals corruption; generic debug output is not a byte viewer. Requiring a formatter trait or plugin ABI adds extensibility that built-in enum dispatch already supplies.

Legacy encoding selection, automatic decompression, base64 decoding, charts, image rendering, editing and export are outside this decision. The byte view remains usable without those decoders. This decision does not change the initial read-only scope.
