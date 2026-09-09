pub mod catalog;
pub mod config;
pub mod provider;
pub mod value;
pub use value::Value;
#[cfg(test)]
mod test_provider;

pub const PAGE_SIZE: i64 = 100;
pub const PAGE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resource {
    pub id: &'static str,
    pub path: Vec<String>,
}

impl Resource {
    pub fn new(id: &'static str, path: Vec<String>) -> Self {
        Self { id, path }
    }
    pub fn breadcrumb(&self) -> String {
        self.path
            .iter()
            .map(|part| display(part))
            .chain(std::iter::once(self.id.into()))
            .collect::<Vec<_>>()
            .join(" / ")
    }
}

#[derive(Clone, Debug)]
pub struct Row {
    pub cells: Vec<Option<Value>>,
    pub target: Option<Resource>,
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub datatype: String,
}

#[derive(Clone, Debug, Default)]
pub struct Page {
    pub rows: Vec<Row>,
    pub next: bool,
    pub columns: Vec<Column>,
    pub continuation: Option<String>,
    pub notice: String,
}

impl Page {
    pub fn bytes(&self) -> usize {
        self.rows
            .iter()
            .map(|row| {
                row.cells
                    .iter()
                    .flatten()
                    .map(|v| v.bytes().len())
                    .sum::<usize>()
                    + row
                        .target
                        .as_ref()
                        .map_or(0, |target| target.path.iter().map(String::len).sum())
            })
            .sum::<usize>()
            + self
                .columns
                .iter()
                .map(|column| column.name.len() + column.datatype.len())
                .sum::<usize>()
            + self.continuation.as_ref().map_or(0, String::len)
            + self.notice.len()
    }
}

// Escape terminal controls and bidirectional overrides once, before storing display data.
pub fn display(value: &str) -> String {
    let mut text = String::with_capacity(value.len());
    for c in value.chars() {
        if c.is_control()
            || matches!(c, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{2028}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        {
            text.extend(c.escape_default());
        } else {
            text.push(c);
        }
    }
    text
}

/// Preserve native error wording and causes without echoing configured secrets or controls.
pub fn diagnostic(error: anyhow::Error, secrets: &[&str]) -> anyhow::Error {
    let mut message = format!("{error:#}");
    for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
        message = message.replace(secret, "[REDACTED]");
    }
    anyhow::anyhow!(display(&message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_preserve_causes_and_escape_after_redaction() {
        let error = anyhow::anyhow!("certificate rejected: secret\nvalue\x1b[31m")
            .context("transport error");
        assert_eq!(
            diagnostic(error, &["", "secret\nvalue"]).to_string(),
            "transport error: certificate rejected: [REDACTED]\\u{1b}[31m"
        );
    }

    #[test]
    fn terminal_text_is_escaped_without_losing_unicode() {
        let escaped = display("София 🌊\n\x1b]52;c;secret\x07\u{202e}abc\u{009b}31m");
        assert!(escaped.starts_with("София 🌊\\n"));
        assert!(!escaped.chars().any(char::is_control));
        assert!(!escaped.contains('\u{202e}'));
        assert!(escaped.contains("secret"));
    }
}
