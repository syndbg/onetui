pub mod catalog;
pub mod config;

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
    pub cells: Vec<String>,
    pub target: Option<Resource>,
}

#[derive(Clone, Debug, Default)]
pub struct Page {
    pub rows: Vec<Row>,
    pub next: bool,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_text_is_escaped_without_losing_unicode() {
        let escaped = display("София 🌊\n\x1b]52;c;secret\x07\u{202e}abc\u{009b}31m");
        assert!(escaped.starts_with("София 🌊\\n"));
        assert!(!escaped.chars().any(char::is_control));
        assert!(!escaped.contains('\u{202e}'));
        assert!(escaped.contains("secret"));
    }
}
