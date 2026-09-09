use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use onetui_core::provider::QUERY_BYTES;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

pub struct Editor {
    pub text: String,
    cursor: usize,
}

impl Editor {
    pub fn new(text: String) -> Self {
        Self {
            cursor: text.len(),
            text,
        }
    }

    pub fn insert(&mut self, input: &str) -> Result<(), &'static str> {
        let input = input
            .replace("\r\n", "\n")
            .replace('\r', "\n")
            .replace('\t', "    ");
        if input.chars().any(|c| c.is_control() && c != '\n') {
            return Err("Query input contains a control character");
        }
        if self.text.len() + input.len() > QUERY_BYTES {
            return Err("Query exceeds 16 KiB");
        }
        self.text.insert_str(self.cursor, &input);
        self.cursor += input.len();
        Ok(())
    }

    pub fn key(&mut self, key: KeyEvent) -> Result<(), &'static str> {
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            match key.code {
                KeyCode::Char('u') => {
                    self.text.clear();
                    self.cursor = 0;
                }
                KeyCode::Home => self.cursor = 0,
                KeyCode::End => self.cursor = self.text.len(),
                _ => {}
            }
            return Ok(());
        }
        if key.modifiers.contains(KeyModifiers::ALT) {
            return Ok(());
        }
        let previous = self.text[..self.cursor]
            .grapheme_indices(true)
            .next_back()
            .map_or(0, |(i, _)| i);
        let next = self.cursor
            + self.text[self.cursor..]
                .graphemes(true)
                .next()
                .map_or(0, str::len);
        match key.code {
            KeyCode::Left => self.cursor = previous,
            KeyCode::Right => self.cursor = next,
            KeyCode::Home => {
                self.cursor = self.text[..self.cursor].rfind('\n').map_or(0, |i| i + 1)
            }
            KeyCode::End => {
                self.cursor += self.text[self.cursor..]
                    .find('\n')
                    .unwrap_or(self.text.len() - self.cursor)
            }
            KeyCode::Up | KeyCode::Down => {
                let start = self.text[..self.cursor].rfind('\n').map_or(0, |i| i + 1);
                let column = self.text[start..self.cursor].graphemes(true).count();
                let target = if key.code == KeyCode::Up {
                    start
                        .checked_sub(1)
                        .map(|end| (self.text[..end].rfind('\n').map_or(0, |i| i + 1), end))
                } else {
                    self.text[self.cursor..].find('\n').map(|i| {
                        let start = self.cursor + i + 1;
                        (
                            start,
                            start
                                + self.text[start..]
                                    .find('\n')
                                    .unwrap_or(self.text.len() - start),
                        )
                    })
                };
                if let Some((start, end)) = target {
                    self.cursor = start
                        + self.text[start..end]
                            .graphemes(true)
                            .take(column)
                            .map(str::len)
                            .sum::<usize>();
                }
            }
            KeyCode::Backspace => {
                self.text.drain(previous..self.cursor);
                self.cursor = previous;
            }
            KeyCode::Delete => {
                self.text.drain(self.cursor..next);
            }
            KeyCode::Enter => self.insert("\n")?,
            KeyCode::Tab => self.insert("    ")?,
            KeyCode::Char(c) if !c.is_control() => self.insert(&c.to_string())?,
            _ => {}
        }
        Ok(())
    }

    pub fn lines(&self, width: usize, wrap: bool) -> (Vec<Line<'static>>, usize, usize) {
        let mut lines = vec![Line::default()];
        let mut column = 0;
        let mut caret = (0, 0);
        for (offset, grapheme) in self
            .text
            .grapheme_indices(true)
            .chain(std::iter::once((self.text.len(), " ")))
        {
            let text = if grapheme == "\n" {
                " ".into()
            } else {
                onetui_core::display(grapheme)
            };
            let size = text.width();
            if wrap && column > 0 && column + size > width.max(1) {
                lines.push(Line::default());
                column = 0;
            }
            let style = if offset == self.cursor
                || (offset < self.cursor && self.cursor < offset + grapheme.len())
            {
                caret = (lines.len() - 1, column);
                Style::new().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
            };
            lines
                .last_mut()
                .unwrap()
                .spans
                .push(Span::styled(text, style));
            column += size;
            if grapheme == "\n" {
                lines.push(Line::default());
                column = 0;
            }
        }
        (lines, caret.0, caret.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn multiline_unicode_editing_paste_and_bounds() {
        let mut editor = Editor::new(String::new());
        editor.insert("SELECT 'София 🌊'\r\nFROM demo").unwrap();
        editor
            .key(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE))
            .unwrap();
        editor
            .key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE))
            .unwrap();
        assert!(!editor.text.contains('\n'));
        editor
            .key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .unwrap();
        assert!(editor.text.contains("\nFROM"));
        assert!(editor.insert("\x1b[31m").is_err());
        assert!(editor.insert(&"x".repeat(QUERY_BYTES)).is_err());
        let (lines, row, _) = editor.lines(8, true);
        assert!(lines.len() > 2 && row > 0);
        editor
            .key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL))
            .unwrap();
        assert!(editor.text.is_empty());
        editor.insert("e\u{301}🌊").unwrap();
        editor
            .key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE))
            .unwrap();
        editor
            .key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE))
            .unwrap();
        assert_eq!(editor.text, "🌊");
    }
}
