use anyhow::{Result, ensure};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use onetui_core::provider::{ConnectionInput, ProviderDescriptor};

use crate::query::Editor;

pub struct Form {
    pub providers: Vec<&'static ProviderDescriptor>,
    pub selected: usize,
    pub choosing: bool,
    pub field: usize,
    pub inputs: Vec<Editor>,
    pub save: bool,
    pub documentation: serde_json::Value,
}

impl Form {
    pub fn new(providers: Vec<&'static ProviderDescriptor>) -> Self {
        Self {
            providers,
            selected: 0,
            choosing: true,
            field: 0,
            inputs: Vec::new(),
            save: false,
            documentation: serde_json::Value::Null,
        }
    }

    pub fn provider(&self) -> &'static ProviderDescriptor {
        self.providers[self.selected]
    }

    pub fn key(&mut self, key: KeyEvent) -> Result<()> {
        if self.choosing {
            match key.code {
                KeyCode::Up | KeyCode::Char('k') => self.selected = self.selected.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => {
                    self.selected = (self.selected + 1).min(self.providers.len() - 1)
                }
                KeyCode::Enter => {
                    self.inputs = (0..=self.provider().connection_fields.len())
                        .map(|_| Editor::new(String::new()))
                        .collect();
                    self.choosing = false;
                    self.documentation = (self.provider().documentation)()["configuration"].take();
                }
                _ => {}
            }
            return Ok(());
        }
        match key.code {
            KeyCode::F(2) => self.save = true,
            KeyCode::Char('s') if key.modifiers.contains(KeyModifiers::CONTROL) => self.save = true,
            KeyCode::Tab | KeyCode::Enter | KeyCode::Down => {
                self.field = (self.field + 1) % self.inputs.len()
            }
            KeyCode::BackTab | KeyCode::Up => {
                self.field = (self.field + self.inputs.len() - 1) % self.inputs.len()
            }
            _ => {
                if let KeyCode::Char(c) = key.code
                    && !key
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                {
                    self.insert(&c.to_string())?;
                } else {
                    self.inputs[self.field]
                        .key(key)
                        .map_err(anyhow::Error::msg)?;
                }
            }
        }
        Ok(())
    }

    pub fn insert(&mut self, text: &str) -> Result<()> {
        if self.choosing {
            return Ok(());
        }
        ensure!(
            !text.chars().any(char::is_control),
            "Connection fields require single-line text without control characters"
        );
        ensure!(
            self.inputs.iter().map(|e| e.text.len()).sum::<usize>() + text.len() <= 16 * 1024,
            "Connection form exceeds 16 KiB"
        );
        self.inputs[self.field]
            .insert(text)
            .map_err(anyhow::Error::msg)
    }

    pub fn options(&self) -> Result<toml::Table> {
        let mut options = toml::Table::new();
        for (field, editor) in self
            .provider()
            .connection_fields
            .iter()
            .zip(&self.inputs[1..])
        {
            let text = editor.text.trim();
            if text.is_empty() {
                continue;
            }
            let value = match field.input {
                ConnectionInput::Text => toml::Value::String(text.into()),
                ConnectionInput::StringList => toml::Value::Array(
                    text.split(',')
                        .map(|item| toml::Value::String(item.trim().into()))
                        .collect(),
                ),
                ConnectionInput::Boolean => toml::Value::Boolean(
                    text.parse()
                        .map_err(|_| anyhow::anyhow!("{} must be true or false", field.name))?,
                ),
            };
            options.insert(field.name.into(), value);
        }
        Ok(options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use onetui_core::provider::ConnectionField;

    static PROVIDER: ProviderDescriptor = ProviderDescriptor {
        connection_fields: &[
            ConnectionField::list("servers"),
            ConnectionField::boolean("tls"),
            ConnectionField::text("token_env"),
        ],
        follow_resources: &[],
        query: None,
        kind: "fake",
        entry_resource: None,
        browsing: "test",
        resources: &[],
        documentation: || serde_json::json!({}),
    };

    #[test]
    fn form_parses_lists_booleans_and_omits_blank_fields() {
        let mut form = Form::new(vec![&PROVIDER]);
        form.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .unwrap();
        form.insert("sample").unwrap();
        form.key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE))
            .unwrap();
        form.insert("host1:4222, host2:4222").unwrap();
        form.key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE))
            .unwrap();
        form.insert("false").unwrap();
        let options = form.options().unwrap();
        assert_eq!(
            options["servers"].as_array().unwrap(),
            &[
                toml::Value::String("host1:4222".into()),
                toml::Value::String("host2:4222".into())
            ]
        );
        assert_eq!(options["tls"].as_bool(), Some(false));
        assert!(!options.contains_key("token_env"));
        form.key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL))
            .unwrap();
        form.insert("not-a-boolean").unwrap();
        assert!(
            form.options()
                .unwrap_err()
                .to_string()
                .contains("tls must be true or false")
        );
    }

    #[test]
    fn paste_rejects_controls_and_total_input_is_bounded() {
        let mut form = Form::new(vec![&PROVIDER]);
        form.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .unwrap();
        assert!(form.insert("line\nbreak").is_err());
        assert!(form.insert("\x1b[31m").is_err());
        assert!(form.inputs[0].text.is_empty());
        form.insert(&"a".repeat(16 * 1024)).unwrap();
        form.key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE))
            .unwrap();
        assert!(form.insert("b").is_err());
        form.key(KeyEvent::new(KeyCode::BackTab, KeyModifiers::SHIFT))
            .unwrap();
        assert_eq!(form.field, 0);
        form.key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL))
            .unwrap();
        form.insert("София").unwrap();
        form.key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE))
            .unwrap();
        assert_eq!(form.inputs[0].text, "Софи");
    }
}
