use std::collections::VecDeque;

use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use onetui_core::catalog::{ACTIONS, Action, ActionDescriptor, ResourceDescriptor};
use onetui_core::config::Config;
use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, display};

#[derive(Clone)]
pub struct Request {
    pub id: u64,
    pub alias: String,
    pub resource: Resource,
    pub offset: i64,
    pub continuation: Option<String>,
    pub queued_at: tokio::time::Instant,
    reset: bool,
}

pub struct View {
    pub alias: Option<String>,
    pub resource: Resource,
    pub page: Page,
    pub selected: usize,
    pub offset: i64,
    pub column: usize,
    pub visible: Vec<usize>,
    pub filter: String,
    pub sort: Option<(usize, bool)>,
    previous: VecDeque<(i64, Page, Option<usize>)>,
}

impl View {
    fn new(alias: Option<String>, resource: Resource) -> Self {
        Self {
            alias,
            resource,
            page: Page::default(),
            selected: 0,
            offset: 0,
            column: 0,
            visible: Vec::new(),
            filter: String::new(),
            sort: None,
            previous: VecDeque::new(),
        }
    }

    pub fn selected_index(&self) -> Option<usize> {
        self.visible.get(self.selected).copied()
    }

    fn rebuild(&mut self, keep: Option<usize>) {
        // Reorder display indices without changing native rows or continuation.
        self.visible = self
            .page
            .rows
            .iter()
            .enumerate()
            .filter_map(|(i, row)| {
                row.cells
                    .iter()
                    .any(|cell| cell.as_deref().unwrap_or("NULL").contains(&self.filter))
                    .then_some(i)
            })
            .collect();
        if let Some((column, descending)) = self.sort {
            self.visible.sort_by(|&a, &b| {
                let order = self.page.rows[a].cells[column].cmp(&self.page.rows[b].cells[column]);
                if descending { order.reverse() } else { order }
            });
        }
        self.selected = keep
            .and_then(|index| self.visible.iter().position(|&i| i == index))
            .unwrap_or(0);
    }
}

pub struct App {
    pub config: Config,
    pub view: View,
    parents: Vec<View>,
    pub generation: u64,
    pub session: u64,
    pub connection_status: Option<onetui_core::provider::ConnectionStatus>,
    pub request: Option<Request>,
    pub loading: bool,
    pub error: Option<String>,
    pub help: bool,
    pub detail: bool,
    pub detail_text: String,
    pub detail_chunk: usize,
    pub detail_chunks: usize,
    pub detail_scroll: u16,
    pub command: Option<String>,
    pub filter_input: Option<String>,
    pub quit: bool,
}

impl App {
    pub fn new(config: Config, alias: Option<&str>) -> Self {
        let mut app = Self {
            config,
            view: View::new(None, Resource::new("connections", vec![])),
            parents: Vec::new(),
            generation: 0,
            session: 0,
            connection_status: None,
            request: None,
            loading: false,
            error: None,
            help: false,
            detail: false,
            detail_text: String::new(),
            detail_chunk: 0,
            detail_chunks: 0,
            detail_scroll: 0,
            command: None,
            filter_input: None,
            quit: false,
        };
        app.connections();
        if let Some(alias) = alias {
            if let Some(index) = app
                .config
                .aliases()
                .iter()
                .position(|(name, _)| *name == alias)
            {
                app.view.selected = index;
                app.act(Action::Open);
            } else {
                app.error = Some("Unknown connection alias; select a configured connection".into());
            }
        }
        app
    }

    fn invalidate(&mut self) {
        self.generation += 1;
        self.request = None;
        self.loading = false;
        self.error = None;
    }

    fn connections(&mut self) {
        self.invalidate();
        self.session += 1;
        self.connection_status = None;
        self.parents.clear();
        self.view = View::new(None, Resource::new("connections", vec![]));
        self.view.page.rows = self
            .config
            .aliases()
            .into_iter()
            .map(|(alias, kind)| Row {
                cells: vec![
                    Some(display(alias)),
                    Some(kind.into()),
                    Some(
                        self.config
                            .descriptor(alias)
                            .expect("validated alias")
                            .browsing
                            .into(),
                    ),
                ],
                target: None,
            })
            .collect();
        self.view.rebuild(None);
        self.help = false;
        self.detail = false;
        self.detail_text.clear();
    }

    fn load(&mut self, offset: i64, reset: bool) {
        self.invalidate();
        self.loading = true;
        self.request = Some(Request {
            id: self.generation,
            alias: self.view.alias.clone().expect("datasource view has alias"),
            resource: self.view.resource.clone(),
            offset,
            continuation: if reset {
                None
            } else {
                self.view.page.continuation.clone()
            },
            reset,
            queued_at: tokio::time::Instant::now(),
        });
    }

    pub fn complete(&mut self, request: &Request, result: Result<Page>) {
        if request.id != self.generation {
            return;
        }
        self.loading = false;
        match result {
            Ok(page) if page.bytes() <= PAGE_BYTES => {
                if self.view.sort.is_some_and(|(column, _)| {
                    self.view.page.columns.get(column).map(|c| &c.name)
                        != page.columns.get(column).map(|c| &c.name)
                }) {
                    self.view.sort = None;
                }
                if request.reset {
                    self.view.previous.clear();
                } else {
                    let selected = self.view.selected_index();
                    self.view.previous.push_back((
                        self.view.offset,
                        std::mem::take(&mut self.view.page),
                        selected,
                    ));
                    while self.view.previous.len() > 2 {
                        self.view.previous.pop_front();
                    }
                }
                self.view.page = page;
                self.view.offset = request.offset;
                self.view.selected = 0;
                self.view.column = self.view.column.min(self.column_count().saturating_sub(1));
                if self
                    .view
                    .sort
                    .is_some_and(|(column, _)| column >= self.column_count())
                {
                    self.view.sort = None;
                }
                self.view.rebuild(None);
                self.error = None;
            }
            Ok(_) => {
                self.error =
                    Some("Page exceeds the 1 MiB display limit; current page retained".into())
            }
            Err(error) => self.error = Some(display(&error.to_string())),
        }
    }

    pub(crate) fn update_connection_status(
        &mut self,
        session: u64,
        alias: &str,
        status: onetui_core::provider::ConnectionStatus,
    ) {
        if session == self.session && self.view.alias.as_deref() == Some(alias) {
            self.connection_status = Some(status);
        }
    }

    pub fn available(&self, action: Action) -> bool {
        match action {
            Action::Filter => !self.detail,
            Action::Sort => !self.detail && self.column_count() > 0,
            Action::Columns => {
                !self.loading && !self.detail && self.action_target(action).is_some()
            }
            Action::Left | Action::Right => self.column_count() > 0,
            Action::Back => self.help || self.detail || !self.parents.is_empty(),
            Action::Open => !self.loading && !self.view.visible.is_empty(),
            Action::Next => {
                if self.detail {
                    self.detail_chunk + 1 < self.detail_chunks
                } else {
                    !self.loading && self.view.page.next
                }
            }
            Action::Previous => {
                if self.detail {
                    self.detail_chunk > 0
                } else {
                    !self.loading && !self.view.previous.is_empty()
                }
            }
            Action::Up | Action::Down => !self.view.visible.is_empty(),
            _ => true,
        }
    }

    pub fn descriptor(&self) -> &'static ResourceDescriptor {
        if self.view.resource.id == "connections" {
            &onetui_core::catalog::CONNECTIONS
        } else {
            self.config
                .descriptor(self.view.alias.as_deref().expect("datasource alias"))
                .and_then(|provider| provider.resource(self.view.resource.id))
                .expect("connector emitted a registered resource")
        }
    }

    fn action_target(&self, action: Action) -> Option<Resource> {
        self.descriptor()
            .actions
            .iter()
            .find(|entry| entry.id == action)?
            .target(
                &self.view.resource,
                self.view
                    .selected_index()
                    .and_then(|i| self.view.page.rows.get(i)),
            )
    }

    pub fn column_count(&self) -> usize {
        if self.view.page.columns.is_empty() {
            self.descriptor().columns.len()
        } else {
            self.view.page.columns.len()
        }
    }

    pub fn column_name(&self, index: usize) -> &str {
        if self.view.page.columns.is_empty() {
            self.descriptor().columns[index]
        } else {
            &self.view.page.columns[index].name
        }
    }

    fn detail_text(&mut self) {
        let cell = &self.view.page.rows[self.view.selected_index().expect("selected detail row")]
            .cells[self.view.column];
        let value = cell.as_deref().unwrap_or("");
        self.detail_chunks = value.chars().count().div_ceil(4096).max(1);
        self.detail_chunk = self.detail_chunk.min(self.detail_chunks - 1);
        let datatype = self
            .view
            .page
            .columns
            .get(self.view.column)
            .map_or("metadata", |c| c.datatype.as_str());
        self.detail_text = format!(
            "{} | {} | {}\n{}",
            self.column_name(self.view.column),
            datatype,
            match cell {
                None => "SQL NULL",
                Some(v) if v.is_empty() => "empty text",
                Some(_) => "non-null text",
            },
            value
                .chars()
                .skip(self.detail_chunk * 4096)
                .take(4096)
                .collect::<String>()
        );
        self.detail_scroll = 0;
    }

    pub fn actions(&self) -> impl Iterator<Item = &'static ActionDescriptor> + '_ {
        ACTIONS.iter().filter(|entry| self.available(entry.id))
    }

    pub fn act(&mut self, action: Action) {
        if !self.available(action) {
            return;
        }
        if self.detail {
            match action {
                Action::Up => self.detail_scroll = self.detail_scroll.saturating_sub(1),
                Action::Down => {
                    self.detail_scroll = self.detail_scroll.saturating_add(1).min(16384)
                }
                Action::Next => {
                    self.detail_chunk += 1;
                    self.detail_text();
                }
                Action::Previous => {
                    self.detail_chunk -= 1;
                    self.detail_text();
                }
                Action::Open => return,
                _ => {}
            }
            if matches!(
                action,
                Action::Up | Action::Down | Action::Next | Action::Previous
            ) {
                return;
            }
        }
        match action {
            Action::Filter => {
                self.help = false;
                self.filter_input = Some(self.view.filter.clone());
            }
            Action::Sort => {
                self.help = false;
                let selected = self.view.selected_index();
                self.view.sort = match self.view.sort {
                    Some((column, false)) if column == self.view.column => Some((column, true)),
                    Some((column, true)) if column == self.view.column => None,
                    _ => Some((self.view.column, false)),
                };
                self.view.rebuild(selected);
            }
            Action::Left | Action::Right => {
                self.view.column = if action == Action::Left {
                    self.view.column.saturating_sub(1)
                } else {
                    (self.view.column + 1).min(self.column_count().saturating_sub(1))
                };
                if self.detail {
                    self.detail_chunk = 0;
                    self.detail_text();
                }
            }
            Action::Columns => {
                self.help = false;
                let target = self
                    .action_target(action)
                    .expect("available resource action");
                let alias = self.view.alias.clone();
                let parent = std::mem::replace(&mut self.view, View::new(alias, target));
                self.parents.push(parent);
                self.load(0, true);
            }
            Action::Up => self.view.selected = self.view.selected.saturating_sub(1),
            Action::Down => {
                self.view.selected =
                    (self.view.selected + 1).min(self.view.visible.len().saturating_sub(1))
            }
            Action::Open => {
                if self.help || self.detail {
                    return;
                }
                let (alias, target) = if self.view.resource == Resource::new("connections", vec![])
                {
                    let (alias, _) = self.config.aliases()
                        [self.view.selected_index().expect("selected connection")];
                    let provider = self.config.descriptor(alias).expect("validated alias");
                    let Some(entry) = provider.entry_resource else {
                        self.error = Some(format!(
                            "{} browsing is unavailable; use --check for now",
                            provider.kind
                        ));
                        return;
                    };
                    self.session += 1;
                    self.connection_status =
                        Some(onetui_core::provider::ConnectionStatus::Configured);
                    (Some(alias.to_owned()), Resource::new(entry, vec![]))
                } else if let Some(target) = &self.view.page.rows
                    [self.view.selected_index().expect("selected resource")]
                .target
                {
                    (self.view.alias.clone(), target.clone())
                } else {
                    self.detail = true;
                    self.detail_chunk = 0;
                    self.detail_text();
                    return;
                };
                let parent = std::mem::replace(&mut self.view, View::new(alias, target));
                self.parents.push(parent);
                self.load(0, true);
            }
            Action::Back => {
                if self.help {
                    self.help = false;
                } else if self.detail {
                    self.detail = false;
                    self.detail_text.clear();
                } else if let Some(parent) = self.parents.pop() {
                    self.invalidate();
                    self.view = parent;
                    if self.view.alias.is_none() {
                        self.session += 1;
                        self.connection_status = None;
                    }
                }
            }
            Action::Connections => self.connections(),
            Action::Next => {
                self.detail = false;
                if let Some(offset) = self.view.offset.checked_add(PAGE_SIZE) {
                    self.load(offset, false);
                }
            }
            Action::Previous => {
                self.detail = false;
                if let Some((offset, page, selected)) = self.view.previous.pop_back() {
                    self.invalidate();
                    self.view.offset = offset;
                    self.view.page = page;
                    self.view.rebuild(selected);
                }
            }
            Action::Refresh => {
                self.detail = false;
                if self.view.resource == Resource::new("connections", vec![]) {
                    let filter = std::mem::take(&mut self.view.filter);
                    let sort = self.view.sort;
                    self.connections();
                    self.view.filter = filter;
                    self.view.sort = sort;
                    self.view.rebuild(None);
                } else {
                    self.load(0, true);
                }
            }
            Action::Help => self.help = !self.help,
            Action::Quit => {
                self.invalidate();
                self.quit = true;
            }
            Action::Cancel => {
                if self.loading {
                    self.invalidate();
                    self.error = Some("Request cancelled; displayed data retained".into());
                } else {
                    self.quit = true;
                }
            }
        }
    }

    pub fn key(&mut self, key: KeyEvent) {
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.command = None;
            self.filter_input = None;
            self.act(Action::Cancel);
            return;
        }
        if let Some(input) = &mut self.filter_input {
            match key.code {
                KeyCode::Esc => self.filter_input = None,
                KeyCode::Enter => {
                    let selected = self.view.selected_index();
                    self.view.filter = self.filter_input.take().unwrap();
                    self.view.rebuild(selected);
                }
                KeyCode::Backspace => {
                    input.pop();
                }
                KeyCode::Char(c)
                    if !c.is_control()
                        && input.len() + c.len_utf8() <= 256
                        && !key
                            .modifiers
                            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
                {
                    input.push(c);
                }
                _ => {}
            }
            return;
        }
        if let Some(command) = &mut self.command {
            match key.code {
                KeyCode::Esc => self.command = None,
                KeyCode::Enter => {
                    let command = self.command.take().unwrap();
                    match serde_json::from_value::<Action>(serde_json::Value::String(command)) {
                        Ok(action) if self.available(action) => self.act(action),
                        _ => {
                            self.error = Some(
                                "Unknown or unavailable command; use ? for available actions"
                                    .into(),
                            )
                        }
                    }
                }
                KeyCode::Backspace => {
                    command.pop();
                }
                KeyCode::Char(c)
                    if c.is_ascii_lowercase()
                        && command.len() < 32
                        && !key
                            .modifiers
                            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
                {
                    command.push(c)
                }
                _ => {}
            }
            return;
        }
        if key
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
        {
            return;
        }
        if key.code == KeyCode::Char(':') {
            self.command = Some(String::new());
            return;
        }
        let name = match key.code {
            KeyCode::Char(c) => c.to_string(),
            KeyCode::Up => "Up".into(),
            KeyCode::Down => "Down".into(),
            KeyCode::Left => "Left".into(),
            KeyCode::Right => "Right".into(),
            KeyCode::Enter => "Enter".into(),
            KeyCode::Esc => "Esc".into(),
            _ => return,
        };
        if let Some(action) = ACTIONS
            .iter()
            .find(|entry| entry.keys.contains(&name.as_str()))
            .map(|entry| entry.id)
        {
            self.act(action);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn app() -> App {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(file, "[connections.pg]\nkind='fake'\nurl_env='NEVER_RESOLVE_THIS'\n[connections.q]\nkind='checkonly'\nurl='http://localhost:6334'").unwrap();
        App::new(
            Config::load(file.path(), crate::test_provider::CATALOG).unwrap(),
            Some("pg"),
        )
    }

    fn filter(app: &mut App, text: &str) {
        app.key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        while !app.filter_input.as_ref().unwrap().is_empty() {
            app.key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        }
        for c in text.chars() {
            app.key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    }

    #[test]
    fn local_sort_filter_and_detail_never_rewrite_rows_or_continuation() {
        let mut app = app();
        let request = app.request.take().unwrap();
        app.view.resource = Resource::new("fake.rows", vec!["public".into(), "sample".into()]);
        let values = vec![
            Some("2".into()),
            None,
            Some("".into()),
            Some("10".into()),
            Some("NULL".into()),
            Some("2".into()),
        ];
        app.complete(
            &request,
            Ok(Page {
                columns: vec![onetui_core::Column {
                    name: "value".into(),
                    datatype: "text".into(),
                }],
                rows: values
                    .iter()
                    .map(|value| Row {
                        cells: vec![value.clone()],
                        target: None,
                    })
                    .collect(),
                next: true,
                continuation: Some("native token".into()),
                ..Page::default()
            }),
        );
        app.key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE));
        assert_eq!(app.view.visible, vec![1, 2, 3, 0, 5, 4]);
        assert_eq!(app.view.selected_index(), Some(0));
        app.act(Action::Sort);
        assert_eq!(app.view.visible, vec![4, 0, 5, 3, 2, 1]);
        app.act(Action::Sort);
        assert_eq!(app.view.visible, (0..6).collect::<Vec<_>>());
        filter(&mut app, "10");
        assert_eq!(app.view.visible, vec![3]);
        app.act(Action::Open);
        assert!(app.detail_text.ends_with("\n10"));
        assert!(!app.available(Action::Filter));
        assert!(!app.available(Action::Sort));
        app.act(Action::Back);
        filter(&mut app, "NULL");
        assert_eq!(app.view.visible, vec![1, 4]);
        filter(&mut app, "not present");
        assert!(app.view.visible.is_empty());
        assert!(!app.available(Action::Open));
        assert!(app.available(Action::Next));
        assert!(app.request.is_none());
        app.act(Action::Next);
        let next = app.request.take().unwrap();
        assert_eq!(next.continuation.as_deref(), Some("native token"));
        app.complete(&next, Err(anyhow::anyhow!("read failed")));
        assert_eq!(app.view.filter, "not present");
        assert_eq!(
            app.view
                .page
                .rows
                .iter()
                .map(|r| r.cells[0].clone())
                .collect::<Vec<_>>(),
            values
        );
        assert_eq!(app.view.page.continuation.as_deref(), Some("native token"));
        filter(&mut app, "");
        assert_eq!(app.view.visible.len(), 6);
    }

    #[test]
    fn filtered_sorted_connections_and_parents_use_source_indices() {
        let mut app = app();
        app.act(Action::Connections);
        filter(&mut app, "checkonly");
        assert_eq!(app.view.selected_index(), Some(1));
        app.act(Action::Open);
        assert!(app.error.as_ref().unwrap().contains("unavailable"));
        filter(&mut app, "fake");
        app.act(Action::Sort);
        app.act(Action::Refresh);
        assert_eq!(app.view.filter, "fake");
        app.act(Action::Open);
        assert_eq!(app.request.as_ref().unwrap().alias, "pg");
        app.act(Action::Back);
        assert_eq!(app.view.filter, "fake");
        assert_eq!(app.view.sort, Some((0, false)));
        assert_eq!(app.view.selected_index(), Some(0));
    }

    #[test]
    fn filter_input_is_bounded_unicode_text_not_navigation() {
        let mut app = app();
        app.act(Action::Connections);
        app.act(Action::Help);
        assert!(app.actions().any(|entry| entry.id == Action::Filter));
        assert!(app.actions().any(|entry| entry.id == Action::Sort));
        filter(&mut app, "q");
        assert!(!app.help);
        app.act(Action::Filter);
        for c in "uit:/snrc?".chars() {
            app.key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        assert!(!app.quit);
        assert!(!app.help);
        assert!(app.command.is_none());
        assert!(app.request.is_none());
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(app.view.filter, "q");
        app.act(Action::Filter);
        app.key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        for _ in 0..100 {
            app.key(KeyEvent::new(KeyCode::Char('🌊'), KeyModifiers::NONE));
        }
        assert_eq!(app.filter_input.as_ref().unwrap().len(), 256);
        app.key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        assert_eq!(app.filter_input.as_ref().unwrap().len(), 252);
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(app.view.visible.is_empty());
        filter(&mut app, "");
        assert_eq!(app.view.visible.len(), 2);
    }

    #[test]
    fn retained_pages_reapply_current_local_state_and_refresh_resets_native_token() {
        let mut app = app();
        let first = app.request.take().unwrap();
        filter(&mut app, "public");
        assert_eq!(
            app.generation, first.id,
            "local input must not invalidate active fetch"
        );
        app.complete(&first, Ok(page(true)));
        app.act(Action::Sort);
        filter(&mut app, "public");
        app.act(Action::Next);
        let next = app.request.take().unwrap();
        app.complete(&next, Ok(page(true)));
        filter(&mut app, "missing");
        app.act(Action::Previous);
        assert!(app.view.visible.is_empty());
        assert_eq!(app.view.filter, "missing");
        assert_eq!(app.view.sort, Some((0, false)));
        assert!(app.request.is_none());
        filter(&mut app, "public");
        app.act(Action::Refresh);
        let refresh = app.request.take().unwrap();
        assert!(refresh.continuation.is_none());
        app.complete(&refresh, Ok(Page::default()));
        assert!(!app.available(Action::Previous));
        assert_eq!(app.view.filter, "public");
    }

    fn page(next: bool) -> Page {
        Page {
            rows: vec![Row {
                cells: vec![Some(display("public\x1b[31m"))],
                target: Some(Resource::new(
                    "fake.relations",
                    vec!["public\x1b[31m".into()],
                )),
            }],
            next,
            ..Page::default()
        }
    }

    #[test]
    fn late_success_and_error_after_navigation_are_ignored() {
        let mut app = app();
        let old = app.request.take().unwrap();
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        app.complete(&old, Ok(page(true)));
        app.complete(&old, Err(anyhow::anyhow!("old failure")));
        assert_eq!(app.view.resource, Resource::new("connections", vec![]));
        assert!(app.error.is_none());
        assert_eq!(app.view.page.rows[0].cells[0].as_deref(), Some("pg"));
    }

    #[test]
    fn cancel_then_new_request_and_empty_or_denied_results() {
        let mut app = app();
        let old = app.request.take().unwrap();
        app.key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
        app.key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE));
        let new = app.request.take().unwrap();
        app.complete(&old, Err(anyhow::anyhow!("old failure")));
        assert!(app.loading);
        app.complete(&new, Ok(Page::default()));
        assert!(!app.loading);
        assert!(!app.available(Action::Open));
        app.act(Action::Refresh);
        let request = app.request.take().unwrap();
        app.complete(&request, Err(anyhow::anyhow!("metadata access denied")));
        assert_eq!(app.error.as_deref(), Some("metadata access denied"));
    }

    #[test]
    fn pagination_preserves_failed_page_and_retains_only_three_pages() {
        let mut app = app();
        let request = app.request.take().unwrap();
        app.complete(&request, Ok(page(true)));
        for _ in 0..4 {
            app.act(Action::Next);
            let request = app.request.take().unwrap();
            app.complete(&request, Ok(page(true)));
        }
        assert_eq!(app.view.previous.len(), 2);
        app.act(Action::Next);
        let request = app.request.take().unwrap();
        app.complete(&request, Err(anyhow::anyhow!("limit exceeded")));
        assert_eq!(app.view.offset, 400);
        app.act(Action::Previous);
        assert_eq!(app.view.offset, 300);
        assert!(app.request.is_none());
        assert!(
            !app.view.page.rows[0].cells[0]
                .as_ref()
                .unwrap()
                .contains('\x1b')
        );
    }

    #[test]
    fn row_continuation_is_opaque_and_failure_preserves_data_and_position() {
        let mut app = app();
        let request = app.request.take().unwrap();
        let mut first = page(true);
        first.continuation = Some("connector-owned exact token".into());
        app.complete(&request, Ok(first));
        app.act(Action::Next);
        let next = app.request.take().unwrap();
        assert_eq!(next.continuation, app.view.page.continuation);
        let token = next.continuation.clone();
        let mut oversized = page(false);
        oversized.rows[0].cells[0] = Some("x".repeat(PAGE_BYTES + 1));
        app.complete(&next, Ok(oversized));
        assert_eq!(app.view.offset, 0);
        assert_eq!(app.view.page.continuation, token);
        assert!(app.error.as_ref().unwrap().contains("limit"));
        app.act(Action::Refresh);
        assert!(app.request.as_ref().unwrap().continuation.is_none());
        app.act(Action::Cancel);
        assert_eq!(app.view.page.continuation, token);
    }

    #[test]
    fn detail_preserves_null_empty_literal_and_every_unicode_chunk() {
        let mut app = app();
        app.view.resource = Resource::new("fake.rows", vec!["public".into(), "sample".into()]);
        let request = app.request.take().unwrap();
        let text = "🌊".repeat(9000);
        app.complete(
            &request,
            Ok(Page {
                columns: (0..4)
                    .map(|i| onetui_core::Column {
                        name: format!("c{i}"),
                        datatype: "text".into(),
                    })
                    .collect(),
                rows: vec![Row {
                    cells: vec![
                        None,
                        Some(String::new()),
                        Some("NULL".into()),
                        Some(text.clone()),
                    ],
                    target: None,
                }],
                ..Page::default()
            }),
        );
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(app.detail_text.contains("SQL NULL"));
        app.act(Action::Right);
        assert!(app.detail_text.contains("empty text"));
        app.act(Action::Right);
        assert!(app.detail_text.contains("non-null text\nNULL"));
        app.act(Action::Right);
        assert_eq!(app.detail_chunks, 3);
        let mut restored = String::new();
        loop {
            restored.push_str(app.detail_text.split_once('\n').unwrap().1);
            app.act(Action::Down);
            assert_eq!(app.detail_scroll, 1);
            if !app.available(Action::Next) {
                break;
            }
            app.key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::NONE));
            assert_eq!(app.detail_scroll, 0);
        }
        assert_eq!(restored, text);
        assert!(app.request.is_none(), "detail chunks must never fetch");
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        app.key(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
        assert_eq!(app.view.resource.id, "fake.columns");
        app.act(Action::Back);
        assert_eq!(app.view.resource.id, "fake.rows");
        assert_eq!(app.view.column, 3);
    }

    #[test]
    fn palette_consumes_navigation_keys_and_checkonly_stays_check_only() {
        let mut app = app();
        app.act(Action::Connections);
        for c in ":quit".chars() {
            app.key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        assert!(!app.quit);
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        app.act(Action::Down);
        app.act(Action::Open);
        assert!(app.error.unwrap().contains("unavailable"));
        assert!(app.request.is_none());
    }
}
