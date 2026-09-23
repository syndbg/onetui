use std::collections::VecDeque;
use std::path::PathBuf;

use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use onetui_core::catalog::{ACTIONS, Action, ActionDescriptor, ResourceDescriptor};
use onetui_core::config::Config;
use onetui_core::value::{DisplayOptions, FORMATS, UnicodeDisplay, ValueFormat};
use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, Value, display};
use onetui_theme::Theme;

const BOOKMARK_LIMIT: usize = 4096;
mod follow;

struct PageBookmark {
    offset: i64,
    position: Option<String>,
    selected: Option<usize>,
    page: Option<Page>,
}

#[derive(Clone)]
pub struct Request {
    pub follow: bool,
    pub query: Option<String>,
    pub id: u64,
    pub alias: String,
    pub resource: Resource,
    pub offset: i64,
    pub continuation: Option<String>,
    pub queued_at: tokio::time::Instant,
    reset: bool,
}

pub struct View {
    pub live: bool,
    pub live_evicted: usize,
    pub query: Option<String>,
    query_draft: Option<String>,
    pub alias: Option<String>,
    pub resource: Resource,
    pub page: Page,
    pub selected: usize,
    pub offset: i64,
    pub column: usize,
    pub visible: Vec<usize>,
    pub filter: String,
    pub sort: Option<(usize, bool)>,
    pub projections: Vec<Vec<Option<String>>>,
    pub previews: Vec<Vec<String>>,
    pub previews_limited: bool,
    position: Option<String>,
    previous: VecDeque<PageBookmark>,
}

impl View {
    fn new(alias: Option<String>, resource: Resource) -> Self {
        Self {
            live: false,
            live_evicted: 0,
            query: None,
            query_draft: None,
            alias,
            resource,
            page: Page::default(),
            selected: 0,
            offset: 0,
            column: 0,
            visible: Vec::new(),
            filter: String::new(),
            sort: None,
            projections: Vec::new(),
            previews: Vec::new(),
            previews_limited: false,
            position: None,
            previous: VecDeque::new(),
        }
    }

    pub fn selected_index(&self) -> Option<usize> {
        self.visible.get(self.selected).copied()
    }

    fn rebuild(&mut self, keep: Option<usize>, options: DisplayOptions) {
        self.projections = projections(&self.page).expect("validated page projections");
        self.prepare_previews(options, true);
        self.reindex(keep);
    }

    fn reindex(&mut self, keep: Option<usize>) {
        // Reorder display indices without changing native rows or continuation.
        self.visible = self
            .projections
            .iter()
            .enumerate()
            .filter_map(|(i, row)| {
                row.iter()
                    .any(|cell| cell.as_deref().unwrap_or("NULL").contains(&self.filter))
                    .then_some(i)
            })
            .collect();
        if let Some((column, descending)) = self.sort {
            self.visible.sort_by(|&a, &b| {
                let order = self.projections[a][column].cmp(&self.projections[b][column]);
                if descending { order.reverse() } else { order }
            });
        }
        self.selected = keep
            .and_then(|index| self.visible.iter().position(|&i| i == index))
            .unwrap_or(0);
    }

    fn prepare_previews(&mut self, options: DisplayOptions, table: bool) {
        let mut budget = PAGE_BYTES;
        self.previews_limited = false;
        self.previews = self
            .page
            .rows
            .iter()
            .map(|row| {
                row.cells
                    .iter()
                    .enumerate()
                    .map(|(i, value)| {
                        if budget < '…'.len_utf8() {
                            self.previews_limited = true;
                            return String::new();
                        }
                        let declared = self
                            .page
                            .columns
                            .get(i)
                            .is_some_and(|c| matches!(c.datatype.as_str(), "json" | "jsonb"));
                        let prepare = if table {
                            crate::value::prepare_table
                        } else {
                            crate::value::prepare
                        };
                        let prepared = prepare(
                            value.as_ref(),
                            DisplayOptions {
                                format: ValueFormat::Auto,
                                ..options
                            },
                            declared,
                        );
                        let raw =
                            if matches!(prepared.format, ValueFormat::Hex | ValueFormat::Binary) {
                                crate::value::byte_chunk(
                                    value.as_ref().map_or(&[], Value::bytes),
                                    prepared.format,
                                    0,
                                )
                            } else if value.is_none() {
                                "NULL".into()
                            } else {
                                prepared.text
                            };
                        let (preview, limited) = crate::value::preview_prefix(&raw, budget);
                        self.previews_limited |= limited;
                        budget = budget.saturating_sub(preview.len());
                        preview
                    })
                    .collect()
            })
            .collect();
    }
}

fn projections(page: &Page) -> Result<Vec<Vec<Option<String>>>, &'static str> {
    let mut bytes = 0;
    page.rows
        .iter()
        .map(|row| {
            row.cells
                .iter()
                .map(|cell| {
                    let text = cell.as_ref().map(crate::value::projection).transpose()?;
                    bytes += text.as_ref().map_or(0, String::len);
                    if bytes > PAGE_BYTES {
                        return Err("Page previews exceed 1 MiB; current page retained");
                    }
                    Ok(text)
                })
                .collect()
        })
        .collect()
}

pub struct App {
    pub connection_form: Option<crate::connection::Form>,
    pub(crate) providers: Vec<&'static onetui_core::provider::ProviderDescriptor>,
    pub following: bool,
    pub(crate) follow_due: Option<tokio::time::Instant>,
    pub query_editor: Option<crate::query::Editor>,
    query_history: VecDeque<(String, String)>,
    history_path: Option<PathBuf>,
    pub(crate) history_menu: Option<usize>,
    history_position: Option<usize>,
    history_current: Option<String>,
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
    pub row_detail: bool,
    pub(crate) row_value: Option<crate::value::Prepared>,
    pub(crate) row_scroll: usize,
    pub detail_text: String,
    pub detail_chunk: usize,
    pub detail_chunks: usize,
    pub detail_scroll: u16,
    pub viewport: ratatui::layout::Rect,
    pub command: Option<String>,
    pub filter_input: Option<String>,
    filter_restore: Option<(String, Option<usize>)>,
    pub theme_menu: Option<Theme>,
    pub display_menu: Option<usize>,
    pub display_reasons: [&'static str; FORMATS.len()],
    pub horizontal_scroll: u16,
    pub detail_format: ValueFormat,
    detail_override: Option<ValueFormat>,
    detail_prepared: String,
    detail_ranges: Vec<std::ops::Range<usize>>,
    pub detail_notice: &'static str,
    pub confirm_quit: bool,
    pub quit: bool,
}

impl App {
    pub fn new(config: Config, alias: Option<&str>) -> Self {
        let (history_path, query_history, history_error) = match crate::history::path(&config) {
            Some(path) => match crate::history::load(&path) {
                Ok(entries) => (Some(path), entries, None),
                Err(error) => (
                    None,
                    VecDeque::new(),
                    Some(format!(
                        "Could not load query history: {}",
                        display(&error.to_string())
                    )),
                ),
            },
            None => (None, VecDeque::new(), None),
        };
        let mut app = Self {
            connection_form: None,
            providers: Vec::new(),
            following: false,
            follow_due: None,
            query_editor: None,
            query_history,
            history_path,
            history_menu: None,
            history_position: None,
            history_current: None,
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
            row_detail: false,
            row_value: None,
            row_scroll: 0,
            detail_text: String::new(),
            detail_chunk: 0,
            detail_chunks: 0,
            detail_scroll: 0,
            viewport: ratatui::layout::Rect::new(0, 0, 80, 24),
            command: None,
            filter_input: None,
            filter_restore: None,
            theme_menu: None,
            display_menu: None,
            display_reasons: [""; FORMATS.len()],
            horizontal_scroll: 0,
            detail_format: ValueFormat::Text,
            detail_override: None,
            detail_prepared: String::new(),
            detail_ranges: Vec::new(),
            detail_notice: "",
            confirm_quit: false,
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
        if app.error.is_none() {
            app.error = history_error;
        }
        app
    }

    fn invalidate(&mut self) {
        self.following = false;
        self.follow_due = None;
        self.generation += 1;
        self.request = None;
        self.loading = false;
        self.error = None;
    }

    fn connections(&mut self) {
        self.query_editor = None;
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
                    Some(alias.into()),
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
        self.view.rebuild(None, self.config.display);
        self.help = false;
        self.detail = false;
        self.row_detail = false;
        self.filter_input = None;
        self.filter_restore = None;
        self.detail_text.clear();
        self.clear_detail();
    }

    pub(crate) fn save_connection<P: onetui_core::provider::Provider>(&mut self, catalog: &[P]) {
        let Some(form) = self.connection_form.as_mut().filter(|form| form.save) else {
            return;
        };
        form.save = false;
        let alias = form.inputs[0].text.trim().to_owned();
        let result = form.options().and_then(|options| {
            self.config
                .add_connection(&alias, form.provider().kind, options, catalog)
        });
        match result {
            Ok(()) => {
                self.connection_form = None;
                self.connections();
                self.view.selected = self
                    .config
                    .aliases()
                    .iter()
                    .position(|(name, _)| *name == alias)
                    .unwrap_or(0);
            }
            Err(error) => self.error = Some(onetui_core::display(&error.to_string())),
        }
    }

    pub(crate) fn paste(&mut self, text: &str) {
        if self.confirm_quit {
            return;
        }
        if let Some(form) = &mut self.connection_form {
            self.error = form.insert(text).err().map(|error| error.to_string());
        } else if !self.loading
            && let Some(editor) = &mut self.query_editor
            && let Err(error) = editor.insert(text)
        {
            self.error = Some(error.into());
        }
    }

    fn load(&mut self, offset: i64, reset: bool) {
        self.invalidate();
        if self.row_detail {
            self.view.prepare_previews(self.config.display, true);
        }
        self.row_detail = false;
        self.row_value = None;
        self.row_scroll = 0;
        self.loading = true;
        self.request = Some(Request {
            follow: false,
            query: self.view.query.clone(),
            id: self.generation,
            alias: self.view.alias.clone().expect("datasource view has alias"),
            resource: self.view.resource.clone(),
            offset,
            continuation: if reset {
                None
            } else if offset < self.view.offset {
                self.view
                    .previous
                    .back()
                    .expect("previous page bookmark")
                    .position
                    .clone()
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
        if request.follow {
            self.complete_follow(request, result);
            return;
        }
        let result = result.and_then(|page| {
            anyhow::ensure!(
                page.bytes() <= PAGE_BYTES,
                "Page exceeds the 1 MiB value limit; current page retained"
            );
            projections(&page).map_err(anyhow::Error::msg)?;
            Ok(page)
        });
        match result {
            Ok(page) if page.bytes() <= PAGE_BYTES => {
                if request.query != self.view.query {
                    let mut view = View::new(Some(request.alias.clone()), request.resource.clone());
                    view.query = request.query.clone();
                    view.query_draft = request.query.clone();
                    let parent = std::mem::replace(&mut self.view, view);
                    if parent.query.is_none() {
                        self.parents.push(parent);
                    }
                }
                self.query_editor = None;
                self.view.live = false;
                self.view.live_evicted = 0;
                if self.view.sort.is_some_and(|(column, _)| {
                    self.view.page.columns.get(column).map(|c| &c.name)
                        != page.columns.get(column).map(|c| &c.name)
                }) {
                    self.view.sort = None;
                }
                let mut selected = None;
                if request.reset {
                    self.view.previous.clear();
                } else if request.offset < self.view.offset {
                    selected = self
                        .view
                        .previous
                        .pop_back()
                        .expect("previous page bookmark")
                        .selected;
                } else {
                    self.view.previous.push_back(PageBookmark {
                        offset: self.view.offset,
                        position: self.view.position.take(),
                        selected: self.view.selected_index(),
                        page: Some(std::mem::take(&mut self.view.page)),
                    });
                    let count = self.view.previous.len();
                    if count > 2 {
                        self.view.previous[count - 3].page = None;
                    }
                }
                self.view.position = request.continuation.clone();
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
                self.view.rebuild(selected, self.config.display);
                self.error = None;
                if self.single_value() && self.view.query.is_none() && self.filter_input.is_none() {
                    self.detail = true;
                    self.prepare_detail();
                }
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
        self.available_while_loading(action, self.loading && !self.following)
    }

    fn available_while_loading(&self, action: Action, loading: bool) -> bool {
        if self.confirm_quit {
            return matches!(action, Action::Open | Action::Back | Action::Cancel);
        }
        if self.connection_form.is_some() {
            return matches!(action, Action::Back | Action::Cancel);
        }
        if self.query_editor.is_some() {
            return matches!(action, Action::Back | Action::Cancel);
        }
        if self.history_menu.is_some() {
            return matches!(
                action,
                Action::Up | Action::Down | Action::Open | Action::Back | Action::Cancel
            );
        }
        if self.display_menu.is_some() {
            return matches!(
                action,
                Action::Up
                    | Action::Down
                    | Action::Open
                    | Action::Back
                    | Action::Cancel
                    | Action::Help
            );
        }
        if self.theme_menu.is_some() {
            return matches!(
                action,
                Action::Up | Action::Down | Action::Open | Action::Back | Action::Cancel
            );
        }
        match action {
            Action::Add => {
                !self.help
                    && !self.detail
                    && !self.row_detail
                    && self.view.alias.is_none()
                    && self.config.path().is_some()
                    && !self.providers.is_empty()
            }
            Action::Follow => {
                !loading
                    && !self.help
                    && !self.detail
                    && !self.row_detail
                    && self.view.query.is_none()
                    && self
                        .view
                        .alias
                        .as_deref()
                        .and_then(|alias| self.config.descriptor(alias))
                        .is_some_and(|provider| {
                            provider.follow_resources.contains(&self.view.resource.id)
                        })
            }
            Action::Query => !loading && self.query_target().is_some(),
            Action::History => {
                !loading
                    && !self.help
                    && !self.detail
                    && !self.row_detail
                    && self.query_target().is_some()
            }
            Action::ScrollLeft | Action::ScrollRight => !self.config.display.word_wrap,
            Action::Filter => !self.detail && !self.row_detail,
            Action::Sort => !self.detail && !self.row_detail && self.column_count() > 0,
            Action::Columns => {
                !loading && !self.detail && !self.row_detail && self.action_target(action).is_some()
            }
            Action::Left | Action::Right => self.column_count() > 0,
            Action::Back => self.help || self.detail || self.row_detail || !self.parents.is_empty(),
            Action::Open => !loading && !self.view.visible.is_empty(),
            Action::Next => {
                if self.detail {
                    self.detail_chunk + 1 < self.detail_chunks
                } else {
                    !self.view.live
                        && !self.following
                        && !self.row_detail
                        && !loading
                        && self.view.page.next
                }
            }
            Action::Previous => {
                if self.detail {
                    self.detail_chunk > 0
                } else {
                    !self.view.live
                        && !self.following
                        && !self.row_detail
                        && !loading
                        && !self.view.previous.is_empty()
                }
            }
            Action::Up
            | Action::Down
            | Action::PageUp
            | Action::PageDown
            | Action::HalfPageUp
            | Action::HalfPageDown => self.help || !self.view.visible.is_empty(),
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

    pub fn query_descriptor(&self) -> Option<onetui_core::provider::QueryDescriptor> {
        self.config.descriptor(self.view.alias.as_deref()?)?.query
    }

    fn query_target(&self) -> Option<Resource> {
        let descriptor = self.query_descriptor()?;
        let resource = if descriptor.accepts(&self.view.resource) {
            &self.view.resource
        } else {
            self.view
                .page
                .rows
                .get(self.view.selected_index()?)?
                .target
                .as_ref()?
        };
        if !descriptor.accepts(resource) {
            return None;
        }
        Some(Resource::new(
            descriptor.resource,
            resource.path.get(..descriptor.path_depth)?.to_vec(),
        ))
    }

    fn close_query(&mut self) {
        if let Some(editor) = self.query_editor.take() {
            self.view.query_draft = Some(editor.text);
        }
        self.history_position = None;
        self.history_current = None;
        self.invalidate();
    }

    pub(crate) fn history_entries(&self) -> impl Iterator<Item = &str> {
        let alias = self.view.alias.as_deref();
        self.query_history
            .iter()
            .rev()
            .filter(move |(connection, _)| Some(connection.as_str()) == alias)
            .map(|(_, text)| text.as_str())
    }

    fn browse_query_history(&mut self, previous: bool) {
        let alias = self.view.alias.as_deref().expect("query connection");
        let position = if previous {
            let end = self.history_position.unwrap_or(self.query_history.len());
            self.query_history
                .iter()
                .take(end)
                .rposition(|(connection, _)| connection == alias)
        } else {
            self.history_position.and_then(|current| {
                self.query_history
                    .iter()
                    .enumerate()
                    .skip(current + 1)
                    .find(|(_, (connection, _))| connection == alias)
                    .map(|(index, _)| index)
            })
        };
        if position.is_none() && (previous || self.history_position.is_none()) {
            return;
        }
        let editor = self.query_editor.as_mut().expect("query editor");
        if self.history_position.is_none() && position.is_some() {
            self.history_current = Some(editor.text.clone());
        }
        let text = position
            .map(|index| self.query_history[index].1.clone())
            .unwrap_or_else(|| {
                self.history_current
                    .take()
                    .unwrap_or_else(|| editor.text.clone())
            });
        *editor = crate::query::Editor::new(text);
        self.history_position = position;
    }

    fn execute_query(&mut self) {
        let text = self
            .query_editor
            .as_ref()
            .expect("query editor")
            .text
            .clone();
        if text.trim().is_empty() {
            self.error = Some("Query is empty".into());
            return;
        }
        let resource = self.query_target().expect("query scope");
        let alias = self.view.alias.as_ref().expect("query connection");
        let recorded = self
            .query_history
            .back()
            .is_none_or(|entry| entry.0 != *alias || entry.1 != text);
        if recorded {
            if self.query_history.len() == crate::history::LIMIT {
                self.query_history.pop_front();
            }
            self.query_history.push_back((alias.clone(), text.clone()));
        }
        let history_error = if recorded {
            self.history_path
                .as_deref()
                .and_then(|path| crate::history::save(path, &self.query_history).err())
        } else {
            None
        };
        self.history_position = None;
        self.history_current = None;
        self.view.query_draft = Some(text.clone());
        self.load(0, true);
        let request = self.request.as_mut().expect("queued query");
        request.query = Some(text);
        request.resource = resource;
        if let Some(error) = history_error {
            self.error = Some(format!(
                "Could not save query history: {}",
                display(&error.to_string())
            ));
        }
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

    fn clear_detail(&mut self) {
        self.row_value = None;
        self.row_scroll = 0;
        self.detail_override = None;
        self.detail_prepared.clear();
        self.detail_ranges.clear();
        self.horizontal_scroll = 0;
    }

    fn single_value(&self) -> bool {
        self.view.alias.is_some()
            && self.column_count() == 1
            && self.view.page.rows.len() == 1
            && !self.view.page.next
            && self.view.page.rows[0].target.is_none()
            && !self.view.visible.is_empty()
    }

    fn prepare_row_value(&mut self) {
        let cell = self.view.page.rows[self.view.selected_index().expect("selected row")].cells
            [self.view.column]
            .as_ref();
        let declared = self
            .view
            .page
            .columns
            .get(self.view.column)
            .is_some_and(|c| matches!(c.datatype.as_str(), "json" | "jsonb"));
        let mut prepared = crate::value::prepare(
            cell,
            DisplayOptions {
                format: ValueFormat::Auto,
                ..self.config.display
            },
            declared,
        );
        if cell.is_none() {
            prepared.text = "NULL".into();
        }
        self.row_value = Some(prepared);
        self.row_scroll = 0;
    }

    fn prepare_detail(&mut self) {
        let cell = self.view.page.rows[self.view.selected_index().expect("selected detail row")]
            .cells[self.view.column]
            .as_ref();
        let mut options = self.config.display;
        options.format = self.detail_override.unwrap_or(options.format);
        let declared_json = self
            .view
            .page
            .columns
            .get(self.view.column)
            .is_some_and(|c| matches!(c.datatype.as_str(), "json" | "jsonb"));
        let prepared = crate::value::prepare(cell, options, declared_json);
        self.detail_format = prepared.format;
        self.detail_notice = prepared.notice;
        self.detail_prepared = prepared.text;
        self.detail_ranges = crate::value::chunk_ranges(&self.detail_prepared);
        self.detail_chunk = 0;
        self.horizontal_scroll = 0;
        self.detail_text();
    }

    fn detail_text(&mut self) {
        let cell = &self.view.page.rows[self.view.selected_index().expect("selected detail row")]
            .cells[self.view.column];
        let bytes = cell.as_ref().map_or(&[][..], Value::bytes);
        let binary = matches!(self.detail_format, ValueFormat::Hex | ValueFormat::Binary);
        self.detail_chunks = if binary {
            bytes.len().div_ceil(crate::value::BYTE_CHUNK).max(1)
        } else {
            self.detail_ranges.len().max(1)
        };
        self.detail_chunk = self.detail_chunk.min(self.detail_chunks - 1);
        let datatype = self
            .view
            .page
            .columns
            .get(self.view.column)
            .map_or("metadata", |c| c.datatype.as_str());
        self.detail_text = format!(
            "{} | {} | {} | {} | {}\n{}",
            self.column_name(self.view.column),
            datatype,
            self.detail_format.name(),
            cell.as_ref().map_or("no bytes", Value::provenance),
            match cell {
                None => "SQL NULL",
                Some(Value::Bytes(v)) if v.is_empty() => "empty bytes",
                Some(Value::Bytes(_)) => "non-null bytes",
                Some(v) if v.bytes().is_empty() => "empty text",
                Some(_) => "non-null text",
            },
            if binary {
                crate::value::byte_chunk(bytes, self.detail_format, self.detail_chunk)
            } else {
                self.detail_ranges
                    .get(self.detail_chunk)
                    .map_or("", |range| &self.detail_prepared[range.clone()])
                    .to_owned()
            }
        );
        self.detail_scroll = 0;
    }

    pub fn actions(&self) -> impl Iterator<Item = &'static ActionDescriptor> + '_ {
        ACTIONS.iter().filter(|entry| self.available(entry.id))
    }

    pub fn context_actions(&self) -> impl Iterator<Item = &'static ActionDescriptor> + '_ {
        ACTIONS
            .iter()
            .filter(|entry| {
                matches!(entry.id, Action::Next | Action::Previous)
                    || self.available_while_loading(entry.id, false)
            })
            .filter(|entry| entry.id != Action::History)
    }

    pub fn act(&mut self, action: Action) {
        if self.confirm_quit {
            match action {
                Action::Open => {
                    self.invalidate();
                    self.quit = true;
                }
                Action::Back | Action::Cancel => self.confirm_quit = false,
                _ => {}
            }
            return;
        }
        if self.connection_form.is_some() {
            if matches!(action, Action::Back | Action::Cancel) {
                self.connection_form = None;
                self.error = None;
            }
            return;
        }
        if self.query_editor.is_some() && matches!(action, Action::Back | Action::Cancel) {
            self.close_query();
            return;
        }
        if !self.available(action) {
            return;
        }
        if let Some(index) = self.history_menu {
            match action {
                Action::Up => self.history_menu = Some(index.saturating_sub(1)),
                Action::Down => {
                    self.history_menu =
                        Some((index + 1).min(self.history_entries().count().saturating_sub(1)))
                }
                Action::Open => {
                    let text = self
                        .history_entries()
                        .nth(index)
                        .expect("history entry")
                        .to_owned();
                    self.history_menu = None;
                    self.act(Action::Query);
                    self.query_editor = Some(crate::query::Editor::new(text));
                }
                Action::Back | Action::Cancel => self.history_menu = None,
                _ => {}
            }
            return;
        }
        if self.following && action != Action::Quit {
            self.invalidate();
            if matches!(action, Action::Follow | Action::Cancel) {
                return;
            }
        }
        if let Some(index) = self.display_menu {
            if self.help && matches!(action, Action::Up | Action::Down) {
                self.detail_scroll = if action == Action::Up {
                    self.detail_scroll.saturating_sub(1)
                } else {
                    self.detail_scroll.saturating_add(1)
                };
                return;
            }
            match action {
                Action::Up => self.display_menu = Some(index.saturating_sub(1)),
                Action::Down => self.display_menu = Some((index + 1).min(FORMATS.len() + 3)),
                Action::Back | Action::Cancel => {
                    self.display_menu = None;
                    self.help = false;
                }
                Action::Help => {
                    self.help = !self.help;
                    self.detail_scroll = 0;
                }
                Action::Open => {
                    let before = self.config.display;
                    let format_before = self.detail_override;
                    if index < FORMATS.len() {
                        if !self.detail {
                            self.error =
                                Some("Open a field detail before selecting its format".into());
                            return;
                        }
                        self.detail_override = Some(FORMATS[index].id);
                    } else {
                        match index - FORMATS.len() {
                            0 => {
                                self.config.display.pretty_print = !self.config.display.pretty_print
                            }
                            1 => self.config.display.highlight = !self.config.display.highlight,
                            2 => self.config.display.word_wrap = !self.config.display.word_wrap,
                            _ => {
                                self.config.display.unicode =
                                    if self.config.display.unicode == UnicodeDisplay::Literal {
                                        UnicodeDisplay::Escaped
                                    } else {
                                        UnicodeDisplay::Literal
                                    }
                            }
                        }
                    }
                    self.display_changed(before, format_before);
                }
                _ => {}
            }
            return;
        }
        if let Some(original) = self.theme_menu {
            match action {
                Action::Up | Action::Down => {
                    let index = self.theme_index();
                    let next = if action == Action::Up {
                        (index + Theme::ALL.len() - 1) % Theme::ALL.len()
                    } else {
                        (index + 1) % Theme::ALL.len()
                    };
                    self.config.theme = Theme::ALL[next];
                }
                Action::Open => self.theme_menu = None,
                Action::Back | Action::Cancel => {
                    self.config.theme = original;
                    self.theme_menu = None;
                }
                _ => {}
            }
            return;
        }
        if self.help && matches!(action, Action::Up | Action::Down) {
            self.detail_scroll = if action == Action::Up {
                self.detail_scroll.saturating_sub(1)
            } else {
                self.detail_scroll.saturating_add(1)
            };
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
        if self.row_detail && !self.detail && matches!(action, Action::Up | Action::Down) {
            self.act(if action == Action::Up {
                Action::Left
            } else {
                Action::Right
            });
            return;
        }
        match action {
            Action::Follow => self.start_follow(),
            Action::Query => {
                let descriptor = self.query_descriptor().expect("query provider");
                let text = self
                    .view
                    .query_draft
                    .clone()
                    .or_else(|| self.view.query.clone())
                    .unwrap_or_else(|| {
                        descriptor.initial_text(
                            &self.view.resource,
                            self.view
                                .selected_index()
                                .and_then(|index| self.view.page.rows.get(index)),
                        )
                    });
                self.query_editor = Some(crate::query::Editor::new(text));
                self.history_position = None;
                self.history_current = None;
                self.help = false;
                self.detail = false;
                if self.row_detail {
                    self.view.prepare_previews(self.config.display, true);
                }
                self.row_detail = false;
                self.row_value = None;
                self.row_scroll = 0;
            }
            Action::History => {
                if self.history_entries().next().is_some() {
                    self.history_menu = Some(0);
                } else {
                    self.error = Some("No queries in this session".into());
                }
            }
            Action::Display => {
                self.help = false;
                self.display_menu = Some(0);
                for (i, format) in FORMATS.iter().enumerate() {
                    self.display_reasons[i] = if !self.detail {
                        "Open field detail to select a format"
                    } else {
                        let cell = self.view.page.rows[self.view.selected_index().unwrap()].cells
                            [self.view.column]
                            .as_ref();
                        if cell.is_none() {
                            "Null has no text or byte content"
                        } else if format.id == ValueFormat::Auto {
                            format.description
                        } else {
                            let prepared = crate::value::prepare(
                                cell,
                                DisplayOptions {
                                    format: format.id,
                                    ..self.config.display
                                },
                                false,
                            );
                            if prepared.format != format.id {
                                prepared.notice
                            } else {
                                format.description
                            }
                        }
                    };
                }
            }
            Action::ScrollLeft => self.horizontal_scroll = self.horizontal_scroll.saturating_sub(8),
            Action::ScrollRight => {
                self.horizontal_scroll = self.horizontal_scroll.saturating_add(8)
            }
            Action::Add => {
                self.error = None;
                self.connection_form = Some(crate::connection::Form::new(self.providers.clone()));
            }
            Action::Themes => self.theme_menu = Some(self.config.theme),
            Action::Filter => {
                self.help = false;
                self.filter_restore = Some((self.view.filter.clone(), self.view.selected_index()));
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
                self.view.reindex(selected);
            }
            Action::Left | Action::Right => {
                let previous = self.view.column;
                self.view.column = if action == Action::Left {
                    self.view.column.saturating_sub(1)
                } else {
                    (self.view.column + 1).min(self.column_count().saturating_sub(1))
                };
                if self.detail {
                    self.prepare_detail();
                } else if self.row_detail && previous != self.view.column {
                    self.prepare_row_value();
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
            Action::PageUp | Action::PageDown | Action::HalfPageUp | Action::HalfPageDown => {
                let down = matches!(action, Action::PageDown | Action::HalfPageDown);
                let half = matches!(action, Action::HalfPageUp | Action::HalfPageDown);
                if self.row_detail && !self.detail && !self.help {
                    let (height, lines) = crate::ui::row_value_extent(self);
                    if lines > height {
                        let step = if half { height / 2 } else { height }.max(1);
                        self.row_scroll = if down {
                            self.row_scroll.saturating_add(step).min(lines - height)
                        } else {
                            self.row_scroll.saturating_sub(step).min(lines - height)
                        };
                        return;
                    }
                }
                for _ in 0..crate::ui::page_step(self, down, half) {
                    self.act(if down { Action::Down } else { Action::Up });
                }
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
                    if self.row_detail || self.column_count() == 1 {
                        self.detail = true;
                        self.prepare_detail();
                    } else {
                        self.row_detail = true;
                        self.view.prepare_previews(self.config.display, false);
                        self.horizontal_scroll = 0;
                        self.prepare_row_value();
                    }
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
                    self.clear_detail();
                    if self.row_detail {
                        self.prepare_row_value();
                    }
                    if !self.row_detail && self.single_value() {
                        self.act(Action::Back);
                    }
                } else if self.row_detail {
                    self.row_detail = false;
                    self.view.prepare_previews(self.config.display, true);
                    self.horizontal_scroll = 0;
                    self.row_value = None;
                    self.row_scroll = 0;
                } else if let Some(parent) = self.parents.pop() {
                    self.invalidate();
                    self.view = parent;
                    self.view.prepare_previews(self.config.display, true);
                    if self.view.alias.is_none() {
                        self.session += 1;
                        self.connection_status = None;
                    }
                }
            }
            Action::Connections => self.connections(),
            Action::Next => {
                self.detail = false;
                let bookmark_bytes = self
                    .view
                    .previous
                    .iter()
                    .map(|bookmark| bookmark.position.as_ref().map_or(0, String::len))
                    .sum::<usize>()
                    + self.view.position.as_ref().map_or(0, String::len);
                if self.view.previous.len() >= BOOKMARK_LIMIT || bookmark_bytes > PAGE_BYTES {
                    self.error = Some("Page bookmark limit reached (4096 bookmarks / 1 MiB tokens); go back or refresh to restart".into());
                    return;
                }
                if let Some(offset) = self.view.offset.checked_add(PAGE_SIZE) {
                    self.load(offset, false);
                }
            }
            Action::Previous => {
                self.detail = false;
                if self
                    .view
                    .previous
                    .back()
                    .is_some_and(|bookmark| bookmark.page.is_some())
                {
                    let bookmark = self.view.previous.pop_back().expect("cached previous page");
                    self.invalidate();
                    self.view.offset = bookmark.offset;
                    self.view.position = bookmark.position;
                    self.view.page = bookmark.page.expect("cached previous page");
                    self.view.rebuild(bookmark.selected, self.config.display);
                } else if let Some(bookmark) = self.view.previous.back() {
                    self.load(bookmark.offset, false);
                }
            }
            Action::Refresh => {
                self.detail = false;
                self.clear_detail();
                if self.view.resource == Resource::new("connections", vec![]) {
                    let filter = std::mem::take(&mut self.view.filter);
                    let sort = self.view.sort;
                    self.connections();
                    self.view.filter = filter;
                    self.view.sort = sort;
                    self.view.rebuild(None, self.config.display);
                } else {
                    self.load(0, true);
                }
            }
            Action::Help => {
                self.help = !self.help;
                self.detail_scroll = 0;
                self.horizontal_scroll = 0;
            }
            Action::Quit => {
                self.confirm_quit = true;
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
        if self.confirm_quit {
            match key.code {
                KeyCode::Enter | KeyCode::Char('y' | 'Y') => self.act(Action::Open),
                KeyCode::Esc | KeyCode::Char('n' | 'N') => self.act(Action::Back),
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.act(Action::Cancel)
                }
                _ => {}
            }
            return;
        }
        if let Some(form) = &mut self.connection_form {
            if key.code == KeyCode::Esc
                || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
            {
                self.act(Action::Cancel);
            } else if let Err(error) = form.key(key) {
                self.error = Some(error.to_string());
            } else {
                self.error = None;
            }
            return;
        }
        if self.query_editor.is_some() {
            match key.code {
                KeyCode::Esc => self.close_query(),
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.close_query()
                }
                KeyCode::F(5) if !self.loading => self.execute_query(),
                KeyCode::Enter if !self.loading && key.modifiers.is_empty() => self.execute_query(),
                KeyCode::Char('r')
                    if !self.loading && key.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    self.execute_query()
                }
                KeyCode::Char('p')
                    if !self.loading && key.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    self.browse_query_history(true)
                }
                KeyCode::Char('n')
                    if !self.loading && key.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    self.browse_query_history(false)
                }
                _ if self.loading => {}
                _ => {
                    if let Err(error) = self.query_editor.as_mut().unwrap().key(key) {
                        self.error = Some(error.into());
                    }
                }
            }
            return;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.command = None;
            self.restore_filter();
            self.act(Action::Cancel);
            return;
        }
        if let Some(input) = &mut self.filter_input {
            match key.code {
                KeyCode::Esc => {
                    self.restore_filter();
                    return;
                }
                KeyCode::Enter => {
                    self.filter_input = None;
                    self.filter_restore = None;
                    return;
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
            self.view.filter = self.filter_input.as_ref().unwrap().clone();
            self.view.reindex(
                self.filter_restore
                    .as_ref()
                    .and_then(|(_, selected)| *selected),
            );
            return;
        }
        if let Some(command) = &mut self.command {
            match key.code {
                KeyCode::Esc => self.command = None,
                KeyCode::Enter => {
                    let command = self.command.take().unwrap();
                    if command.starts_with("display ") {
                        self.display_command(&command);
                        return;
                    }
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
                    if (c.is_ascii_lowercase() || matches!(c, ' ' | '-' | '_'))
                        && command.len() < 64
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
        if key.modifiers.contains(KeyModifiers::ALT) {
            return;
        }
        if key.code == KeyCode::Char(':')
            && !key.modifiers.contains(KeyModifiers::CONTROL)
            && self.theme_menu.is_none()
            && self.display_menu.is_none()
            && self.history_menu.is_none()
        {
            if self.following {
                self.invalidate();
            }
            self.command = Some(String::new());
            return;
        }
        let name = match key.code {
            KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => {
                format!("Ctrl-{c}")
            }
            _ if key.modifiers.contains(KeyModifiers::CONTROL) => return,
            KeyCode::Char(c) => c.to_string(),
            KeyCode::PageUp => "PageUp".into(),
            KeyCode::PageDown => "PageDown".into(),
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
            .find(|entry| entry.keys.contains(&name.as_str()) && self.available(entry.id))
            .map(|entry| entry.id)
        {
            self.act(action);
        }
    }

    pub fn theme_index(&self) -> usize {
        Theme::ALL
            .iter()
            .position(|theme| *theme == self.config.theme)
            .expect("built-in theme is listed")
    }

    fn restore_filter(&mut self) {
        self.filter_input = None;
        if let Some((filter, selected)) = self.filter_restore.take() {
            self.view.filter = filter;
            self.view.reindex(selected);
        }
    }

    fn display_command(&mut self, command: &str) {
        let before = self.config.display;
        let format_before = self.detail_override;
        let args: Vec<_> = command.split_whitespace().collect();
        let valid = match args.as_slice() {
            ["display", "format", name] if self.detail => {
                if let Some(format) = FORMATS.iter().find(|f| f.name == *name) {
                    self.detail_override = Some(format.id);
                    true
                } else {
                    false
                }
            }
            ["display", "unicode", name @ ("literal" | "escaped")] => {
                self.config.display.unicode = if *name == "literal" {
                    UnicodeDisplay::Literal
                } else {
                    UnicodeDisplay::Escaped
                };
                true
            }
            ["display", setting, value @ ("on" | "off")] => {
                let target = match *setting {
                    "pretty-print" => Some(&mut self.config.display.pretty_print),
                    "highlight" => Some(&mut self.config.display.highlight),
                    "word-wrap" => Some(&mut self.config.display.word_wrap),
                    _ => None,
                };
                if let Some(target) = target {
                    *target = *value == "on";
                    true
                } else {
                    false
                }
            }
            _ => false,
        };
        if valid {
            self.error = None;
            self.display_changed(before, format_before);
        } else {
            self.error = Some(
                "Invalid display command; use :display. Format selection requires field detail"
                    .into(),
            );
        }
    }

    fn display_changed(&mut self, before: DisplayOptions, format_before: Option<ValueFormat>) {
        let content_changed = before.pretty_print != self.config.display.pretty_print
            || before.unicode != self.config.display.unicode;
        if self.detail && (content_changed || format_before != self.detail_override) {
            self.prepare_detail();
        }
        if content_changed {
            self.view
                .prepare_previews(self.config.display, !self.row_detail);
            if self.row_detail {
                self.prepare_row_value();
            }
        }
        if before.word_wrap != self.config.display.word_wrap {
            self.row_scroll = 0;
        }
        if self.config.display.word_wrap {
            self.horizontal_scroll = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn command(app: &mut App, command: &str) {
        for c in format!(":{command}").chars() {
            app.key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    }

    #[test]
    fn add_connection_saves_without_connecting_and_cancel_leaves_no_file() {
        use crate::test_provider::CATALOG;
        use onetui_core::provider::Provider;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let mut app = App::new(
            Config::load_for_startup(&path, CATALOG, true).unwrap(),
            None,
        );
        app.providers = CATALOG.iter().map(|p| p.descriptor()).collect();
        command(&mut app, "add");
        assert!(app.connection_form.is_some());
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        app.paste("discarded");
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(!path.exists());
        assert!(app.view.page.rows.is_empty());
        app.key(KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE));
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        app.key(KeyEvent::new(KeyCode::F(2), KeyModifiers::NONE));
        app.save_connection(CATALOG);
        assert!(app.error.as_ref().unwrap().contains("Alias"));
        assert!(app.connection_form.is_some());
        assert!(!path.exists());
        app.paste("my_connection");
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        app.paste("UNSET_SECRET");
        app.key(KeyEvent::new(KeyCode::F(2), KeyModifiers::NONE));
        app.save_connection(CATALOG);
        assert!(app.error.is_none(), "{:?}", app.error);
        assert!(app.connection_form.is_none());
        assert_eq!(app.config.aliases()[0].0, "my_connection");
        assert_eq!(app.view.page.rows.len(), 1);
        assert!(app.request.is_none());
        assert!(app.view.alias.is_none());
        assert!(
            app.config
                .configure("my_connection", CATALOG, &|_| None)
                .is_ok()
        );
        app.act(Action::Open);
        assert!(app.request.is_some());
    }

    #[test]
    fn query_enter_executes_and_shift_enter_only_inserts_newline() {
        let mut app = app();
        let browse = app.request.take().unwrap();
        app.complete(&browse, Ok(page(false)));
        app.act(Action::Query);
        app.query_editor = Some(crate::query::Editor::new("SELECT 1".into()));
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT));
        assert_eq!(app.query_editor.as_ref().unwrap().text, "SELECT 1\n");
        assert!(app.request.is_none());
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let request = app.request.take().expect("Enter executes");
        assert_eq!(request.query.as_deref(), Some("SELECT 1\n"));
        for key in [
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
            KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT),
            KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE),
        ] {
            app.key(key);
            assert!(
                app.request.is_none(),
                "loading must not submit another request"
            );
            assert_eq!(app.query_editor.as_ref().unwrap().text, "SELECT 1\n");
        }
        app.complete(&request, Err(anyhow::anyhow!("native query error")));
        app.key(KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE));
        let retry = app.request.take().expect("F5 executes");
        assert_eq!(retry.query, request.query);
    }

    #[test]
    fn query_history_does_not_touch_disk_without_opt_in() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        let history_path = config_path.with_extension("history.json");
        std::fs::write(&config_path, "[connections.pg]\nkind='fake'").unwrap();
        let open = || {
            App::new(
                Config::load(&config_path, crate::test_provider::CATALOG).unwrap(),
                Some("pg"),
            )
        };
        let mut app = open();
        let browse = app.request.take().unwrap();
        app.complete(&browse, Ok(page(false)));
        app.act(Action::Query);
        app.query_editor = Some(crate::query::Editor::new("SELECT 'secret'".into()));
        app.key(KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE));
        assert_eq!(
            app.history_entries().collect::<Vec<_>>(),
            ["SELECT 'secret'"]
        );
        drop(app);
        assert!(!history_path.exists());

        let old_history = serde_json::to_vec(&[("pg", "SELECT 'old secret'")]).unwrap();
        std::fs::write(&history_path, &old_history).unwrap();
        let reopened = open();
        assert_eq!(reopened.history_entries().count(), 0);
        assert_eq!(std::fs::read(&history_path).unwrap(), old_history);
    }

    #[test]
    fn query_history_survives_restart_and_stays_scoped_to_connection() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            "persist_query_history = true\n[connections.pg]\nkind='fake'\nurl_env='NEVER_RESOLVE_THIS'\n[connections.q]\nkind='checkonly'\nurl='http://localhost:6334'",
        )
        .unwrap();
        let open = || {
            App::new(
                Config::load(&config_path, crate::test_provider::CATALOG).unwrap(),
                Some("pg"),
            )
        };
        let mut app = open();
        let browse = app.request.take().unwrap();
        app.complete(&browse, Ok(page(false)));
        app.act(Action::Query);
        for text in ["SELECT 1", "SELECT 2"] {
            app.query_editor = Some(crate::query::Editor::new(text.into()));
            app.key(KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE));
            let request = app.request.take().unwrap();
            app.complete(&request, Err(anyhow::anyhow!("query failed")));
        }
        drop(app);
        assert!(config_path.with_extension("history.json").is_file());

        let mut reopened = open();
        assert_eq!(
            reopened.history_entries().collect::<Vec<_>>(),
            ["SELECT 2", "SELECT 1"]
        );
        reopened.view.alias = Some("q".into());
        assert_eq!(reopened.history_entries().count(), 0);
        reopened.view.alias = Some("pg".into());
        let browse = reopened.request.take().unwrap();
        reopened.complete(&browse, Ok(page(false)));
        reopened.act(Action::Query);
        reopened.query_editor = Some(crate::query::Editor::new("SELECT 2".into()));
        reopened.key(KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE));
        assert_eq!(reopened.query_history.len(), 2);
    }

    #[test]
    fn query_history_recalls_submissions_for_the_current_connection() {
        let mut app = app();
        let browse = app.request.take().unwrap();
        app.complete(&browse, Ok(page(false)));
        app.act(Action::Query);
        for text in ["SELECT 1", "SELECT 2"] {
            app.query_editor = Some(crate::query::Editor::new(text.into()));
            app.key(KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE));
            let request = app.request.take().unwrap();
            app.complete(&request, Err(anyhow::anyhow!("query failed")));
        }
        assert_eq!(app.query_history.len(), 2);
        app.query_editor = Some(crate::query::Editor::new("current input".into()));
        let previous = KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL);
        let next = KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL);
        app.key(previous);
        assert_eq!(app.query_editor.as_ref().unwrap().text, "SELECT 2");
        app.key(previous);
        assert_eq!(app.query_editor.as_ref().unwrap().text, "SELECT 1");
        app.key(previous);
        assert_eq!(app.query_editor.as_ref().unwrap().text, "SELECT 1");
        app.key(next);
        assert_eq!(app.query_editor.as_ref().unwrap().text, "SELECT 2");
        app.key(next);
        assert_eq!(app.query_editor.as_ref().unwrap().text, "current input");

        app.view.alias = Some("q".into());
        app.key(previous);
        assert_eq!(app.query_editor.as_ref().unwrap().text, "current input");

        app.view.alias = Some("pg".into());
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        app.key(KeyEvent::new(KeyCode::Char('H'), KeyModifiers::SHIFT));
        assert_eq!(app.history_menu, Some(0));
        app.key(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE));
        assert_eq!(app.history_menu, Some(1));
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(app.history_menu.is_none());
        assert_eq!(app.query_editor.as_ref().unwrap().text, "SELECT 1");
        assert!(app.request.is_none());
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        command(&mut app, "history");
        assert_eq!(app.history_menu, Some(0));
    }

    #[test]
    fn queries_preserve_browsing_and_isolate_paging_errors_and_drafts() {
        let mut app = app();
        let browse = app.request.take().unwrap();
        app.complete(&browse, Ok(page(false)));
        let parent_resource = app.view.resource.clone();
        command(&mut app, "query");
        assert!(app.query_editor.is_some());
        app.query_editor = Some(crate::query::Editor::new("select value".into()));
        app.key(KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE));
        let request = app.request.take().unwrap();
        assert_eq!(request.query.as_deref(), Some("select value"));
        assert_eq!(request.continuation, None);
        app.complete(&request, Err(anyhow::anyhow!("native syntax error")));
        assert_eq!(app.view.resource, parent_resource);
        assert!(app.query_editor.is_some());
        assert!(
            app.error
                .as_deref()
                .unwrap()
                .contains("native syntax error")
        );
        app.key(KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE));
        let request = app.request.take().unwrap();
        let result = || Page {
            rows: vec![Row {
                cells: vec![Some("1".into()), Some("value".into())],
                target: None,
            }],
            columns: vec![
                onetui_core::Column {
                    name: "id".into(),
                    datatype: "int".into(),
                },
                onetui_core::Column {
                    name: "text".into(),
                    datatype: "text".into(),
                },
            ],
            continuation: Some("query-token".into()),
            next: true,
            ..Page::default()
        };
        app.complete(&request, Ok(result()));
        assert!(app.query_editor.is_none());
        assert_eq!(app.view.query.as_deref(), Some("select value"));
        app.act(Action::Next);
        let request = app.request.take().unwrap();
        assert_eq!(request.query, app.view.query);
        assert_eq!(request.continuation.as_deref(), Some("query-token"));
        app.complete(&request, Ok(result()));
        assert_eq!(app.view.previous.len(), 1);
        command(&mut app, "query");
        app.query_editor = Some(crate::query::Editor::new("select changed".into()));
        app.key(KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE));
        let request = app.request.take().unwrap();
        assert!(request.continuation.is_none());
        app.complete(&request, Ok(result()));
        assert!(app.view.previous.is_empty());
        assert_eq!(app.view.offset, 0);
        app.act(Action::Query);
        app.key(KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE));
        let cancelled = app.request.take().unwrap();
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        app.complete(&cancelled, Err(anyhow::anyhow!("late error")));
        assert!(app.error.is_none());
        assert!(!app.quit);
        app.act(Action::Back);
        assert_eq!(app.view.resource, parent_resource);
        assert!(app.view.query.is_none());
        app.act(Action::Connections);
        assert!(!app.available(Action::Query));
    }

    #[test]
    fn single_value_opens_without_a_truncated_table() {
        let mut app = app();
        let request = app.request.take().unwrap();
        app.complete(
            &request,
            Ok(Page {
                columns: vec![onetui_core::Column {
                    name: "payload".into(),
                    datatype: "json".into(),
                }],
                rows: vec![Row {
                    cells: vec![Some(Value::Json(
                        "{\"a\":1,\"b\":[2,3],\"last\":true}".into(),
                    ))],
                    target: None,
                }],
                ..Page::default()
            }),
        );
        assert!(app.detail, "a scalar result must use the value viewer");
        assert!(app.detail_text.contains("\"last\": true"));
        assert!(app.request.is_none());
    }

    #[test]
    fn row_opens_field_list_before_individual_value() {
        let mut app = app();
        let request = app.request.take().unwrap();
        app.complete(
            &request,
            Ok(Page {
                columns: ["name", "balance"]
                    .map(|name| onetui_core::Column {
                        name: name.into(),
                        datatype: "text".into(),
                    })
                    .to_vec(),
                rows: vec![Row {
                    cells: vec![Some("Mina".into()), Some("12.50".into())],
                    target: None,
                }],
                ..Page::default()
            }),
        );
        app.act(Action::Open);
        assert!(!app.detail, "first Enter should list every field");
        app.act(Action::Down);
        app.act(Action::Open);
        assert!(app.detail_text.contains("12.50"));
        app.act(Action::Back);
        assert!(!app.detail);
        assert_eq!(app.view.selected, 0);
        assert!(app.request.is_none());
    }

    #[test]
    fn filter_updates_while_typing_and_escape_restores_selection_without_reformatting() {
        let mut app = app();
        app.act(Action::Connections);
        app.view.selected = 1;
        let previews = app.view.previews.as_ptr();
        let projections = app.view.projections.as_ptr();
        app.act(Action::Filter);
        for c in "fake".chars() {
            app.key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        assert_eq!(app.view.visible, vec![0]);
        assert_eq!(app.view.previews.as_ptr(), previews);
        assert_eq!(app.view.projections.as_ptr(), projections);
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(app.view.visible, vec![0, 1]);
        assert_eq!(app.view.selected, 1);
        assert!(app.view.filter.is_empty());
        assert!(app.request.is_none());
    }

    #[test]
    fn display_commands_menu_and_formats_preserve_data_and_request_identity() {
        let mut app = app();
        app.view.resource = Resource::new("fake.rows", vec!["public".into(), "values".into()]);
        let request = app.request.take().unwrap();
        let json = "{\"name\":\"София 🌊\",\"a\":1,\"a\":2}";
        let cells = vec![
            Some(Value::Json(json.into())),
            Some(Value::Bytes((0..=255).cycle().take(600).collect())),
            None,
        ];
        app.complete(
            &request,
            Ok(Page {
                rows: vec![Row {
                    cells: cells.clone(),
                    target: None,
                }],
                columns: ["json", "bytes", "null"]
                    .into_iter()
                    .map(|name| onetui_core::Column {
                        name: name.into(),
                        datatype: "text".into(),
                    })
                    .collect(),
                continuation: Some("native-token".into()),
                ..Page::default()
            }),
        );
        let projections = app.view.projections.clone();
        let generation = app.generation;
        app.act(Action::Open);
        app.act(Action::Open);
        assert!(app.detail_text.contains("\n  \"name\": "));
        for pretty in ["on", "off"] {
            for highlight in ["on", "off"] {
                for wrap in ["on", "off"] {
                    for unicode in ["literal", "escaped"] {
                        command(&mut app, &format!("display pretty-print {pretty}"));
                        command(&mut app, &format!("display highlight {highlight}"));
                        command(&mut app, &format!("display word-wrap {wrap}"));
                        command(&mut app, &format!("display unicode {unicode}"));
                        assert_eq!(app.config.display.pretty_print, pretty == "on");
                        assert_eq!(app.config.display.highlight, highlight == "on");
                        assert_eq!(app.config.display.word_wrap, wrap == "on");
                        assert_eq!(app.detail_text.contains('🌊'), unicode == "literal");
                        assert!(app.error.is_none());
                    }
                }
            }
        }
        command(&mut app, "display format hex");
        assert_eq!(app.detail_format, ValueFormat::Hex);
        assert!(app.detail_text.contains("7b 22 6e"));
        app.act(Action::Back);
        assert!(app.detail_override.is_none());
        app.act(Action::Right);
        app.act(Action::Open);
        assert_eq!(app.detail_format, ValueFormat::Hex);
        command(&mut app, "display format binary");
        assert_eq!(app.detail_chunks, 3);
        app.act(Action::Next);
        assert!(app.detail_text.contains("00000100:"));
        let cached = app.detail_text.as_ptr();
        command(&mut app, "display highlight on");
        command(&mut app, "display word-wrap on");
        assert_eq!(app.detail_chunk, 1);
        assert_eq!(app.detail_text.as_ptr(), cached);
        command(&mut app, "display highlight off");
        command(&mut app, "display word-wrap off");
        command(&mut app, "scroll_right");
        assert_eq!(app.horizontal_scroll, 8);
        app.key(KeyEvent::new(KeyCode::Char('H'), KeyModifiers::SHIFT));
        assert_eq!(app.horizontal_scroll, 0);
        command(&mut app, "display format text");
        assert!(app.detail_notice.contains("Invalid UTF-8"));
        app.act(Action::Display);
        assert!(app.display_reasons[1].contains("Invalid UTF-8"));
        app.act(Action::Cancel);
        assert!(app.display_menu.is_none() && !app.quit);
        assert_eq!(app.view.page.rows[0].cells, cells);
        assert_eq!(app.view.projections, projections);
        assert_eq!(app.view.page.continuation.as_deref(), Some("native-token"));
        assert_eq!(app.generation, generation);
        assert!(app.request.is_none());
        command(&mut app, "display word-wrap maybe");
        assert!(app.error.is_some());
        app.act(Action::Connections);
        assert!(!app.config.display.word_wrap && !app.config.display.highlight);
        assert!(app.detail_prepared.is_empty());
    }

    #[test]
    fn startup_requires_explicit_alias_even_with_one_connection() {
        for contents in [
            "[connections]",
            "[connections.a]\nkind='fake'",
            "[connections.a]\nkind='fake'\n[connections.b]\nkind='fake'",
        ] {
            let config = Config::parse(contents, crate::test_provider::CATALOG).unwrap();
            let app = App::new(config, None);
            assert_eq!(app.view.resource.id, "connections");
            assert!(app.view.alias.is_none());
            assert!(app.request.is_none() && !app.loading);
            assert!(app.connection_status.is_none());
            if !app.view.page.rows.is_empty() {
                let selected = App::new(app.config, Some("a"));
                assert_eq!(selected.view.alias.as_deref(), Some("a"));
                assert!(selected.request.is_some() && selected.loading);
            }
        }
    }

    #[test]
    fn theme_picker_previews_accepts_and_reverts_without_touching_browsing_or_config() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let original = "theme='monokai'\n[connections.pg]\nkind='fake'";
        write!(file, "{original}").unwrap();
        let mut app = App::new(
            Config::load(file.path(), crate::test_provider::CATALOG).unwrap(),
            Some("pg"),
        );
        let request_id = app.request.as_ref().unwrap().id;
        let session = app.session;
        let resource = app.view.resource.clone();
        app.key(KeyEvent::new(KeyCode::Char('T'), KeyModifiers::SHIFT));
        assert_eq!(app.theme_menu, Some(Theme::Monokai));
        app.key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(app.config.theme, Theme::Flexoki);
        app.key(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE));
        assert_eq!(app.config.theme, Theme::Catppuccin);
        app.key(KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE));
        assert_eq!(app.config.theme, Theme::Flexoki);
        for c in [':', 'q', 'c', 'r', '/', 's'] {
            app.key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        assert!(app.command.is_none() && app.filter_input.is_none() && !app.quit);
        assert_eq!(app.request.as_ref().unwrap().id, request_id);
        assert_eq!(app.generation, request_id);
        assert_eq!(app.session, session);
        assert_eq!(app.view.resource, resource);
        assert!(app.loading);
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(app.config.theme, Theme::Monokai);
        assert!(app.theme_menu.is_none());

        for c in ":themes".chars() {
            app.key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(app.theme_menu.is_some());
        app.key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(app.config.theme, Theme::Flexoki);
        assert!(app.theme_menu.is_none());
        assert_eq!(std::fs::read_to_string(file.path()).unwrap(), original);

        app.act(Action::Themes);
        app.act(Action::Down);
        app.key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert_eq!(app.config.theme, Theme::Flexoki);
        assert!(app.theme_menu.is_none() && app.loading && !app.quit);
        assert_eq!(app.request.as_ref().unwrap().id, request_id);

        app.key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        app.key(KeyEvent::new(KeyCode::Char('T'), KeyModifiers::SHIFT));
        assert_eq!(app.filter_input.as_deref(), Some("T"));
        assert!(app.theme_menu.is_none());
    }

    fn app() -> App {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(file, "[connections.pg]\nkind='fake'\nurl_env='NEVER_RESOLVE_THIS'\n[connections.q]\nkind='checkonly'\nurl='http://localhost:6334'").unwrap();
        App::new(
            Config::load(file.path(), crate::test_provider::CATALOG).unwrap(),
            Some("pg"),
        )
    }

    #[test]
    fn page_keys_scroll_loaded_data_and_detail_without_fetching() {
        let mut app = app();
        let request = app.request.take().unwrap();
        app.complete(
            &request,
            Ok(Page {
                columns: (0..40)
                    .map(|i| onetui_core::Column {
                        name: format!("field_{i}"),
                        datatype: "text".into(),
                    })
                    .collect(),
                rows: (0..50)
                    .map(|_| Row {
                        cells: vec![Some("value".into()); 40],
                        target: None,
                    })
                    .collect(),
                next: true,
                ..Page::default()
            }),
        );
        let projections = app.view.projections.clone();
        let previews = app.view.previews.clone();
        let generation = app.generation;
        app.key(KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE));
        assert_eq!(app.view.selected, 11);
        app.key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL));
        assert_eq!(app.view.selected, 16);
        app.key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL));
        assert_eq!(app.view.selected, 11);
        app.key(KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE));
        assert_eq!(app.view.selected, 0);
        app.viewport.height = 34;
        app.key(KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE));
        assert_eq!(app.view.selected, 21);
        app.act(Action::Open);
        assert!(app.row_detail);
        app.key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL));
        assert_eq!(app.view.column, 10);
        assert_eq!(app.view.selected, 21);
        app.act(Action::Open);
        assert!(app.detail);
        app.key(KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE));
        assert_eq!(app.detail_scroll, 22);
        app.key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL));
        assert_eq!(app.detail_scroll, 11);
        app.key(KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE));
        assert_eq!(app.detail_scroll, 0);
        app.act(Action::Back);
        app.act(Action::Back);
        for _ in 0..10 {
            app.act(Action::PageDown);
        }
        assert_eq!(app.view.selected, 49);
        app.key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE));
        app.key(KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE));
        app.key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL));
        assert_eq!(app.view.selected, 49);
        assert_eq!(app.filter_input.as_deref(), Some(""));
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        app.key(KeyEvent::new_with_kind(
            KeyCode::PageUp,
            KeyModifiers::NONE,
            KeyEventKind::Release,
        ));
        assert_eq!(app.view.selected, 49);
        app.key(KeyEvent::new_with_kind(
            KeyCode::PageUp,
            KeyModifiers::NONE,
            KeyEventKind::Repeat,
        ));
        assert_eq!(app.view.selected, 28);
        app.command = Some("page_up".into());
        app.key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL));
        assert_eq!(app.command.as_deref(), Some("page_up"));
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(app.view.selected, 7);
        assert_eq!(app.view.projections, projections);
        assert_eq!(app.view.previews, previews);
        assert_eq!(app.generation, generation);
        assert_eq!(app.view.offset, 0);
        assert!(app.request.is_none());
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
                cells: vec![Some("public\x1b[31m".into())],
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
        assert_eq!(
            app.view.page.rows[0].cells[0]
                .as_ref()
                .and_then(onetui_core::Value::text),
            Some("pg")
        );
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
    fn page_bookmarks_return_from_page_100_to_the_first_page() {
        let mut app = app();
        let numbered = |index: i64| {
            let mut result = page(true);
            result.rows[0].cells[0] = Some(format!("row {}", index * PAGE_SIZE + 1).into());
            let mut second = result.rows[0].clone();
            second.cells[0] = Some(format!("row {}", index * PAGE_SIZE + 2).into());
            result.rows.push(second);
            result.continuation = Some(format!("opaque:{index}:\"София\""));
            result
        };
        let first = app.request.take().unwrap();
        app.complete(&first, Ok(numbered(0)));
        filter(&mut app, "row");
        app.act(Action::Sort);
        for index in 1..100 {
            app.view.selected = 1;
            app.act(Action::Next);
            let request = app.request.take().unwrap();
            app.complete(&request, Ok(numbered(index)));
            assert!(
                app.view
                    .previous
                    .iter()
                    .filter(|bookmark| bookmark.page.is_some())
                    .count()
                    <= 2
            );
        }
        for index in (0..99).rev() {
            assert!(
                app.available(Action::Previous),
                "cannot return to page {}",
                index + 1
            );
            app.act(Action::Previous);
            if let Some(request) = app.request.take() {
                assert_eq!(request.offset, index * PAGE_SIZE);
                assert_eq!(
                    request.continuation,
                    (index > 0).then(|| format!("opaque:{}:\"София\"", index - 1))
                );
                app.complete(&request, Ok(numbered(index)));
            }
            assert_eq!(app.view.offset, index * PAGE_SIZE);
            assert_eq!(app.view.selected, 1);
            assert_eq!(app.view.filter, "row");
            assert_eq!(app.view.sort, Some((0, false)));
            assert_eq!(
                app.view.page.rows[0].cells[0],
                Some(format!("row {}", index * PAGE_SIZE + 1).into())
            );
        }
        assert!(!app.available(Action::Previous));
        assert!(app.request.is_none());
    }

    #[test]
    fn failed_cancelled_and_oversized_backward_reads_keep_the_bookmark() {
        let mut app = app();
        for index in 0..5 {
            if index > 0 {
                app.act(Action::Next);
            }
            let request = app.request.take().unwrap();
            let mut result = page(true);
            result.continuation = Some(format!("token-{index}"));
            app.complete(&request, Ok(result));
        }
        app.act(Action::Previous);
        app.act(Action::Previous);
        assert_eq!(app.view.offset, 200);
        app.act(Action::Previous);
        let failed = app.request.take().unwrap();
        assert_eq!(failed.continuation.as_deref(), Some("token-0"));
        app.complete(&failed, Err(anyhow::anyhow!("connection lost")));
        assert_eq!(app.view.offset, 200);
        assert_eq!(app.view.previous.len(), 2);
        app.act(Action::Previous);
        let cancelled = app.request.take().unwrap();
        app.act(Action::Cancel);
        app.complete(&cancelled, Ok(page(false)));
        assert_eq!(app.view.offset, 200);
        assert_eq!(app.view.previous.len(), 2);
        app.act(Action::Previous);
        let oversized = app.request.take().unwrap();
        let mut large = page(true);
        large.rows[0].cells[0] = Some("x".repeat(PAGE_BYTES + 1).into());
        app.complete(&oversized, Ok(large));
        assert_eq!(app.view.offset, 200);
        assert_eq!(app.view.previous.len(), 2);
        app.act(Action::Previous);
        let retry = app.request.take().unwrap();
        assert_eq!(retry.continuation, failed.continuation);
        let mut fresh = page(true);
        fresh.continuation = Some("new-forward-token".into());
        app.complete(&retry, Ok(fresh));
        assert_eq!(app.view.offset, 100);
        assert_eq!(app.view.previous.len(), 1);
        app.act(Action::Next);
        let next = app.request.take().unwrap();
        assert_eq!(next.continuation.as_deref(), Some("new-forward-token"));
        app.complete(&next, Ok(page(true)));
        app.act(Action::Refresh);
        let refresh = app.request.take().unwrap();
        assert!(refresh.continuation.is_none());
        app.complete(&refresh, Ok(page(true)));
        assert!(app.view.position.is_none());
        assert!(app.view.previous.is_empty());
        app.act(Action::Connections);
        app.complete(&retry, Ok(page(true)));
        assert!(app.view.previous.is_empty());
        assert_eq!(app.view.resource.id, "connections");
    }

    #[test]
    fn bookmark_budgets_stop_forward_reads_without_erasing_history() {
        let mut app = app();
        let first = app.request.take().unwrap();
        app.complete(&first, Ok(page(true)));
        app.view.previous = (0..BOOKMARK_LIMIT)
            .map(|index| PageBookmark {
                offset: index as i64 * PAGE_SIZE,
                position: None,
                selected: None,
                page: None,
            })
            .collect();
        app.act(Action::Next);
        assert!(app.request.is_none());
        assert_eq!(app.view.previous.len(), BOOKMARK_LIMIT);
        assert!(app.error.as_deref().unwrap().contains("bookmark limit"));
        app.view.previous.clear();
        app.view.previous.push_back(PageBookmark {
            offset: 0,
            position: Some("x".repeat(PAGE_BYTES)),
            selected: None,
            page: None,
        });
        app.view.position = Some("x".into());
        app.act(Action::Next);
        assert!(app.request.is_none());
        assert_eq!(app.view.previous.len(), 1);
        assert!(app.error.as_deref().unwrap().contains("bookmark limit"));
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
        assert_eq!(app.view.previous.len(), 4);
        assert_eq!(
            app.view
                .previous
                .iter()
                .filter(|bookmark| bookmark.page.is_some())
                .count(),
            2
        );
        app.act(Action::Next);
        let request = app.request.take().unwrap();
        app.complete(&request, Err(anyhow::anyhow!("limit exceeded")));
        assert_eq!(app.view.offset, 400);
        app.act(Action::Previous);
        assert_eq!(app.view.offset, 300);
        assert!(app.request.is_none());
        assert!(
            !app.view.projections[0][0]
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
        oversized.rows[0].cells[0] = Some("x".repeat(PAGE_BYTES + 1).into());
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
                        Some(String::new().into()),
                        Some("NULL".into()),
                        Some(text.clone().into()),
                    ],
                    target: None,
                }],
                ..Page::default()
            }),
        );
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
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
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        app.key(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
        assert_eq!(app.view.resource.id, "fake.columns");
        app.act(Action::Back);
        assert_eq!(app.view.resource.id, "fake.rows");
        assert_eq!(app.view.column, 3);
    }

    #[test]
    fn quit_requires_confirmation_and_keeps_work_until_confirmed() {
        let mut active = app();
        let request_id = active.request.as_ref().unwrap().id;
        active.key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE));
        assert!(active.confirm_quit);
        assert!(!active.quit);
        assert_eq!(active.request.as_ref().unwrap().id, request_id);
        active.key(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE));
        assert!(active.confirm_quit);
        active.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(!active.confirm_quit);
        assert!(!active.quit);
        assert_eq!(active.request.as_ref().unwrap().id, request_id);

        command(&mut active, "quit");
        assert!(active.confirm_quit);
        active.key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::NONE));
        assert!(!active.confirm_quit);
        active.act(Action::Cancel);
        assert!(!active.confirm_quit);
        assert!(!active.quit);
        active.key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE));
        active.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(active.quit);
        assert!(active.request.is_none());

        let mut idle = app();
        idle.request = None;
        idle.loading = false;
        idle.key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert!(!idle.confirm_quit);
        assert!(idle.quit);
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
