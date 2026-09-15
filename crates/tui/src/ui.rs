use std::io::{IsTerminal, stdout};
use std::time::Duration;

use anyhow::{Result, anyhow, ensure};
use crossterm::event::{Event, EventStream};
use futures_util::StreamExt;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Cell, Paragraph, Row, Table, TableState, Wrap};
use ratatui::{DefaultTerminal, Frame};

use crate::app::App;
#[cfg(test)]
use onetui_core::Page;
use onetui_core::catalog::Action;
use onetui_core::{PAGE_SIZE, display};

use onetui_theme::Palette;

fn color([r, g, b]: [u8; 3]) -> Color {
    Color::Rgb(r, g, b)
}

fn panel(p: &Palette, title: impl Into<Line<'static>>) -> Block<'static> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(color(p.border)))
        .title(title.into().style(Style::new().fg(color(p.title)).bold()))
}

fn key_hints(
    frame: &mut Frame,
    area: Rect,
    p: &Palette,
    hints: &[(&str, &str)],
    disabled: &[&str],
) {
    let width = hints
        .iter()
        .map(|(key, _)| key.len())
        .max()
        .unwrap_or(0)
        .max(5)
        + 2;
    let lines = hints
        .iter()
        .map(|(key, description)| {
            Line::from(vec![
                Span::styled(
                    format!("{key:<width$}"),
                    if disabled.contains(key) {
                        Style::new().fg(color(p.muted))
                    } else {
                        Style::new().fg(color(p.key_hint)).bold()
                    },
                ),
                Span::styled(*description, Style::new().fg(color(p.muted))),
            ])
        })
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(lines), area);
}

fn context(frame: &mut Frame, area: Rect, app: &App) {
    let p = app.config.theme.palette();
    if area.height < 8 {
        frame.render_widget(
            Paragraph::new(format!(
                "read-only | {} | {}",
                display(app.view.alias.as_deref().unwrap_or("choose connection")),
                app.view.resource.breadcrumb()
            ))
            .style(Style::new().fg(color(p.title))),
            area,
        );
        return;
    }
    let block = panel(
        p,
        Line::from(vec![
            Span::raw(" Context | "),
            Span::styled("read-only", Style::new().fg(color(p.error))),
            Span::raw(" "),
        ]),
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let [details, keys] = Layout::horizontal([
        Constraint::Percentage(if area.width >= 100 { 45 } else { 100 }),
        Constraint::Min(0),
    ])
    .areas(inner);
    let alias = app.view.alias.as_deref().unwrap_or("choose connection");
    let kind = app
        .view
        .alias
        .as_deref()
        .and_then(|alias| app.config.descriptor(alias))
        .map_or("none selected", |descriptor| descriptor.kind);
    let transport_color = match app.connection_status {
        Some(onetui_core::provider::ConnectionStatus::Connected) => color(p.success),
        Some(onetui_core::provider::ConnectionStatus::Disconnected) => color(p.error),
        _ => color(p.warning),
    };
    let transport = app
        .connection_status
        .map_or_else(|| "Not connected".into(), |status| format!("{status:?}"));
    let path = app
        .view
        .resource
        .path
        .iter()
        .map(|part| display(part))
        .collect::<Vec<_>>()
        .join(" / ");
    let fields = [
        ("Connection", display(alias), color(p.border)),
        ("Datasource", kind.to_owned(), color(p.identifier)),
        (
            "Resource",
            app.view.resource.id.to_owned(),
            color(p.table_heading),
        ),
        (
            "Path",
            if path.is_empty() { "/".into() } else { path },
            color(p.title),
        ),
        (
            "Loaded",
            format!(
                "{} items | {} shown",
                app.view.page.rows.len(),
                app.view.visible.len()
            ),
            color(p.text),
        ),
        ("Transport", transport, transport_color),
    ];
    frame.render_widget(
        Paragraph::new(
            fields
                .into_iter()
                .map(|(label, value, value_color)| {
                    Line::from(vec![
                        Span::styled(format!("{label:<11}"), Style::new().fg(color(p.muted))),
                        Span::styled(value, Style::new().fg(value_color)),
                    ])
                })
                .collect::<Vec<_>>(),
        ),
        details,
    );
    if keys.width == 0 {
        return;
    }
    if let Some(form) = &app.connection_form {
        let hints = if form.choosing {
            vec![
                ("j/k", "choose datasource"),
                ("Enter", "continue"),
                ("Esc", "cancel"),
            ]
        } else {
            vec![
                ("Tab/Enter", "next field"),
                ("Shift-Tab", "previous field"),
                ("F2/Ctrl-s", "save connection"),
                ("Esc", "discard"),
                ("Ctrl-u", "clear field"),
                ("", "Secrets: environment variable names only"),
            ]
        };
        key_hints(frame, keys, p, &hints, &[]);
        return;
    }
    if app.query_editor.is_some() {
        key_hints(
            frame,
            keys,
            p,
            &[
                ("Enter/F5", "execute read-only query"),
                ("Shift-Enter", "new line"),
                ("Esc", "return / cancel request"),
                ("Ctrl-u", "clear draft"),
                ("arrows", "move cursor"),
                ("", "16 KiB; draft kept in memory"),
            ],
            &[],
        );
        return;
    } else if app.display_menu.is_some() {
        key_hints(
            frame,
            keys,
            p,
            &[
                ("j/k", "select display setting"),
                ("Enter", "apply / toggle"),
                ("Esc", "close; session settings kept"),
                ("?", "help"),
                ("", "Config file is unchanged"),
            ],
            &[],
        );
        return;
    }
    if app.theme_menu.is_some() {
        key_hints(
            frame,
            keys,
            p,
            &[
                ("j/k Up/Down", "Theme preview"),
                ("Enter", "keep for session"),
                ("Esc", "restore previous"),
                ("Ctrl-c", "restore previous"),
                ("", "Config file is unchanged"),
            ],
            &[],
        );
        return;
    }
    if app.command.is_some() || app.filter_input.is_some() {
        let hints: &[(&str, &str)] = if app.filter_input.is_some() {
            &[
                ("/", "Filter displayed page"),
                ("Enter", "keep filter (live)"),
                ("Esc", "restore previous"),
                ("Backspace", "delete character"),
                ("", "Max 256 UTF-8 bytes"),
            ]
        } else {
            &[
                (":", "Command input"),
                ("Enter", "execute"),
                ("Esc", "cancel"),
                ("Backspace", "delete character"),
            ]
        };
        key_hints(frame, keys, p, hints, &[]);
        return;
    }
    let actions: Vec<_> = app.context_actions().collect();
    let column_count = actions.len().div_ceil(6).max(1);
    let columns = Layout::horizontal(vec![Constraint::Fill(1); column_count]).split(keys);
    for (column, chunk) in actions.chunks(6).enumerate() {
        let Some(area) = columns.get(column) else {
            break;
        };
        let names = chunk
            .iter()
            .map(|entry| {
                serde_json::to_value(entry.id)
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        let hints = chunk
            .iter()
            .zip(&names)
            .map(|(entry, name)| (entry.keys[0], name.as_str()))
            .collect::<Vec<_>>();
        let disabled: Vec<_> = chunk
            .iter()
            .filter(|entry| !app.available(entry.id))
            .map(|entry| entry.keys[0])
            .collect();
        key_hints(frame, *area, p, &hints, &disabled);
    }
}

struct TerminalGuard;

static ENHANCED_KEYS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn restore_input_modes() {
    if ENHANCED_KEYS.swap(false, std::sync::atomic::Ordering::SeqCst) {
        let _ = crossterm::execute!(stdout(), crossterm::event::PopKeyboardEnhancementFlags);
    }
    let _ = crossterm::execute!(stdout(), crossterm::event::DisableBracketedPaste);
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_input_modes();
        ratatui::restore();
    }
}

fn terminal() -> Result<(TerminalGuard, DefaultTerminal)> {
    ensure!(
        std::io::stdin().is_terminal() && stdout().is_terminal(),
        "interactive browsing requires a terminal on stdin/stdout; use --check or schema for headless operation"
    );
    // Restore terminal modes even if initialization fails halfway through.
    let guard = TerminalGuard;
    let terminal = ratatui::try_init().map_err(|_| anyhow!("cannot initialize terminal"))?;
    // Pop the alternate-screen keyboard mode before Ratatui's panic hook leaves that screen.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore_input_modes();
        hook(info);
    }));
    ENHANCED_KEYS.store(true, std::sync::atomic::Ordering::SeqCst);
    crossterm::execute!(
        stdout(),
        crossterm::event::PushKeyboardEnhancementFlags(
            crossterm::event::KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
        ),
        crossterm::event::EnableBracketedPaste
    )?;
    Ok((guard, terminal))
}

pub async fn run<P: onetui_core::provider::Provider>(
    mut app: App,
    deadline: Duration,
    catalog: &[P],
) -> Result<()>
where
    P::Executor: 'static,
{
    use crate::worker::{Worker, WorkerEvent};
    app.providers = catalog
        .iter()
        .map(|provider| provider.descriptor())
        .collect();
    let (_guard, mut terminal) = terminal()?;
    let mut events = EventStream::new();
    let mut worker: Option<Worker> = None;
    let outcome = async {
        while !app.quit {
            app.save_connection(catalog);
            if let Some(active) = &mut worker {
                if !app.following {
                    active.pause_follow();
                }
                if app.view.alias.as_deref() != Some(active.alias.as_str()) || app.session != active.session {
                    active.stop();
                } else if active.request.as_ref().is_some_and(|request| request.id != app.generation) {
                    active.cancel();
                }
            }
            if worker.is_none() && app.request.is_some() {
                let alias = app.request.as_ref().expect("pending request").alias.clone();
                match app.config.configure(&alias, catalog, &|name| std::env::var(name).ok()) {
                    Ok(executor) => worker = Some(Worker::new(alias, app.session, executor)),
                    Err(error) => {
                        let request = app.request.take().expect("pending request");
                        app.complete(&request, Err(error));
                    }
                }
            }
            if let Some(active) = &mut worker
                && !active.closing && active.request.is_none()
                && let Some(request) = app.request.take()
            {
                active.submit(request, deadline)?;
            }
            terminal.draw(|frame| {
                app.viewport = frame.area();
                draw(frame, &app);
            }).map_err(|_| anyhow!("cannot draw terminal"))?;
            tokio::select! {
                _ = async { tokio::time::sleep_until(app.follow_due.expect("follow deadline")).await }, if app.follow_due.is_some() => app.follow_tick(),
                event = events.next() => match event {
                    Some(Ok(Event::Key(key))) => app.key(key),
                    Some(Ok(Event::Paste(text))) => {
                        app.paste(&text);
                    },
                    Some(Ok(_)) => {},
                    Some(Err(_)) | None => return Err(anyhow!("terminal input closed or failed")),
                },
                signal = tokio::signal::ctrl_c() => {
                    signal.map_err(|_| anyhow!("cannot listen for interruption"))?;
                    app.act(Action::Cancel);
                },
                event = async { worker.as_mut().expect("guarded worker").event().await }, if worker.is_some() => {
                    match event {
                        WorkerEvent::Page(result) => {
                            let request = worker.as_mut().expect("active worker").request.take().expect("completed request");
                            if worker.as_ref().is_some_and(|w| w.session == app.session) {
                                app.complete(&request, result);
                            }
                        }
                        WorkerEvent::Status(status) => {
                            if let Some(worker) = &worker {
                                app.update_connection_status(worker.session, &worker.alias, status);
                            }
                        }
                        WorkerEvent::Finished(result) => {
                            let finished = worker.take().expect("finished worker");
                            result?;
                            ensure!(finished.closing, "browsing worker stopped unexpectedly");
                        }
                    }
                },
            }
        }
        Ok(())
    }.await;
    let cleanup = if let Some(active) = &mut worker {
        active.stop();
        match tokio::time::timeout(Duration::from_secs(2), &mut active.task).await {
            Ok(result) => result
                .map_err(|_| anyhow!("browsing worker failed; terminal restored"))
                .and_then(|r| r),
            Err(_) => {
                active.task.abort();
                let _ = (&mut active.task).await;
                Err(anyhow!(
                    "browsing worker shutdown timed out; connections discarded"
                ))
            }
        }
    } else {
        Ok(())
    };
    match (outcome, cleanup) {
        (Err(primary), Err(cleanup)) => Err(anyhow!("{primary}; cleanup: {cleanup}")),
        (Err(error), _) | (_, Err(error)) => Err(error),
        _ => Ok(()),
    }
}

fn command_bar(frame: &mut Frame, area: Rect, app: &App) {
    let p = app.config.theme.palette();
    let value = if let Some(value) = &app.filter_input {
        format!("/{}", display(value))
    } else if let Some(value) = &app.command {
        format!(":{value}")
    } else {
        return;
    };
    let style = Style::new().fg(color(p.key_hint)).bg(color(p.surface));
    let inner = if area.height >= 3 {
        let block = panel(p, "").style(style);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        inner
    } else {
        area
    };
    let line = Line::raw(value);
    // Leave a cell for a wide glyph clipped at the left edge; keep the edited tail visible.
    let scroll = line
        .width()
        .saturating_sub(usize::from(inner.width.saturating_sub(1))) as u16;
    frame.render_widget(Paragraph::new(line).style(style).scroll((0, scroll)), inner);
}

fn help(frame: &mut Frame, area: Rect, app: &App) {
    let p = app.config.theme.palette();
    let block =
        panel(p, " Help | :command | Esc close ").title_bottom(" Keybindings are currently fixed ");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let entries = app
        .actions()
        .map(|entry| {
            (
                entry.keys.join(" / "),
                serde_json::to_value(entry.id)
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_owned(),
                entry.description,
            )
        })
        .collect::<Vec<_>>();
    if inner.width < 60 {
        let lines = entries
            .iter()
            .flat_map(|(key, command, description)| {
                [
                    Line::from(vec![
                        Span::styled(key, Style::new().fg(color(p.key_hint)).bold()),
                        Span::styled(
                            format!("  :{command}"),
                            Style::new().fg(color(p.identifier)),
                        ),
                    ]),
                    Line::raw(*description),
                    Line::default(),
                ]
            })
            .collect::<Vec<_>>();
        frame.render_widget(
            wrapping(Paragraph::new(lines), app).scroll((app.detail_scroll, app.horizontal_scroll)),
            inner,
        );
        return;
    }
    let key_width = entries
        .iter()
        .map(|(key, _, _)| key.len())
        .max()
        .unwrap_or(3)
        .max(3) as u16;
    let command_width = entries
        .iter()
        .map(|(_, command, _)| command.len())
        .max()
        .unwrap_or(7)
        .max(7) as u16;
    let description_width = usize::from(inner.width.saturating_sub(key_width + command_width + 4));
    let rows = entries
        .into_iter()
        .enumerate()
        .map(|(index, (key, command, description))| {
            let mut lines = vec![String::new()];
            for word in description.split_whitespace() {
                let line = lines.last_mut().unwrap();
                if app.config.display.word_wrap
                    && !line.is_empty()
                    && line.len() + 1 + word.len() > description_width
                {
                    lines.push(word.to_owned());
                } else {
                    if !line.is_empty() {
                        line.push(' ');
                    }
                    line.push_str(word);
                }
            }
            Row::new([
                Cell::from(key).style(Style::new().fg(color(p.key_hint)).bold()),
                Cell::from(command).style(Style::new().fg(color(p.identifier))),
                Cell::from(horizontal(&lines.join("\n"), app)),
            ])
            .height(lines.len() as u16)
            .style(Style::new().bg(color(if index % 2 == 0 {
                p.background
            } else {
                p.surface
            })))
        });
    frame.render_stateful_widget(
        Table::new(
            rows,
            [
                Constraint::Length(key_width),
                Constraint::Length(command_width),
                Constraint::Min(1),
            ],
        )
        .column_spacing(2)
        .header(
            Row::new(["KEY", "COMMAND", "DESCRIPTION"])
                .style(Style::new().fg(color(p.table_heading)).bold())
                .bottom_margin(1),
        ),
        inner,
        &mut TableState::default().with_offset(app.detail_scroll as usize),
    );
}

fn wrapping<'a>(paragraph: Paragraph<'a>, app: &App) -> Paragraph<'a> {
    if app.config.display.word_wrap {
        paragraph.wrap(Wrap { trim: false })
    } else {
        paragraph
    }
}

fn horizontal(text: &str, app: &App) -> String {
    use unicode_segmentation::UnicodeSegmentation;
    use unicode_width::UnicodeWidthStr;
    if app.config.display.word_wrap {
        return text.to_owned();
    }
    text.split('\n')
        .map(|line| {
            let mut skipped = 0;
            line.graphemes(true)
                .skip_while(|g| {
                    if skipped >= usize::from(app.horizontal_scroll) {
                        false
                    } else {
                        skipped += g.width();
                        true
                    }
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn wrap_preview(text: &str, width: usize, app: &App) -> String {
    use unicode_segmentation::UnicodeSegmentation;
    use unicode_width::UnicodeWidthStr;
    if width == 0 {
        return String::new();
    }
    if !app.config.display.word_wrap {
        return horizontal(text, app);
    }
    let truncated = |mut out: String| {
        let start = out.rfind('\n').map_or(0, |offset| offset + 1);
        let mut used = 0;
        let mut end = start;
        for (offset, grapheme) in out[start..].grapheme_indices(true) {
            if used + grapheme.width() >= width {
                break;
            }
            used += grapheme.width();
            end = start + offset + grapheme.len();
        }
        out.truncate(end);
        out.push('…');
        out
    };
    let mut out = String::new();
    let mut used = 0;
    let mut lines = 1;
    for word in text.split_word_bounds() {
        if word == "\n" {
            if lines == 3 {
                return truncated(out);
            }
            out.push('\n');
            lines += 1;
            used = 0;
            continue;
        }
        if used > 0 && used + word.width() > width {
            if lines == 3 {
                return truncated(out);
            }
            out.push('\n');
            lines += 1;
            used = 0;
        }
        for grapheme in word.graphemes(true) {
            if grapheme.width() > width {
                return truncated(out);
            }
            if used > 0 && used + grapheme.width() > width {
                if lines == 3 {
                    return truncated(out);
                }
                out.push('\n');
                lines += 1;
                used = 0;
            }
            out.push_str(grapheme);
            used += grapheme.width();
        }
    }
    out
}

fn detail_spans(app: &App) -> Vec<Line<'_>> {
    let p = app.config.theme.palette();
    app.detail_text
        .split('\n')
        .enumerate()
        .map(|(index, line)| {
            if index == 0 || !app.config.display.highlight {
                return Line::raw(line);
            }
            if matches!(
                app.detail_format,
                onetui_core::value::ValueFormat::Hex | onetui_core::value::ValueFormat::Binary
            ) && let Some((offset, bytes)) = line.split_once(':')
            {
                return Line::from(vec![
                    Span::styled(offset, Style::new().fg(color(p.muted))),
                    Span::raw(":"),
                    Span::styled(bytes, Style::new().fg(color(p.identifier))),
                ]);
            }
            if app.detail_format != onetui_core::value::ValueFormat::Json {
                return Line::raw(line);
            }
            let mut spans = Vec::new();
            let mut quoted = false;
            let mut escaped = false;
            let mut start = 0;
            for (i, c) in line.char_indices() {
                if c == '"' && !escaped {
                    if !quoted {
                        if start < i {
                            spans.push(Span::styled(
                                &line[start..i],
                                Style::new().fg(color(p.table_heading)),
                            ));
                        }
                        start = i;
                    } else {
                        spans.push(Span::styled(
                            &line[start..i + 1],
                            Style::new().fg(color(p.identifier)),
                        ));
                        start = i + 1;
                    }
                    quoted = !quoted;
                }
                escaped = quoted && c == '\\' && !escaped;
            }
            if start < line.len() {
                spans.push(Span::styled(
                    &line[start..],
                    Style::new().fg(color(if quoted {
                        p.identifier
                    } else {
                        p.table_heading
                    })),
                ));
            }
            Line::from(spans)
        })
        .collect()
}

fn panels(area: Rect, app: &App) -> [Rect; 4] {
    let show_command = app.command.is_some() || app.filter_input.is_some();
    Layout::vertical([
        Constraint::Length(if area.height >= 20 && area.width >= 60 {
            8
        } else {
            1
        }),
        Constraint::Length(if !show_command {
            0
        } else if area.height >= 12 {
            3
        } else {
            1
        }),
        Constraint::Min(1),
        Constraint::Length(if area.height >= 12 { 2 } else { 1 }),
    ])
    .areas(area)
}

fn query_panels(body: Rect, app: &App) -> [Rect; 2] {
    let visible = app.query_editor.is_some()
        || (app.view.query.is_some()
            && !app.help
            && !app.detail
            && !app.row_detail
            && app.theme_menu.is_none()
            && app.display_menu.is_none());
    let height = if !visible || body.height < 4 {
        0
    } else if body.height < 7 {
        1
    } else {
        (body.height / 3).clamp(3, 8)
    };
    Layout::vertical([Constraint::Length(height), Constraint::Min(0)]).areas(body)
}

fn table_widths(app: &App, body: Rect) -> Vec<u16> {
    use unicode_width::UnicodeWidthStr;
    let start = app.view.column / 4 * 4;
    let end = (start + 4).min(app.column_count());
    let count = end - start;
    let available = body
        .width
        .saturating_sub(4 + count.saturating_sub(1) as u16);
    if count == 1 {
        return vec![available];
    }
    // Measure the loaded page, not the selected row or filtered subset.
    let needs: Vec<_> = (start..end)
        .map(|column| {
            app.view
                .previews
                .iter()
                .flat_map(|row| row[column].lines())
                .map(UnicodeWidthStr::width)
                .max()
                .unwrap_or(0)
                // Keep selection and sort markers from moving columns.
                .max(app.column_name(column).width() + 4)
                .min(usize::from(available)) as u16
        })
        .collect();
    let mut order: Vec<_> = (0..count).collect();
    order.sort_by_key(|&column| needs[column]);
    let mut widths = vec![0; count];
    let mut remaining = available;
    for (rank, column) in order.into_iter().enumerate() {
        let share = remaining / (count - rank) as u16;
        widths[column] = needs[column].min(share);
        remaining -= widths[column];
    }
    widths
}

pub(crate) fn page_step(app: &App, down: bool, half: bool) -> usize {
    let body = query_panels(panels(app.viewport, app)[2], app)[1];
    let mut budget = usize::from(body.height.saturating_sub(if app.detail { 2 } else { 3 }));
    if half {
        budget /= 2;
    }
    budget = budget.max(1);
    if app.detail || app.help {
        return budget;
    }
    let mut index = if app.row_detail {
        app.view.column
    } else {
        app.view.selected
    };
    let count = if app.row_detail {
        app.column_count()
    } else {
        app.view.visible.len()
    };
    let mut steps = 0;
    let widths = table_widths(app, body);
    while index < count && budget > 0 {
        let height = if app.row_detail {
            if index == app.view.column {
                let (height, lines) = row_value_extent(app);
                lines.min(height)
            } else {
                let available = body.width.saturating_sub(6);
                let width = usize::from(available - available / 4 * 2).max(1);
                wrap_preview(
                    &app.view.previews[app.view.selected_index().unwrap()][index],
                    width,
                    app,
                )
                .lines()
                .count()
                .clamp(1, 3)
            }
        } else {
            let start = app.view.column / 4 * 4;
            let end = (start + 4).min(app.column_count());
            app.view.previews[app.view.visible[index]][start..end]
                .iter()
                .zip(&widths)
                .map(|(text, &width)| {
                    wrap_preview(text, usize::from(width).max(1), app)
                        .lines()
                        .count()
                        .clamp(1, 3)
                })
                .max()
                .unwrap_or(1)
        };
        budget = budget.saturating_sub(height);
        steps += 1;
        if !down && index == 0 {
            break;
        }
        index = if down { index + 1 } else { index - 1 };
    }
    steps
}

pub(crate) fn row_value_extent(app: &App) -> (usize, usize) {
    let body = query_panels(panels(app.viewport, app)[2], app)[1];
    let available = body.width.saturating_sub(6);
    let width = usize::from(available - available / 4 * 2).max(1);
    let height = usize::from(body.height.saturating_sub(3)).max(1);
    let Some(prepared) = &app.row_value else {
        return (height, 1);
    };
    let cell = &app.view.page.rows[app.view.selected_index().expect("selected row")].cells
        [app.view.column];
    let (_, lines) = crate::row_value::viewport(
        prepared,
        cell.as_ref().map_or(&[], onetui_core::Value::bytes),
        width,
        app.config.display.word_wrap,
        0,
        0,
    );
    (height, lines)
}

fn connection_form(frame: &mut Frame, area: Rect, app: &App) {
    let form = app.connection_form.as_ref().unwrap();
    let p = app.config.theme.palette();
    let selected = Style::new()
        .fg(color(p.selection_fg))
        .bg(color(p.selection_bg))
        .bold();
    if form.choosing {
        let rows = form
            .providers
            .iter()
            .map(|provider| Row::new([provider.kind, provider.browsing]));
        frame.render_stateful_widget(
            Table::new(rows, [Constraint::Length(14), Constraint::Min(1)])
                .block(panel(p, " Add connection | choose datasource "))
                .row_highlight_style(selected)
                .highlight_symbol("> "),
            area,
            &mut TableState::default().with_selected(Some(form.selected)),
        );
        return;
    }
    let block = panel(
        p,
        format!(
            " Add {} connection | F2 save | Esc discard ",
            form.provider().kind
        ),
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let [fields, help] = Layout::vertical([
        Constraint::Min(1),
        Constraint::Length(if inner.height >= 6 { 4 } else { 0 }),
    ])
    .areas(inner);
    let label_width = form
        .provider()
        .connection_fields
        .iter()
        .map(|f| f.name.len())
        .max()
        .unwrap_or(5)
        .max(5) as u16
        + 3;
    let label_width = label_width.min(fields.width / 2);
    let rows = form.inputs.iter().enumerate().map(|(i, input)| {
        let name = if i == 0 {
            "alias"
        } else {
            form.provider().connection_fields[i - 1].name
        };
        let width = fields.width.saturating_sub(label_width + 2).max(1) as usize;
        let value = if i == form.field {
            // Keep the caret visible while editing a value longer than the field.
            let (lines, row, _) = input.lines(width, true);
            lines.into_iter().nth(row).unwrap_or_default()
        } else {
            Line::raw(display(&input.text))
        };
        let required = i == 0 || form.documentation[name]["required"] == true;
        Row::new(vec![
            Cell::from(format!("{name}{}", if required { " *" } else { "" })),
            Cell::from(value),
        ])
    });
    frame.render_stateful_widget(
        Table::new(rows, [Constraint::Length(label_width), Constraint::Min(1)])
            .row_highlight_style(selected)
            .highlight_symbol("> "),
        fields,
        &mut TableState::default().with_selected(Some(form.field)),
    );
    let hint = if form.field == 0 {
        "Alias: ASCII letters, digits, underscores or hyphens. Optional fields may stay blank."
            .into()
    } else {
        let field = form.provider().connection_fields[form.field - 1];
        let spec = &form.documentation[field.name];
        let input = match field.input {
            onetui_core::provider::ConnectionInput::Text => "Text",
            onetui_core::provider::ConnectionInput::StringList => {
                "Comma-separated values, without quotes or brackets"
            }
            onetui_core::provider::ConnectionInput::Boolean => "true or false",
        };
        let purpose = spec["purpose"].as_str().unwrap_or("");
        let values = spec
            .get("values")
            .or_else(|| spec.get("example"))
            .map_or(String::new(), |v| format!(" | Values/example: {v}"));
        let default = spec
            .get("default")
            .map_or(String::new(), |v| format!(" | Blank: {v}"));
        let required = spec
            .get("required")
            .map_or(String::new(), |v| format!(" | Required: {v}"));
        format!(
            "{input}. {purpose}{values}{default}{required}\nNested settings (OAuth, decoders): edit TOML; see onetui schema."
        )
    };
    frame.render_widget(
        wrapping(
            Paragraph::new(hint).style(Style::new().fg(color(p.muted))),
            app,
        ),
        help,
    );
}

pub fn draw(frame: &mut Frame, app: &App) {
    let p = app.config.theme.palette();
    let area = frame.area();
    frame.render_widget(
        Block::new().style(Style::new().fg(color(p.text)).bg(color(p.background))),
        area,
    );
    let [info, command, body, status] = panels(area, app);
    context(frame, info, app);
    if app.command.is_some() || app.filter_input.is_some() {
        command_bar(frame, command, app);
    }
    let [query, body] = query_panels(body, app);
    if query.height > 0 {
        let language = app.query_descriptor().map_or("Query", |d| d.language);
        let hint = if app.loading {
            "Loading | Esc cancel"
        } else if app.query_editor.is_some() {
            "Enter/F5 run | Esc rows"
        } else {
            "executed | e edit"
        };
        let block = panel(p, format!(" {language} query | {hint} "));
        let inner = if query.height > 2 {
            block.inner(query)
        } else {
            query
        };
        if query.height > 2 {
            frame.render_widget(block, query);
        }
        if let Some(editor) = &app.query_editor {
            let (lines, row, column) =
                editor.lines(inner.width as usize, app.config.display.word_wrap);
            let top = row.saturating_sub(inner.height.saturating_sub(1) as usize);
            let left = if app.config.display.word_wrap {
                0
            } else {
                column.saturating_sub(inner.width.saturating_sub(1) as usize)
            };
            frame.render_widget(
                Paragraph::new(lines).scroll((top as u16, left as u16)),
                inner,
            );
        } else if let Some(text) = &app.view.query {
            let lines = text
                .split('\n')
                .map(|line| Line::raw(display(line)))
                .collect::<Vec<_>>();
            frame.render_widget(wrapping(Paragraph::new(lines), app), inner);
        }
    }
    if app.connection_form.is_some() {
        connection_form(frame, body, app);
    } else if app.view.alias.is_none()
        && app.view.page.rows.is_empty()
        && !app.help
        && app.theme_menu.is_none()
        && app.display_menu.is_none()
    {
        let path = app.config.path().map_or_else(
            || "not configured".into(),
            |path| display(&path.display().to_string()),
        );
        let text = format!(
            "No connections yet. Press a to add one.\n\nConfig: {path}\nNothing is created until you save."
        );
        frame.render_widget(
            wrapping(Paragraph::new(text), app).block(panel(p, " Connections ")),
            body,
        );
    } else if app.display_menu.is_some() && !app.help {
        let options = app.config.display;
        let mut entries: Vec<_> = onetui_core::value::FORMATS
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let reason = app.display_reasons[i];
                Row::new([
                    format!(
                        "{}{}",
                        f.name,
                        if app.detail && f.id == app.detail_format {
                            " *"
                        } else {
                            ""
                        }
                    ),
                    reason.to_owned(),
                ])
            })
            .collect();
        for (name, value) in [
            ("pretty-print", options.pretty_print),
            ("highlight", options.highlight),
            ("word-wrap", options.word_wrap),
        ] {
            entries.push(Row::new([
                name.to_owned(),
                if value { "on" } else { "off" }.to_owned(),
            ]));
        }
        entries.push(Row::new([
            "unicode".to_owned(),
            format!("{:?}", options.unicode).to_lowercase(),
        ]));
        let table = Table::new(entries, [Constraint::Length(16), Constraint::Min(1)])
            .block(panel(p, " Display | * effective format "))
            .row_highlight_style(
                Style::new()
                    .fg(color(p.selection_fg))
                    .bg(color(p.selection_bg))
                    .bold(),
            )
            .highlight_symbol("> ");
        frame.render_stateful_widget(
            table,
            body,
            &mut TableState::default().with_selected(app.display_menu),
        );
    } else if app.theme_menu.is_some() {
        let rows = onetui_theme::Theme::ALL.iter().map(|theme| {
            let name = serde_json::to_value(theme).unwrap();
            Row::new([name.as_str().unwrap().to_owned()])
        });
        let table = Table::new(rows, [Constraint::Fill(1)])
            .block(panel(p, " Themes | preview "))
            .row_highlight_style(
                Style::new()
                    .fg(color(p.selection_fg))
                    .bg(color(p.selection_bg))
                    .bold(),
            )
            .highlight_symbol("> ");
        let mut state = TableState::default().with_selected(Some(app.theme_index()));
        frame.render_stateful_widget(table, body, &mut state);
    } else if app.help {
        help(frame, body, app);
    } else if app.detail {
        frame.render_widget(
            wrapping(Paragraph::new(detail_spans(app)), app)
                .scroll((app.detail_scroll, if app.config.display.word_wrap { 0 } else { app.horizontal_scroll }))
                .block(panel(
                    p,
                    format!(
                        "Field {}/{} | chunk {}/{} | h/l fields, j/k scroll, n/p chunks, Esc back",
                        app.view.column + 1,
                        app.column_count(),
                        app.detail_chunk + 1,
                        app.detail_chunks
                    ),
                ).title_bottom(app.detail_notice)),
            body,
        );
    } else if app.row_detail {
        let index = app.view.selected_index().expect("selected row");
        let available = body.width.saturating_sub(6);
        let metadata_width = available / 4;
        let value_width = available - metadata_width * 2;
        let width = usize::from(value_width).max(1);
        let rows = (0..app.column_count()).map(|column| {
            let preview = if column == app.view.column
                && let Some(prepared) = &app.row_value
            {
                let bytes = app.view.page.rows[index].cells[column]
                    .as_ref()
                    .map_or(&[][..], onetui_core::Value::bytes);
                let height = usize::from(body.height.saturating_sub(3)).max(1);
                let (_, lines) = crate::row_value::viewport(
                    prepared,
                    bytes,
                    width,
                    app.config.display.word_wrap,
                    0,
                    0,
                );
                let scroll = app.row_scroll.min(lines.saturating_sub(height));
                let (text, _) = crate::row_value::viewport(
                    prepared,
                    bytes,
                    width,
                    app.config.display.word_wrap,
                    scroll,
                    height,
                );
                horizontal(&text, app)
            } else {
                wrap_preview(&app.view.previews[index][column], width, app)
            };
            let height = if column == app.view.column {
                preview.split('\n').count().max(1)
            } else {
                preview.split('\n').count().clamp(1, 3)
            } as u16;
            Row::new([
                Cell::from(app.column_name(column).to_owned())
                    .style(Style::new().fg(color(p.identifier))),
                Cell::from(
                    app.view
                        .page
                        .columns
                        .get(column)
                        .map_or("metadata", |c| c.datatype.as_str()),
                ),
                Cell::from(preview),
            ])
            .height(height)
        });
        frame.render_stateful_widget(
            Table::new(
                rows,
                [
                    Constraint::Length(metadata_width),
                    Constraint::Length(metadata_width),
                    Constraint::Length(value_width),
                ],
            )
            .header(
                Row::new(["FIELD", "TYPE", "VALUE"])
                    .style(Style::new().fg(color(p.table_heading)).bold()),
            )
            .block(panel(
                p,
                format!(
                    " Row data | {} fields | PgUp/PgDn value | Enter full value, Esc back ",
                    app.column_count()
                ),
            ))
            .row_highlight_style(
                Style::new()
                    .fg(color(p.selection_fg))
                    .bg(color(p.selection_bg))
                    .bold(),
            )
            .highlight_symbol("> "),
            body,
            &mut TableState::default().with_selected(Some(app.view.column)),
        );
    } else {
        let descriptor = app.descriptor();
        let start = app.view.column / 4 * 4;
        let end = (start + 4).min(app.column_count());
        let widths = table_widths(app, body);
        let rows = app.view.visible.iter().map(|&index| {
            let row = &app.view.projections[index];
            let mut height = 1;
            let cells: Vec<_> = row
                .iter()
                .enumerate()
                .skip(start)
                .take(end - start)
                .map(|(column, cell)| {
                    let preview = wrap_preview(
                        &app.view.previews[index][column],
                        widths[column - start].max(1) as usize,
                        app,
                    );
                    height = height.max(preview.lines().count().min(3) as u16);
                    Cell::from(preview).style(Style::new().fg(if !app.config.display.highlight {
                        color(p.text)
                    } else if cell.is_none() {
                        color(p.muted)
                    } else if column == 0 {
                        color(p.identifier)
                    } else {
                        color(p.text)
                    }))
                })
                .collect();
            Row::new(cells)
                .height(height)
                .style(Style::new().bg(if index % 2 == 0 {
                    color(p.background)
                } else {
                    color(p.surface)
                }))
        });
        let table = Table::new(rows, widths.iter().copied().map(Constraint::Length))
            .header(
                Row::new((start..end).map(|i| {
                    format!(
                        "{}{}{}",
                        if i == app.view.column { "> " } else { "" },
                        app.column_name(i),
                        match app.view.sort {
                            Some((column, false)) if column == i => " ↑",
                            Some((column, true)) if column == i => " ↓",
                            _ => "",
                        }
                    )
                }))
                .style(
                    Style::new()
                        .fg(color(p.table_heading))
                        .add_modifier(Modifier::BOLD),
                ),
            )
            .row_highlight_style(
                Style::new()
                    .fg(color(p.selection_fg))
                    .bg(color(p.selection_bg))
                    .bold(),
            )
            .highlight_symbol("> ")
            .block(panel(
                p,
                format!(
                    " {} [{} shown / {} loaded]{}{}{} ",
                    descriptor.id,
                    app.view.visible.len(),
                    app.view.page.rows.len(),
                    if app.view.previews_limited {
                        " | preview limit; Enter for detail"
                    } else {
                        ""
                    },
                    if app.view.filter.is_empty() {
                        String::new()
                    } else {
                        format!(" | filter: {:?}", display(&app.view.filter))
                    },
                    if app.query_editor.is_some() {
                        " | retained data"
                    } else {
                        ""
                    }
                ),
            ));
        let mut state = TableState::default().with_selected(if app.view.visible.is_empty() {
            None
        } else {
            Some(app.view.selected)
        });
        frame.render_stateful_widget(table, body, &mut state);
    }
    let state = if app.connection_form.is_some() {
        "Adding connection"
    } else if app.view.alias.is_none() && app.view.page.rows.is_empty() {
        "a add connection | q quit"
    } else if app.following {
        "LIVE | f / Ctrl-c stop | navigation pauses"
    } else if app.view.live && !app.loading {
        "Following stopped | f starts at current end | r returns to historical browsing"
    } else if app.loading {
        "Loading… Ctrl-c cancels"
    } else if app.view.page.rows.is_empty() {
        "Empty result"
    } else if app.view.visible.is_empty() {
        "No matches on this page"
    } else {
        "Ready"
    };
    let error = app.error.as_deref().unwrap_or(state);
    let config_path = app
        .config
        .path()
        .map(|path| format!("Config: {}", display(&path.display().to_string())));
    let scope = if app.view.resource.id == "connections" {
        config_path.as_deref().unwrap_or("configured aliases")
    } else {
        if app.view.page.notice.is_empty() {
            "metadata; offset pages, no cross-request snapshot"
        } else {
            &app.view.page.notice
        }
    };
    let version_text = concat!("v", env!("CARGO_PKG_VERSION"));
    let [status, version] = Layout::horizontal([
        Constraint::Min(1),
        Constraint::Length(if area.width >= 40 {
            version_text.len() as u16 + 1
        } else {
            0
        }),
    ])
    .areas(status);
    let [_, version] = Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(version);
    frame.render_widget(
        Paragraph::new(version_text)
            .right_aligned()
            .style(Style::new().fg(color(p.muted))),
        version,
    );
    frame.render_widget(
        wrapping(
            Paragraph::new(vec![
                Line::styled(
                    error,
                    Style::new().fg(if app.error.is_some() {
                        color(p.error)
                    } else if app.loading && !app.following {
                        color(p.warning)
                    } else {
                        color(p.success)
                    }),
                ),
                Line::raw(if app.view.live {
                    format!(
                        "Live window | {} retained | {} evicted locally | {scope}",
                        app.view.page.rows.len(),
                        app.view.live_evicted
                    )
                } else {
                    format!(
                        "Page {} | {} items | next: {} | {scope}",
                        app.view.offset / PAGE_SIZE + 1,
                        app.view.page.rows.len(),
                        app.view.page.next,
                    )
                }),
            ]),
            app,
        )
        .style(Style::new().fg(color(p.muted))),
        status,
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn truncated_previews_keep_the_marker_inside_the_column() {
        use super::*;
        use unicode_width::UnicodeWidthStr;
        let config = onetui_core::config::Config::parse(
            "[connections.sample]\nkind='fake'",
            crate::test_provider::CATALOG,
        )
        .unwrap();
        let app = App::new(config, None);
        for source in [
            "x".repeat(100),
            "界".repeat(100),
            "e\u{301}".repeat(100),
            "1234\n1234\n1234\nlast".into(),
        ] {
            for width in [1, 2, 4, 12] {
                let preview = wrap_preview(&source, width, &app);
                assert!(preview.lines().count() <= 3);
                assert!(
                    preview.lines().all(|line| line.width() <= width),
                    "width={width}: {preview:?}"
                );
                assert!(preview.ends_with('…'), "{preview:?}");
            }
        }
        assert_eq!(
            wrap_preview("1234\n1234\n1234", 4, &app),
            "1234\n1234\n1234"
        );
        assert_eq!(wrap_preview("value", 0, &app), "");
    }

    #[test]
    fn compact_json_and_short_columns_leave_room_for_overflowing_data() {
        use super::*;
        use onetui_core::{Column, Page, Resource, Row as DataRow, Value};
        use ratatui::backend::TestBackend;
        let config = onetui_core::config::Config::parse(
            "[connections.sample]\nkind='fake'",
            crate::test_provider::CATALOG,
        )
        .unwrap();
        let mut app = App::new(config, Some("sample"));
        app.view.resource = Resource::new("fake.rows", vec![]);
        let request = app.request.take().unwrap();
        let raw = format!(
            "{{\n  \"customer\": {{\"id\": 21, \"name\": \"{}\"}},\n  \"tail\": true\n}}",
            "x".repeat(180)
        );
        app.complete(
            &request,
            Ok(Page {
                columns: [
                    "headers",
                    "value_decoded",
                    "value_schema",
                    "value_decode_error",
                ]
                .map(|name| Column {
                    name: name.into(),
                    datatype: "text".into(),
                })
                .into(),
                rows: (0..2)
                    .map(|_| DataRow {
                        cells: vec![
                            Some(Value::Json("[]".into())),
                            Some(Value::Json(raw.clone())),
                            Some("confluent:http://127.0.0.1:18081#id=2".into()),
                            None,
                        ],
                        target: None,
                    })
                    .collect(),
                ..Page::default()
            }),
        );
        assert!(!app.view.previews[0][1].contains('\n'));
        assert!(app.view.previews[0][1].contains("\"tail\":true"));
        app.viewport = Rect::new(0, 0, 180, 30);
        let body = panels(app.viewport, &app)[2];
        let widths = table_widths(&app, body);
        assert!(widths[1] > widths[2] * 2, "{widths:?}");
        assert_eq!(widths[0], 11);
        assert_eq!(widths[3], 22);
        let cached = app.view.previews.as_ptr();
        app.act(Action::Down);
        app.act(Action::Right);
        app.act(Action::Sort);
        assert_eq!(table_widths(&app, body), widths);
        assert_eq!(app.view.previews.as_ptr(), cached);
        let mut terminal = Terminal::new(TestBackend::new(180, 30)).unwrap();
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let shown: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol())
            .collect();
        assert!(shown.contains("\"tail\":true"), "{shown}");
        app.act(Action::Open);
        assert!(app.row_detail);
        assert!(app.view.previews[0][1].contains('\n'));
        assert!(
            app.row_value
                .as_ref()
                .unwrap()
                .text
                .contains("\n  \"customer\"")
        );
        app.act(Action::Open);
        assert!(app.detail && app.detail_text.contains("\n  \"customer\""));
        app.act(Action::Back);
        app.act(Action::Back);
        assert!(!app.view.previews[0][1].contains('\n'));
        assert_eq!(table_widths(&app, body), widths);
        assert_eq!(app.view.page.rows[0].cells[1], Some(Value::Json(raw)));
        assert!(app.request.is_none());
        for row in &mut app.view.previews {
            row[1] = "界".repeat(500);
            row[3] = "e\u{301}".repeat(500);
        }
        for width in [0, 1, 8, 40, 180, 240] {
            let widths = table_widths(&app, Rect::new(0, 0, width, 30));
            assert_eq!(widths.iter().sum::<u16>(), width.saturating_sub(7));
            assert!(widths[1].abs_diff(widths[3]) <= 1, "{widths:?}");
        }
    }

    #[test]
    fn table_previews_use_the_available_width_after_resize() {
        use super::*;
        use onetui_core::{Column, Page, Resource, Row as DataRow};
        use ratatui::backend::TestBackend;
        for columns in [1, 2, 4] {
            let config = onetui_core::config::Config::parse(
                "[connections.sample]\nkind='fake'",
                crate::test_provider::CATALOG,
            )
            .unwrap();
            let mut app = App::new(config, Some("sample"));
            app.view.resource = Resource::new("fake.rows", vec![]);
            let request = app.request.take().unwrap();
            let payload = format!("{}END", "x".repeat(400));
            app.complete(
                &request,
                Ok(Page {
                    columns: (0..columns)
                        .map(|i| Column {
                            name: format!("c{i}"),
                            datatype: "text".into(),
                        })
                        .collect(),
                    rows: vec![DataRow {
                        cells: (0..columns)
                            .map(|i| {
                                Some(
                                    if i == columns - 1 {
                                        payload.clone()
                                    } else {
                                        "1".into()
                                    }
                                    .into(),
                                )
                            })
                            .collect(),
                        target: Some(Resource::new("fake.rows", vec!["selected".into()])),
                    }],
                    ..Page::default()
                }),
            );
            let cached = app.view.previews.as_ptr();
            for width in [80, 240, 80, 240] {
                let mut terminal = Terminal::new(TestBackend::new(width, 24)).unwrap();
                terminal.draw(|frame| draw(frame, &app)).unwrap();
                let lines: Vec<String> = terminal
                    .backend()
                    .buffer()
                    .content
                    .chunks(usize::from(width))
                    .map(|line| line.iter().map(|c| c.symbol()).collect())
                    .collect();
                assert_eq!(
                    lines.iter().any(|line| line.contains("END")),
                    width == 240,
                    "columns={columns}, width={width}: {lines:?}"
                );
                let data: Vec<_> = lines.iter().filter(|line| line.contains("xxxx")).collect();
                assert!(data.len() <= 3);
                assert!(data[0].ends_with("xxxx│"), "{}", data[0]);
                if width == 80 {
                    assert!(data.last().unwrap().ends_with("…│"));
                }
                assert_eq!(app.view.previews.as_ptr(), cached);
            }
            assert_eq!(
                app.view.page.rows[0].cells[columns - 1]
                    .as_ref()
                    .unwrap()
                    .text(),
                Some(payload.as_str())
            );
            let mut page = std::mem::take(&mut app.view.page);
            page.rows = vec![page.rows[0].clone(); 100];
            app.complete(&request, Ok(page));
            for (width, step) in [(80, 4), (240, 6)] {
                app.viewport = Rect::new(0, 0, width, 24);
                assert_eq!(page_step(&app, true, false), step);
                let widths = table_widths(&app, panels(app.viewport, &app)[2]);
                app.view.column = columns - 1;
                app.view.visible.truncate(1);
                assert_eq!(table_widths(&app, panels(app.viewport, &app)[2]), widths);
                app.view.visible = (0..100).collect();
            }
        }
    }

    #[test]
    fn focused_row_value_expands_and_scrolls_without_changing_records() {
        use super::*;
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use onetui_core::{Column, Page, Resource, Row as DataRow, Value, catalog::Action};
        use ratatui::backend::TestBackend;
        let config = onetui_core::config::Config::parse(
            "[connections.sample]\nkind='fake'",
            crate::test_provider::CATALOG,
        )
        .unwrap();
        let mut app = App::new(config, Some("sample"));
        app.view.resource = Resource::new("fake.rows", vec![]);
        let request = app.request.take().unwrap();
        let value = Value::Bytes(
            serde_json::to_vec(
                &(0..100)
                    .map(|i| format!("entry_{i:03}"))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        );
        app.complete(
            &request,
            Ok(Page {
                columns: vec![
                    Column {
                        name: "payload".into(),
                        datatype: "bytes".into(),
                    },
                    Column {
                        name: "other".into(),
                        datatype: "text".into(),
                    },
                ],
                rows: vec![DataRow {
                    cells: vec![Some(value.clone()), Some("other field".into())],
                    target: None,
                }],
                ..Page::default()
            }),
        );
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
        let render = |app: &mut App, terminal: &mut Terminal<TestBackend>| {
            terminal
                .draw(|frame| {
                    app.viewport = frame.area();
                    draw(frame, app)
                })
                .unwrap();
            terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|c| c.symbol())
                .collect::<String>()
        };
        assert!(!render(&mut app, &mut terminal).contains("entry_099"));
        app.act(Action::Open);
        let shown = render(&mut app, &mut terminal);
        assert!(
            shown.contains("Row data") && shown.contains("entry_004"),
            "{shown}"
        );
        assert!(!shown.contains('…'));
        let cached = app.row_value.as_ref().unwrap().text.as_ptr();
        app.key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL));
        let halfway = app.row_scroll;
        assert!(halfway > 0);
        app.key(KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE));
        assert!(app.row_scroll > halfway);
        for _ in 0..100 {
            app.act(Action::PageDown);
        }
        assert!(render(&mut app, &mut terminal).contains("entry_099"));
        assert_eq!(app.row_value.as_ref().unwrap().text.as_ptr(), cached);
        assert_eq!(app.view.column, 0);
        assert_eq!(app.view.selected, 0);
        assert_eq!(app.view.page.rows[0].cells[0], Some(value));
        app.act(Action::Left);
        assert!(
            app.row_scroll > 0,
            "field boundary must not reset value scrolling"
        );
        for (command, pretty) in [
            ("display pretty-print off", false),
            ("display pretty-print on", true),
        ] {
            for c in format!(":{command}").chars() {
                app.key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
            }
            app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
            assert_eq!(app.row_value.as_ref().unwrap().text.contains('\n'), pretty);
            assert_eq!(app.row_scroll, 0);
        }
        app.act(Action::Open);
        assert!(app.detail);
        app.act(Action::Back);
        assert!(app.row_detail && !app.detail && app.row_value.is_some());
        for _ in 0..100 {
            app.act(Action::PageUp);
        }
        assert!(render(&mut app, &mut terminal).contains("entry_000"));
        app.act(Action::Down);
        assert_eq!(app.view.column, 1);
        assert_eq!(app.row_scroll, 0);
        assert!(render(&mut app, &mut terminal).contains("other field"));
        app.act(Action::Up);
        app.act(Action::PageDown);
        for width in [1, 20, 60, 160] {
            let mut resized = Terminal::new(TestBackend::new(width, 24)).unwrap();
            render(&mut app, &mut resized);
            app.act(Action::PageUp);
        }
        app.act(Action::Back);
        assert!(!app.row_detail && app.row_value.is_none());
        assert!(app.request.is_none());
    }

    #[test]
    fn row_inspector_lists_fields_beyond_the_four_column_window() {
        use super::*;
        use onetui_core::catalog::Action;
        use onetui_core::{Column, Page, Resource, Row as DataRow};
        use ratatui::backend::TestBackend;
        let config = onetui_core::config::Config::parse(
            "[connections.sample]\nkind='fake'",
            crate::test_provider::CATALOG,
        )
        .unwrap();
        let mut app = App::new(config, Some("sample"));
        app.view.resource = Resource::new("fake.rows", vec![]);
        let request = app.request.take().unwrap();
        app.complete(
            &request,
            Ok(Page {
                columns: (0..65)
                    .map(|i| Column {
                        name: format!("field_{i}"),
                        datatype: "text".into(),
                    })
                    .collect(),
                rows: vec![DataRow {
                    cells: (0..65).map(|i| Some(format!("value_{i}").into())).collect(),
                    target: None,
                }],
                ..Page::default()
            }),
        );
        app.act(Action::Open);
        assert!(app.row_detail && !app.detail);
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).unwrap();
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol())
            .collect();
        assert!(text.contains("65 fields") && text.contains("field_0") && text.contains("value_0"));
        app.view.page.columns[0].datatype = "timestamp with time zone".into();
        app.view.previews[0][0] = "x".repeat(120);
        app.view.page.rows[0].cells[0] = Some("x".repeat(120).into());
        app.act(Action::Right);
        app.act(Action::Left);
        for width in [160, 240] {
            let mut wide = Terminal::new(TestBackend::new(width, 24)).unwrap();
            wide.draw(|frame| draw(frame, &app)).unwrap();
            let lines: Vec<String> = wide
                .backend()
                .buffer()
                .content
                .chunks(usize::from(width))
                .map(|line| line.iter().map(|c| c.symbol()).collect())
                .collect();
            let heading = lines.iter().find(|line| line.contains("FIELD")).unwrap();
            let value_x = heading.chars().position(|c| c == 'V').unwrap();
            assert!(value_x >= usize::from(width / 2), "{heading}");
            let row = lines.iter().find(|line| line.contains("field_0")).unwrap();
            assert!(row.contains("timestamp with time zone"), "{row}");
            assert_eq!(row.chars().nth(usize::from(width - 2)), Some('x'));
        }
        assert_eq!(page_step(&app, true, false), 8);
        assert_eq!(page_step(&app, true, true), 2);
        app.config.display.word_wrap = false;
        assert_eq!(page_step(&app, true, false), 11);
        app.config.display.word_wrap = true;
        app.viewport = Rect::new(0, 0, 1, 1);
        assert_eq!(page_step(&app, true, true), 1);
        for _ in 0..64 {
            app.act(Action::Down);
        }
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol())
            .collect();
        assert!(text.contains("field_64") && text.contains("value_64"));
        for width in [1, 20, 60] {
            let mut narrow = Terminal::new(TestBackend::new(width, 20)).unwrap();
            narrow.draw(|frame| draw(frame, &app)).unwrap();
        }
        assert!(app.request.is_none());
    }

    #[test]
    fn display_rendering_wrap_highlight_and_scrolling_are_independent() {
        use super::*;
        use onetui_core::catalog::Action;
        use ratatui::backend::TestBackend;
        let contents = |terminal: &ratatui::Terminal<TestBackend>| {
            terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|c| c.symbol())
                .collect::<String>()
        };
        let mut app = App::new(
            onetui_core::config::Config::parse(
                "[connections.sample]\nkind='fake'",
                crate::test_provider::CATALOG,
            )
            .unwrap(),
            Some("sample"),
        );
        app.view.resource = onetui_core::Resource::new("fake.rows", vec![]);
        let request = app.request.take().unwrap();
        app.complete(
            &request,
            Ok(onetui_core::Page {
                columns: vec![onetui_core::Column {
                    name: "payload".into(),
                    datatype: "json".into(),
                }],
                rows: vec![onetui_core::Row {
                    cells: vec![Some(onetui_core::Value::Json(
                        "{\"emoji\":\"🌊\",\"control\":\"\\u001b[31m\",\"values\":[1,2,3]}".into(),
                    ))],
                    target: None,
                }],
                ..onetui_core::Page::default()
            }),
        );
        app.act(Action::Open);
        let cached = app.detail_text.as_ptr();
        for width in [1, 12, 60, 160] {
            for wrap in [true, false] {
                for highlight in [true, false] {
                    app.config.display.word_wrap = wrap;
                    app.config.display.highlight = highlight;
                    let mut terminal = ratatui::Terminal::new(TestBackend::new(width, 24)).unwrap();
                    terminal.draw(|frame| draw(frame, &app)).unwrap();
                    assert!(!contents(&terminal).contains('\x1b'));
                    assert_eq!(app.detail_text.as_ptr(), cached);
                    let spans = detail_spans(&app);
                    let plain = spans
                        .iter()
                        .map(|l| {
                            l.spans
                                .iter()
                                .map(|s| s.content.as_ref())
                                .collect::<String>()
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    assert_eq!(plain, app.detail_text);
                    if !highlight {
                        assert!(
                            spans
                                .iter()
                                .flat_map(|l| &l.spans)
                                .all(|s| s.style == Style::default())
                        );
                    }
                }
            }
        }
        app.act(Action::ScrollRight);
        assert_eq!(app.horizontal_scroll, 8);
        app.act(Action::Display);
        let mut terminal = ratatui::Terminal::new(TestBackend::new(120, 28)).unwrap();
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        assert!(contents(&terminal).contains("word-wrap"));
        assert!(contents(&terminal).contains("binary"));
        assert!(app.command.is_none());
    }
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};
    use std::io::Write;

    #[test]
    fn query_draft_and_results_remain_visible_through_execution_and_errors() {
        let contents = |terminal: &Terminal<TestBackend>| {
            terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|c| c.symbol())
                .collect::<String>()
        };
        let mut app = App::new(
            onetui_core::config::Config::parse(
                "[connections.sample]\nkind='fake'",
                crate::test_provider::CATALOG,
            )
            .unwrap(),
            Some("sample"),
        );
        let page = || Page {
            columns: vec![onetui_core::Column {
                name: "result_value".into(),
                datatype: "text".into(),
            }],
            rows: ["retained_one", "retained_two"]
                .into_iter()
                .map(|v| onetui_core::Row {
                    cells: vec![Some(v.into())],
                    target: None,
                })
                .collect(),
            ..Page::default()
        };
        let request = app.request.take().unwrap();
        app.complete(&request, Ok(page()));
        app.act(Action::Query);
        app.query_editor = Some(crate::query::Editor::new("SELECT result_value".into()));
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = contents(&terminal);
        assert!(text.contains("SELECT result_value") && text.contains("retained_one"));
        assert!(text.contains("Enter/F5") && text.contains("Shift-Enter"));
        let [editor_area, rows_area] =
            query_panels(panels(terminal.backend().buffer().area, &app)[2], &app);
        assert!(editor_area.height < rows_area.height);
        assert_eq!(editor_area.bottom(), rows_area.y);
        app.key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('r'),
            crossterm::event::KeyModifiers::CONTROL,
        ));
        let request = app.request.take().expect("Ctrl-r executes the query");
        assert_eq!(request.query.as_deref(), Some("SELECT result_value"));
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = contents(&terminal);
        assert!(text.contains("Loading") && text.contains("retained_one"));
        app.complete(&request, Err(anyhow!("native query error")));
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = contents(&terminal);
        assert!(text.contains("native query error") && text.contains("retained_one"));
        app.key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::F(5),
            crossterm::event::KeyModifiers::NONE,
        ));
        let request = app.request.take().unwrap();
        app.complete(&request, Ok(page()));
        assert!(app.query_editor.is_none());
        for (width, height) in [(120, 30), (40, 12)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            terminal.draw(|frame| draw(frame, &app)).unwrap();
            let text = contents(&terminal);
            assert!(text.contains("SELECT result_value") && text.contains("retained_two"));
            assert!(
                !terminal
                    .backend()
                    .buffer()
                    .content
                    .iter()
                    .any(|c| c.modifier.contains(Modifier::REVERSED))
            );
        }
        app.act(Action::Query);
        app.key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::F(5),
            crossterm::event::KeyModifiers::NONE,
        ));
        let request = app.request.take().unwrap();
        let mut single = page();
        single.rows.truncate(1);
        app.complete(&request, Ok(single));
        assert!(
            !app.detail,
            "one-cell queries still show the query and result table"
        );
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = contents(&terminal);
        assert!(text.contains("SELECT result_value") && text.contains("retained_one"));
        app.act(Action::Refresh);
        let request = app.request.take().unwrap();
        let mut many = page();
        many.rows.resize(100, many.rows[0].clone());
        app.complete(&request, Ok(many));
        app.viewport = Rect::new(0, 0, 120, 30);
        let rows_area = query_panels(panels(app.viewport, &app)[2], &app)[1];
        let step = usize::from(rows_area.height - 3);
        assert_eq!(page_step(&app, true, false), step);
        app.act(Action::PageDown);
        assert_eq!(app.view.selected, step);
    }

    #[test]
    fn query_editor_uses_content_panel_and_keeps_caret_visible() {
        let mut app = App::new(
            onetui_core::config::Config::parse(
                "[connections.sample]\nkind='fake'",
                crate::test_provider::CATALOG,
            )
            .unwrap(),
            Some("sample"),
        );
        let request = app.request.take().unwrap();
        app.complete(&request, Ok(Page::default()));
        app.act(Action::Query);
        app.query_editor = Some(crate::query::Editor::new(format!(
            "{}\nlast_line_София",
            "long query ".repeat(40)
        )));
        for (width, height) in [(120, 24), (40, 12), (10, 4)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            terminal.draw(|frame| draw(frame, &app)).unwrap();
            let buffer = terminal.backend().buffer();
            let [_, command, body, _] = panels(buffer.area, &app);
            assert_eq!(command.height, 0);
            if body.height > 2 {
                assert!(
                    buffer
                        .content
                        .iter()
                        .any(|cell| cell.modifier.contains(Modifier::REVERSED))
                );
            }
            let text = buffer
                .content
                .iter()
                .map(|c| c.symbol())
                .collect::<String>();
            assert!(!text.contains("Filter displayed page"));
            if width == 120 {
                assert!(text.contains("F5") && text.contains("execute read-only query"));
                assert!(text.contains("last_line_София"));
            }
        }
    }

    #[test]
    fn mode_key_hints_and_help_columns_are_aligned() {
        let mut app = App::new(
            onetui_core::config::Config::parse("[connections]", crate::test_provider::CATALOG)
                .unwrap(),
            None,
        );
        let p = app.config.theme.palette();
        let mut terminal = Terminal::new(TestBackend::new(180, 40)).unwrap();
        app.filter_input = Some(String::new());
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let buffer = terminal.backend().buffer();
        let mut key_columns = Vec::new();
        for key in ["Enter", "Esc", "Backspace"] {
            let (y, x) = (1..8)
                .find_map(|y| {
                    let line = (0..180)
                        .map(|x| buffer[(x, y)].symbol())
                        .collect::<String>();
                    line.find(key).map(|x| (y, x as u16))
                })
                .unwrap();
            key_columns.push(x);
            assert_eq!(buffer[(x, y)].fg, color(p.key_hint));
            assert!(buffer[(x, y)].modifier.contains(Modifier::BOLD));
            assert_eq!(buffer[(x + 11, y)].fg, color(p.muted));
        }
        assert!(key_columns.windows(2).all(|pair| pair[0] == pair[1]));
        app.filter_input = None;
        for width in [20, 60, 80, 180] {
            let mut terminal = Terminal::new(TestBackend::new(width, 60)).unwrap();
            terminal
                .draw(|frame| help(frame, frame.area(), &app))
                .unwrap();
            let buffer = terminal.backend().buffer();
            let text = buffer
                .content
                .chunks(width as usize)
                .map(|row| row.iter().map(|c| c.symbol()).collect::<String>())
                .collect::<Vec<_>>();
            if width >= 80 {
                let command = text[1].find("COMMAND").unwrap();
                let description = text[1].find("DESCRIPTION").unwrap();
                assert_eq!(text[3].find("themes"), Some(command));
                assert_eq!(text[3].find("Choose"), Some(description));
                assert!(text.iter().any(|line| line.contains("Ctrl-c")));
                let theme_description = text[3..]
                    .iter()
                    .take_while(|line| !line.contains("filter  "))
                    .map(|line| line[description..].trim_matches([' ', '│']))
                    .collect::<Vec<_>>()
                    .join(" ");
                assert!(theme_description.contains("config is unchanged"));
            } else if width == 60 {
                assert!(text.join("").contains(":themes"));
            }
            assert!(text.join("").contains("themes"));
        }
    }

    #[test]
    fn command_bar_stays_above_table_and_scrolls_long_input() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        let mut app = App::new(
            onetui_core::config::Config::parse(
                "[connections.a4b]\nkind='fake'\n[connections.other]\nkind='fake'",
                crate::test_provider::CATALOG,
            )
            .unwrap(),
            None,
        );
        let mut terminal = Terminal::new(TestBackend::new(120, 24)).unwrap();
        let lines = |terminal: &Terminal<TestBackend>| {
            terminal
                .backend()
                .buffer()
                .content
                .chunks(120)
                .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
                .collect::<Vec<_>>()
        };
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = lines(&terminal);
        assert!(text[8].contains("connections [2 shown / 2 loaded]"));
        assert!(!text.join("").contains("Page-local:"));
        app.act(Action::Filter);
        for c in "4b".chars() {
            app.key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = lines(&terminal);
        assert!(text[0].contains("Context"));
        assert!(text[..8].join("").contains("Filter displayed page"));
        assert_eq!(text[8], format!("╭{}╮", "─".repeat(118)));
        assert!(text[9].contains("/4b"));
        assert!(text[..8].join("").contains("keep filter (live)"));
        assert_eq!(text[10], format!("╰{}╯", "─".repeat(118)));
        assert!(text[11].contains("connections [1 shown / 2 loaded]"));
        assert!(text[22].contains("Ready"));
        assert!(text[23].contains("Page 1"));
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = lines(&terminal);
        assert!(text[8].contains("connections [1 shown / 2 loaded] | filter: \"4b\""));
        assert!(!text[22..].join("").contains("filter:"));
        app.key(KeyEvent::new(KeyCode::Char(':'), KeyModifiers::NONE));
        app.command = Some("refresh".into());
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        assert!(lines(&terminal)[9].contains(":refresh"));
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(app.view.filter, "4b");

        app.act(Action::Filter);
        app.key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        app.key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        app.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        assert!(lines(&terminal)[8].contains("connections [2 shown / 2 loaded]"));
        app.act(Action::Sort);
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        assert!(lines(&terminal)[8].contains("connections [2 shown / 2 loaded]"));
        assert!(lines(&terminal)[9].contains("alias ↑"));
        app.act(Action::Sort);
        app.act(Action::Sort);
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        assert!(lines(&terminal)[8].contains("connections [2 shown / 2 loaded]"));
        app.key(KeyEvent::new(KeyCode::Char(':'), KeyModifiers::NONE));
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = lines(&terminal);
        assert_eq!(text[8], format!("╭{}╮", "─".repeat(118)));
        assert_eq!(text[9], format!("│:{}│", " ".repeat(117)));
        assert_eq!(text[10], format!("╰{}╯", "─".repeat(118)));
        app.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        assert!(lines(&terminal)[8].contains("connections [2 shown / 2 loaded]"));

        app.act(Action::Filter);
        app.filter_input = Some(format!("{}\u{001b}END", "🌊".repeat(50)));
        for (width, height) in [(1, 1), (20, 6), (20, 12), (60, 20)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            terminal.draw(|frame| draw(frame, &app)).unwrap();
            let text = terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|c| c.symbol())
                .collect::<String>();
            assert!(!text.contains('\u{001b}'));
            if width >= 20 {
                assert!(
                    text.contains("\\u{1b}END"),
                    "edited tail hidden at {width}x{height}: {text}"
                );
            }
        }
    }

    #[test]
    fn theme_menu_lists_all_names_and_tracks_preview_on_small_screens() {
        let mut app = App::new(
            onetui_core::config::Config::parse("[connections]", crate::test_provider::CATALOG)
                .unwrap(),
            None,
        );
        app.act(Action::Themes);
        for (width, height) in [(1, 1), (20, 6), (80, 24), (120, 32)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            for _ in onetui_theme::Theme::ALL {
                terminal.draw(|frame| draw(frame, &app)).unwrap();
                if width == 120 {
                    let buffer = terminal.backend().buffer();
                    let row = |y: u16| {
                        (0..width)
                            .map(|x| buffer[(x, y)].symbol())
                            .collect::<String>()
                    };
                    assert!(row(8).contains("Themes | preview"));
                    assert!(row(9).contains("catppuccin"));
                    let text = buffer
                        .content
                        .iter()
                        .map(|c| c.symbol())
                        .collect::<String>();
                    for theme in onetui_theme::Theme::ALL {
                        let name = serde_json::to_value(theme).unwrap();
                        assert!(text.contains(name.as_str().unwrap()));
                    }
                    assert!(text.contains("Config file is unchanged"));
                    assert!(buffer.content.iter().any(|cell| cell.symbol() == ">"
                        && cell.bg == color(app.config.theme.palette().selection_bg)));
                }
                app.act(Action::Down);
            }
        }
        app.act(Action::Back);
        assert_eq!(app.config.theme, onetui_theme::Theme::Catppuccin);
    }

    #[test]
    fn context_layout_colors_and_input_mode_match_visible_state() {
        for theme in onetui_theme::Theme::ALL {
            assert_context_layout(theme);
        }
    }

    #[test]
    fn paging_hints_keep_their_positions_when_loading_or_at_page_boundaries() {
        use super::*;
        use ratatui::backend::TestBackend;
        let config = onetui_core::config::Config::parse(
            "[connections.sample]\nkind='fake'",
            crate::test_provider::CATALOG,
        )
        .unwrap();
        let mut app = App::new(config, Some("sample"));
        let request = app.request.take().unwrap();
        let page = Page {
            rows: vec![onetui_core::Row {
                cells: vec![Some("public".into())],
                target: Some(onetui_core::Resource::new(
                    "fake.relations",
                    vec!["public".into()],
                )),
            }],
            next: true,
            ..Page::default()
        };
        app.complete(&request, Ok(page.clone()));
        let mut terminal = Terminal::new(TestBackend::new(180, 8)).unwrap();
        let render = |terminal: &mut Terminal<TestBackend>, app: &App| {
            terminal
                .draw(|frame| context(frame, frame.area(), app))
                .unwrap();
            terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>()
        };
        let first = render(&mut terminal, &app);
        assert!(first.contains("next") && first.contains("previous"));
        let previous = first.find("previous").unwrap();
        assert!(!app.available(Action::Previous));
        app.act(Action::Previous);
        assert!(app.request.is_none());
        app.act(Action::Next);
        assert!(app.loading);
        assert_eq!(first, render(&mut terminal, &app));
        let pending = app.request.take().unwrap();
        let generation = app.generation;
        app.act(Action::Next);
        assert_eq!(app.generation, generation);
        assert!(app.request.is_none());
        let mut last_page = page;
        last_page.next = false;
        app.complete(&pending, Ok(last_page));
        assert!(app.available(Action::Previous));
        assert!(!app.available(Action::Next));
        assert_eq!(first, render(&mut terminal, &app));
        app.act(Action::Next);
        assert!(app.request.is_none());
        app.act(Action::Previous);
        assert_eq!(first, render(&mut terminal, &app));
        let buffer = terminal.backend().buffer();
        let description = first[..previous].chars().count();
        let key = buffer.content[..description]
            .iter()
            .rev()
            .find(|cell| cell.symbol() == "p")
            .unwrap();
        assert_eq!(key.fg, color(app.config.theme.palette().muted));
    }

    fn assert_context_layout(theme: onetui_theme::Theme) {
        let p = theme.palette();
        let mut app = App::new(
            onetui_core::config::Config::parse(
                "[connections.sample]\nkind='fake'\nurl_env='DO_NOT_RENDER'",
                crate::test_provider::CATALOG,
            )
            .unwrap(),
            Some("sample"),
        );
        app.config.theme = theme;
        let request = app.request.take().unwrap();
        app.view.resource =
            onetui_core::Resource::new("fake.rows", vec!["public".into(), "orders".into()]);
        app.complete(
            &request,
            Ok(Page {
                columns: ["id", "city", "note"]
                    .into_iter()
                    .map(|name| onetui_core::Column {
                        name: name.into(),
                        datatype: "text".into(),
                    })
                    .collect(),
                rows: vec![onetui_core::Row {
                    cells: vec![Some("42".into()), Some("София\u{001b}".into()), None],
                    target: None,
                }],
                next: true,
                ..Page::default()
            }),
        );
        app.connection_status = Some(onetui_core::provider::ConnectionStatus::Connected);
        let mut terminal = Terminal::new(TestBackend::new(120, 24)).unwrap();
        let contents = |terminal: &Terminal<TestBackend>| {
            terminal
                .backend()
                .buffer()
                .content
                .chunks(120)
                .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
                .collect::<Vec<_>>()
                .join("\n")
        };
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = contents(&terminal);
        assert!(text.lines().next().unwrap().contains("Context | read-only"));
        assert!(!text.contains("OneTUI"));
        assert!(
            text.lines()
                .last()
                .unwrap()
                .ends_with(concat!("v", env!("CARGO_PKG_VERSION")))
        );
        for expected in [
            "Connection sample",
            "Datasource fake",
            "public / orders",
            "Connected",
            "1 shown / 1 loaded",
            "m      columns",
            "n      next",
            "София\\u{1b}",
        ] {
            assert!(text.contains(expected), "missing {expected}:\n{text}");
        }
        assert!(text.contains("previous"));
        assert!(!app.available(Action::Previous));
        assert!(!text.contains("Page-local:"));
        assert!(!text.contains("DO_NOT_RENDER"));
        assert!(!text.contains('\u{001b}'));
        let selected = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .find(|cell| cell.symbol() == "4" && cell.bg == color(p.selection_bg))
            .unwrap();
        assert_eq!(selected.fg, color(p.selection_fg));
        assert!(
            terminal
                .backend()
                .buffer()
                .content
                .iter()
                .any(|cell| cell.symbol() == "R" && cell.fg == color(p.success))
        );

        app.act(Action::Sort);
        app.loading = true;
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = contents(&terminal);
        assert!(text.contains("id ↑"));
        assert!(text.contains("Loading"));
        assert!(text.contains("m      columns"));
        assert!(!app.available(Action::Columns));
        assert!(!app.available(Action::Next));
        assert!(text.lines().any(|line| {
            line.split_whitespace()
                .collect::<Vec<_>>()
                .windows(2)
                .any(|pair| pair == ["n", "next"])
        }));
        assert!(
            terminal
                .backend()
                .buffer()
                .content
                .iter()
                .any(|cell| cell.symbol() == "L" && cell.fg == color(p.warning))
        );

        app.loading = false;
        app.error = Some("Request cancelled".into());
        app.act(Action::Filter);
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = contents(&terminal);
        assert!(text.contains("Request cancelled"));
        assert!(text.contains("Filter displayed page"));
        assert!(!text.contains("s      sort"));
        assert!(
            terminal
                .backend()
                .buffer()
                .content
                .iter()
                .any(|cell| cell.symbol() == "R" && cell.fg == color(p.error))
        );

        app.filter_input = None;
        app.command = Some("connections".into());
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        assert!(contents(&terminal).contains(":connections"));
        assert_eq!(terminal.backend().buffer()[(1, 9)].fg, color(p.key_hint));
        assert_eq!(terminal.backend().buffer()[(1, 9)].bg, color(p.surface));

        app.command = None;
        app.help = true;
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        assert!(contents(&terminal).contains("DESCRIPTION"));
        assert_eq!(
            terminal.backend().buffer()[(1, 9)].fg,
            color(p.table_heading)
        );
        assert_eq!(terminal.backend().buffer()[(1, 9)].bg, color(p.background));

        app.help = false;
        app.detail = true;
        app.detail_text = "Themed detail".into();
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        assert!(contents(&terminal).contains("Themed detail"));
        assert_eq!(terminal.backend().buffer()[(1, 9)].fg, color(p.text));
        assert_eq!(terminal.backend().buffer()[(1, 9)].bg, color(p.background));
    }

    #[test]
    #[ignore = "timing sample; run with --release --nocapture"]
    fn large_page_render_timings() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use std::time::Instant;

        let mut app = App::new(
            onetui_core::config::Config::parse(
                "[connections.sample]\nkind='fake'",
                crate::test_provider::CATALOG,
            )
            .unwrap(),
            Some("sample"),
        );
        let request = app.request.take().unwrap();
        app.view.resource = onetui_core::Resource::new("fake.rows", vec![]);
        let raw = format!("{}\n\u{001b}", "🌊".repeat(512));
        let started = Instant::now();
        let page = Page {
            columns: (0..4)
                .map(|i| onetui_core::Column {
                    name: format!("field{i}"),
                    datatype: "text".into(),
                })
                .collect(),
            rows: (0..100)
                .map(|_| onetui_core::Row {
                    cells: (0..4).map(|_| Some(raw.clone().into())).collect(),
                    target: None,
                })
                .collect(),
            ..Page::default()
        };
        let format_time = started.elapsed();
        let bytes = page.bytes();
        assert!(bytes <= onetui_core::PAGE_BYTES);
        let started = Instant::now();
        app.complete(&request, Ok(page));
        let install_time = started.elapsed();
        assert_eq!(app.view.page.rows.len(), 100);
        let cached_cell = app.view.page.rows[0].cells[0]
            .as_ref()
            .unwrap()
            .bytes()
            .as_ptr();
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let mut unchanged = Vec::new();
        let mut input_draw = Vec::new();
        app.act(Action::Refresh);
        assert!(app.loading);
        for _ in 0..100 {
            let started = Instant::now();
            terminal.draw(|frame| draw(frame, &app)).unwrap();
            unchanged.push(started.elapsed());
            let started = Instant::now();
            app.key(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE));
            terminal.draw(|frame| draw(frame, &app)).unwrap();
            input_draw.push(started.elapsed());
        }
        assert_eq!(app.view.selected, 99);
        assert_eq!(
            app.view.page.rows[0].cells[0]
                .as_ref()
                .unwrap()
                .bytes()
                .as_ptr(),
            cached_cell
        );
        app.act(Action::Cancel);
        app.act(Action::Open);
        assert!(app.detail);
        let cached_detail = app.detail_text.as_ptr();
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        assert_eq!(app.detail_text.as_ptr(), cached_detail);
        unchanged.sort_unstable();
        input_draw.sort_unstable();
        eprintln!(
            "120x40, 100x4 cells, {bytes} retained bytes: format={format_time:?}, install={install_time:?}; 100 samples median/max: unchanged={:?}/{:?}, key-to-TestBackend-draw={:?}/{:?}",
            unchanged[50], unchanged[99], input_draw[50], input_draw[99]
        );
    }

    #[test]
    fn empty_help_and_narrow_frames_render_without_panics() {
        for theme in onetui_theme::Theme::ALL {
            assert_empty_help_and_narrow_frames(theme);
        }
    }

    #[test]
    fn connection_form_renders_controls_path_and_editing_on_narrow_frames() {
        use onetui_core::provider::Provider;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let catalog = crate::test_provider::CATALOG;
        let mut app = App::new(
            onetui_core::config::Config::load_for_startup(&path, catalog, true).unwrap(),
            None,
        );
        app.providers = catalog
            .iter()
            .map(|provider| provider.descriptor())
            .collect();
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(text.contains("No connections yet"));
        assert!(text.contains("config.toml"));
        app.act(Action::Add);
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        app.key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Enter,
            crossterm::event::KeyModifiers::NONE,
        ));
        app.paste("a_long_alias");
        for (width, height) in [(1, 1), (20, 6), (60, 19), (120, 30)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            terminal.draw(|frame| draw(frame, &app)).unwrap();
            if width == 120 {
                let text = terminal
                    .backend()
                    .buffer()
                    .content()
                    .iter()
                    .map(|cell| cell.symbol())
                    .collect::<String>();
                assert!(text.contains("F2 save"));
                assert!(text.contains("alias *"));
                assert!(text.contains("a_long_alias"));
                assert!(text.contains("token_env"));
                assert!(text.contains("config.toml"));
            }
        }
        assert!(!path.exists());
    }

    fn assert_empty_help_and_narrow_frames(theme: onetui_theme::Theme) {
        let mut config = tempfile::NamedTempFile::new().unwrap();
        write!(config, "[connections]").unwrap();
        let mut app = App::new(
            onetui_core::config::Config::load(config.path(), crate::test_provider::CATALOG)
                .unwrap(),
            None,
        );
        app.config.theme = theme;
        for (width, height) in [
            (1, 1),
            (20, 6),
            (59, 24),
            (60, 19),
            (60, 20),
            (99, 24),
            (100, 30),
        ] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            terminal.draw(|frame| draw(frame, &app)).unwrap();
            let buffer = terminal.backend().buffer();
            let last = (0..width)
                .map(|x| buffer[(x, height - 1)].symbol())
                .collect::<String>();
            if width >= 40 {
                assert!(last.ends_with(concat!("v", env!("CARGO_PKG_VERSION"))));
            } else {
                assert!(!last.contains(concat!("v", env!("CARGO_PKG_VERSION"))));
            }
            app.act(Action::Help);
            terminal.draw(|frame| draw(frame, &app)).unwrap();
        }
    }

    #[test]
    fn dynamic_row_columns_and_bounded_detail_render_on_narrow_frames() {
        for theme in onetui_theme::Theme::ALL {
            assert_dynamic_rows_and_detail(theme);
        }
    }

    fn assert_dynamic_rows_and_detail(theme: onetui_theme::Theme) {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(file, "[connections.pg]\nkind='fake'\nurl_env='UNUSED'").unwrap();
        let mut app = App::new(
            onetui_core::config::Config::load(file.path(), crate::test_provider::CATALOG).unwrap(),
            Some("pg"),
        );
        let request = app.request.take().unwrap();
        app.view.resource =
            onetui_core::Resource::new("fake.rows", vec!["public".into(), "test".into()]);
        app.complete(
            &request,
            Ok(Page {
                columns: (0..6)
                    .map(|i| onetui_core::Column {
                        name: format!("column{i}"),
                        datatype: "text".into(),
                    })
                    .collect(),
                rows: vec![onetui_core::Row {
                    cells: vec![
                        None,
                        Some(String::new().into()),
                        Some("NULL".into()),
                        Some("🌊".repeat(5000).into()),
                        Some("four".into()),
                        Some("five".into()),
                    ],
                    target: None,
                }],
                ..Page::default()
            }),
        );
        app.config.theme = theme;
        for (width, height) in [(1, 1), (20, 10), (100, 30)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            for _ in 0..6 {
                terminal.draw(|frame| draw(frame, &app)).unwrap();
                app.act(Action::Right);
            }
            app.act(Action::Open);
            terminal.draw(|frame| draw(frame, &app)).unwrap();
            app.act(Action::Back);
        }
        assert_eq!(app.view.column, 5);
    }

    #[test]
    fn terminal_child() {
        let Ok(mode) = std::env::var("ONETUI_PTY_TEST_MODE") else {
            return;
        };
        let outcome = std::panic::catch_unwind(|| {
            if mode == "panic" {
                let (_guard, mut terminal) = terminal().unwrap();
                terminal.draw(|_| {}).unwrap();
                panic!("intentional terminal-restoration test");
            }
            let mut file = tempfile::NamedTempFile::new().unwrap();
            let alias = if mode == "connection_error" {
                write!(
                    file,
                    "[connections.pg]\nkind='fake'\nurl_env='ONETUI_PTY_DSN'"
                )
                .unwrap();
                Some("pg")
            } else {
                write!(file, "[connections]").unwrap();
                None
            };
            let app = App::new(
                onetui_core::config::Config::load(file.path(), crate::test_provider::CATALOG)
                    .unwrap(),
                alias,
            );
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(run(
                    app,
                    Duration::from_secs(1),
                    crate::test_provider::CATALOG,
                ))
                .unwrap();
        });
        // macOS revokes the slave when its session leader exits; inspect modes before that happens.
        println!("ONETUI_TERMINAL_RESTORED");
        let mut acknowledgement = String::new();
        std::io::stdin().read_line(&mut acknowledgement).unwrap();
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }

    #[test]
    #[cfg(unix)]
    fn real_pty_restores_after_quit_connection_error_and_panic() {
        use nix::fcntl::{FcntlArg, OFlag, fcntl};
        use nix::pty::{Winsize, openpty};
        use nix::sys::termios::{LocalFlags, tcgetattr};
        use std::io::Read;
        use std::os::unix::process::CommandExt;
        use std::process::{Command, Stdio};

        struct KillOnDrop(std::process::Child);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                if self.0.try_wait().ok().flatten().is_none() {
                    let _ = self.0.kill();
                    let _ = self.0.wait();
                }
            }
        }

        for mode in ["normal", "connection_error", "panic"] {
            let pair = openpty(
                &Winsize {
                    ws_row: 30,
                    ws_col: 100,
                    ws_xpixel: 0,
                    ws_ypixel: 0,
                },
                None,
            )
            .unwrap();
            let slave = std::fs::File::from(pair.slave);
            let before = tcgetattr(&slave).unwrap();
            let mut master = std::fs::File::from(pair.master);
            let flags = OFlag::from_bits_truncate(fcntl(&master, FcntlArg::F_GETFL).unwrap());
            fcntl(&master, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).unwrap();
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "ui::tests::terminal_child", "--nocapture"])
                .env("ONETUI_PTY_TEST_MODE", mode)
                .env("ONETUI_PTY_DSN", "invalid-fake-secret")
                .stdin(Stdio::from(slave.try_clone().unwrap()))
                .stdout(Stdio::from(slave.try_clone().unwrap()))
                .stderr(Stdio::from(slave.try_clone().unwrap()));
            // Crossterm opens /dev/tty: the child must own the PTY, never the developer's terminal.
            unsafe {
                command.pre_exec(|| {
                    if nix::libc::setsid() == -1
                        || nix::libc::ioctl(0, nix::libc::TIOCSCTTY as _, 0) == -1
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let mut child = KillOnDrop(command.spawn().unwrap());
            let start = std::time::Instant::now();
            let mut output = Vec::new();
            let mut sent_quit = false;
            let mut restored_modes = None;
            let status = loop {
                let mut buffer = [0; 16384];
                while let Ok(count) = master.read(&mut buffer) {
                    if count == 0 {
                        break;
                    }
                    output.extend_from_slice(&buffer[..count]);
                }
                let text = String::from_utf8_lossy(&output);
                let ready = if mode == "connection_error" {
                    text.contains("fake connection")
                } else {
                    text.contains("No connections yet")
                };
                if mode != "panic" && ready && !sent_quit {
                    assert!(
                        !tcgetattr(&slave)
                            .unwrap()
                            .local_flags
                            .contains(LocalFlags::ICANON)
                    );
                    master.write_all(b"q").unwrap();
                    sent_quit = true;
                }
                if text.contains("ONETUI_TERMINAL_RESTORED") && restored_modes.is_none() {
                    restored_modes = Some(tcgetattr(&slave).unwrap());
                    master.write_all(b"\n").unwrap();
                }
                if let Some(status) = child.0.try_wait().unwrap() {
                    break status;
                }
                if start.elapsed() > Duration::from_secs(8) {
                    child.0.kill().unwrap();
                    child.0.wait().unwrap();
                    panic!("PTY test timed out ({mode}): {text}");
                }
                std::thread::sleep(Duration::from_millis(10));
            };
            let mut buffer = [0; 16384];
            while let Ok(count) = master.read(&mut buffer) {
                if count == 0 {
                    break;
                }
                output.extend_from_slice(&buffer[..count]);
            }
            assert_eq!(status.success(), mode != "panic");
            let after = restored_modes.expect("child exited before restoration handshake");
            assert_eq!(after.input_flags, before.input_flags, "{mode}");
            assert_eq!(after.output_flags, before.output_flags, "{mode}");
            assert_eq!(after.control_flags, before.control_flags, "{mode}");
            // PENDIN is kernel-maintained pending-input state, not a raw-mode setting.
            assert_eq!(
                after.local_flags & !LocalFlags::PENDIN,
                before.local_flags & !LocalFlags::PENDIN,
                "{mode}"
            );
            assert_eq!(after.control_chars, before.control_chars, "{mode}");
            let text = String::from_utf8_lossy(&output);
            assert!(text.contains("\x1b[?1049h"), "{mode}: no alternate screen");
            assert!(
                text.contains("\x1b[>1u"),
                "{mode}: enhanced keys not enabled"
            );
            let pop = text.find("\x1b[<1u").expect("keyboard flags restored");
            let leave = text.find("\x1b[?1049l").expect("alternate screen restored");
            assert!(
                pop < leave,
                "{mode}: restore keyboard before leaving alternate screen"
            );
            assert_eq!(
                text.matches("\x1b[<1u").count(),
                1,
                "{mode}: pop exactly once"
            );
            assert!(
                text.contains("\x1b[?1049l"),
                "{mode}: alternate screen not restored"
            );
            assert!(text.contains("\x1b[?25h"), "{mode}: cursor not restored");
            assert!(!text.contains("invalid-fake-secret"));
        }
    }
}
