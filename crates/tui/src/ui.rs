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

fn key_hints(frame: &mut Frame, area: Rect, p: &Palette, hints: &[(&str, &str)]) {
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
                    Style::new().fg(color(p.key_hint)).bold(),
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
        );
        return;
    }
    if app.command.is_some() || app.filter_input.is_some() {
        let hints: &[(&str, &str)] = if app.filter_input.is_some() {
            &[
                ("/", "Filter displayed page"),
                ("Enter", "apply (empty clears)"),
                ("Esc", "discard"),
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
        key_hints(frame, keys, p, hints);
        return;
    }
    let columns = Layout::horizontal([Constraint::Fill(1); 3]).split(keys);
    let actions: Vec<_> = app.actions().collect();
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
        key_hints(frame, *area, p, &hints);
    }
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
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
    let (_guard, mut terminal) = terminal()?;
    let mut events = EventStream::new();
    let mut worker: Option<Worker> = None;
    let outcome = async {
        while !app.quit {
            if let Some(active) = &mut worker {
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
            terminal.draw(|frame| draw(frame, &app)).map_err(|_| anyhow!("cannot draw terminal"))?;
            tokio::select! {
                event = events.next() => match event {
                    Some(Ok(Event::Key(key))) => app.key(key),
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
        frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
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
                if !line.is_empty() && line.len() + 1 + word.len() > description_width {
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
                Cell::from(lines.join("\n")),
            ])
            .height(lines.len() as u16)
            .style(Style::new().bg(color(if index % 2 == 0 {
                p.background
            } else {
                p.surface
            })))
        });
    frame.render_widget(
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
    );
}

pub fn draw(frame: &mut Frame, app: &App) {
    let p = app.config.theme.palette();
    let area = frame.area();
    frame.render_widget(
        Block::new().style(Style::new().fg(color(p.text)).bg(color(p.background))),
        area,
    );
    let show_command = app.command.is_some() || app.filter_input.is_some();
    let [info, command, body, status] = Layout::vertical([
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
    .areas(area);
    context(frame, info, app);
    if show_command {
        command_bar(frame, command, app);
    }
    if app.theme_menu.is_some() {
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
            Paragraph::new(app.detail_text.as_str())
                .wrap(Wrap { trim: false })
                .scroll((app.detail_scroll, 0))
                .block(panel(
                    p,
                    format!(
                        "Field {}/{} | chunk {}/{} | h/l fields, j/k scroll, n/p chunks, Esc back",
                        app.view.column + 1,
                        app.column_count(),
                        app.detail_chunk + 1,
                        app.detail_chunks
                    ),
                )),
            body,
        );
    } else {
        let descriptor = app.descriptor();
        let start = app.view.column / 4 * 4;
        let end = (start + 4).min(app.column_count());
        let rows = app.view.visible.iter().map(|&index| {
            let row = &app.view.page.rows[index];
            Row::new(
                row.cells
                    .iter()
                    .enumerate()
                    .skip(start)
                    .take(end - start)
                    .map(|(column, cell)| {
                        let value = cell.as_deref().unwrap_or("NULL");
                        let mut preview: String = value.chars().take(128).collect();
                        if preview.len() < value.len() {
                            preview.push('…');
                        }
                        Cell::from(preview).style(Style::new().fg(if cell.is_none() {
                            color(p.muted)
                        } else if column == 0 {
                            color(p.identifier)
                        } else {
                            color(p.text)
                        }))
                    }),
            )
            .style(Style::new().bg(if index % 2 == 0 {
                color(p.background)
            } else {
                color(p.surface)
            }))
        });
        let widths = vec![Constraint::Ratio(1, (end - start).max(1) as u32); end - start];
        let table = Table::new(rows, widths)
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
                    " {} [{} shown / {} loaded]{} ",
                    descriptor.id,
                    app.view.visible.len(),
                    app.view.page.rows.len(),
                    if app.view.filter.is_empty() {
                        String::new()
                    } else {
                        format!(" | filter: {:?}", display(&app.view.filter))
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
    let state = if app.loading {
        "Loading… Ctrl-c cancels"
    } else if app.view.page.rows.is_empty() {
        "Empty result"
    } else if app.view.visible.is_empty() {
        "No matches on this page"
    } else {
        "Ready"
    };
    let error = app.error.as_deref().unwrap_or(state);
    let scope = if app.view.resource.id == "connections" {
        "configured aliases"
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
        Paragraph::new(vec![
            Line::styled(
                error,
                Style::new().fg(if app.error.is_some() {
                    color(p.error)
                } else if app.loading {
                    color(p.warning)
                } else {
                    color(p.success)
                }),
            ),
            Line::raw(format!(
                "Page {} | {} items | next: {} | {scope}",
                app.view.offset / PAGE_SIZE + 1,
                app.view.page.rows.len(),
                app.view.page.next,
            )),
        ])
        .style(Style::new().fg(color(p.muted))),
        status,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};
    use std::io::Write;

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
        app.filter_input = Some("4b".into());
        terminal.draw(|frame| draw(frame, &app)).unwrap();
        let text = lines(&terminal);
        assert!(text[0].contains("Context"));
        assert!(text[..8].join("").contains("Filter displayed page"));
        assert_eq!(text[8], format!("╭{}╮", "─".repeat(118)));
        assert!(text[9].contains("/4b"));
        assert!(text[..8].join("").contains("apply (empty clears)"));
        assert_eq!(text[10], format!("╰{}╯", "─".repeat(118)));
        assert!(text[11].contains("connections [2 shown / 2 loaded]"));
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
        app.filter_input = Some(String::new());
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
                    cells: vec![Some("42".into()), Some(display("София\u{001b}")), None],
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
        assert!(!text.contains("p      previous"));
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
        assert!(!text.contains("m      columns"));
        assert!(!text.contains("n      next"));
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
                    cells: (0..4).map(|_| Some(display(&raw))).collect(),
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
        let cached_cell = app.view.page.rows[0].cells[0].as_ref().unwrap().as_ptr();
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
            app.view.page.rows[0].cells[0].as_ref().unwrap().as_ptr(),
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
                        Some(String::new()),
                        Some("NULL".into()),
                        Some("🌊".repeat(5000)),
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
                    text.contains("Empty") && text.contains("result")
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
                text.contains("\x1b[?1049l"),
                "{mode}: alternate screen not restored"
            );
            assert!(text.contains("\x1b[?25h"), "{mode}: cursor not restored");
            assert!(!text.contains("invalid-fake-secret"));
        }
    }
}
