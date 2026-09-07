use std::io::{IsTerminal, stdout};
use std::time::Duration;

use anyhow::{Result, anyhow, ensure};
use crossterm::event::{Event, EventStream};
use futures_util::StreamExt;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Paragraph, Row, Table, TableState, Wrap};
use ratatui::{DefaultTerminal, Frame};
use tokio::sync::oneshot;

use crate::app::{App, Request};
use onetui_core::catalog::Action;
use onetui_core::{PAGE_SIZE, Page, display};

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
    // Partial initialization errors must also attempt restoration.
    let guard = TerminalGuard;
    let terminal = ratatui::try_init().map_err(|_| anyhow!("cannot initialize terminal"))?;
    Ok((guard, terminal))
}

struct Worker {
    request: Request,
    task: tokio::task::JoinHandle<Result<Page>>,
    cancel: Option<oneshot::Sender<()>>,
}

impl Worker {
    fn cancel(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.cancel();
        self.task.abort();
    }
}

pub async fn run(mut app: App, deadline: Duration) -> Result<()> {
    let (_guard, mut terminal) = terminal()?;
    let mut events = EventStream::new();
    let mut worker: Option<Worker> = None;
    while !app.quit {
        if worker.is_none()
            && let Some(request) = app.request.take()
        {
            match app
                .config
                .resolve(&request.alias, |name| std::env::var(name).ok())
            {
                Ok(onetui_core::config::ResolvedConnection::Postgres { url, ca_file }) => {
                    let (cancel, receiver) = oneshot::channel();
                    let task = tokio::spawn(onetui_postgres::fetch(
                        url,
                        ca_file,
                        request.resource.clone(),
                        request.offset,
                        request.continuation.clone(),
                        deadline,
                        receiver,
                    ));
                    worker = Some(Worker {
                        request,
                        task,
                        cancel: Some(cancel),
                    });
                }
                Err(error) => app.complete(&request, Err(error)),
                Ok(_) => app.complete(
                    &request,
                    Err(anyhow!("This datasource does not support browsing yet")),
                ),
            }
        }
        terminal
            .draw(|frame| draw(frame, &app))
            .map_err(|_| anyhow!("cannot draw terminal"))?;
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
            result = async { (&mut worker.as_mut().expect("guarded worker").task).await }, if worker.is_some() => {
                let finished = worker.take().expect("completed worker");
                // The global panic hook restores terminal modes; do not resume drawing after a worker panic.
                let result = result.map_err(|_| anyhow!("browsing worker failed; terminal restored"))?;
                app.complete(&finished.request, result);
            },
        }
        if let Some(worker) = &mut worker
            && worker.request.id != app.generation
        {
            worker.cancel();
        }
    }
    if let Some(worker) = &mut worker {
        worker.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut worker.task).await;
    }
    Ok(())
}

pub fn draw(frame: &mut Frame, app: &App) {
    let [header, body, status, command] = Layout::vertical([
        Constraint::Length(2),
        Constraint::Min(0),
        Constraint::Length(3),
        Constraint::Length(2),
    ])
    .areas(frame.area());
    let alias = app
        .view
        .alias
        .as_deref()
        .map(display)
        .unwrap_or_else(|| "choose connection".into());
    frame.render_widget(
        Paragraph::new(format!(
            "OneTUI | {alias} | read-only\n{}",
            app.view.resource.breadcrumb()
        )),
        header,
    );
    if app.help {
        let mut lines = vec!["Navigation actions (also available as :commands):".to_owned()];
        lines.extend(app.actions().map(|entry| {
            format!(
                "{}  {}  {}",
                entry.keys.join("/"),
                serde_json::to_value(entry.id).unwrap().as_str().unwrap(),
                entry.description
            )
        }));
        lines.push(
            ": opens the command prompt; Esc closes it. Keybindings are currently fixed.".into(),
        );
        frame.render_widget(
            Paragraph::new(lines.join("\n"))
                .wrap(Wrap { trim: false })
                .block(Block::bordered().title("Help")),
            body,
        );
    } else if app.detail {
        frame.render_widget(
            Paragraph::new(app.detail_text.as_str())
                .wrap(Wrap { trim: false })
                .scroll((app.detail_scroll, 0))
                .block(Block::bordered().title(format!(
                    "Field {}/{} | chunk {}/{} | h/l fields, j/k scroll, n/p chunks, Esc back",
                    app.view.column + 1,
                    app.column_count(),
                    app.detail_chunk + 1,
                    app.detail_chunks
                ))),
            body,
        );
    } else {
        let descriptor = app.descriptor();
        let start = app.view.column / 4 * 4;
        let end = (start + 4).min(app.column_count());
        let rows = app.view.visible.iter().map(|&index| {
            let row = &app.view.page.rows[index];
            Row::new(row.cells.iter().skip(start).take(end - start).map(|cell| {
                let value = cell.as_deref().unwrap_or("NULL");
                let mut preview: String = value.chars().take(128).collect();
                if preview.len() < value.len() {
                    preview.push('…');
                }
                preview
            }))
        });
        let widths = vec![Constraint::Ratio(1, (end - start).max(1) as u32); end - start];
        let table = Table::new(rows, widths)
            .header(
                Row::new((start..end).map(|i| {
                    format!(
                        "{}{}",
                        if i == app.view.column { "> " } else { "" },
                        app.column_name(i)
                    )
                }))
                .style(Style::new().add_modifier(Modifier::BOLD)),
            )
            .row_highlight_style(Style::new().add_modifier(Modifier::REVERSED))
            .highlight_symbol("> ")
            .block(Block::bordered().title(descriptor.id));
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
    let local_sort = app.view.sort.map_or_else(
        || "source order".into(),
        |(column, descending)| {
            format!(
                "{} {}",
                app.column_name(column),
                if descending { "desc" } else { "asc" }
            )
        },
    );
    frame.render_widget(
        Paragraph::new(format!(
            "{error}\nPage {} | {} items | next: {} | {scope}\nPage-local: {}/{} shown | filter: {:?} | lexical sort: {local_sort}",
            app.view.offset / PAGE_SIZE + 1,
            app.view.page.rows.len(),
            app.view.page.next,
            app.view.visible.len(), app.view.page.rows.len(), display(&app.view.filter)
        )),
        status,
    );
    let prompt = app.filter_input.as_ref().map(|value| format!("Filter displayed page: /{}\nEnter apply (empty clears) | Esc discard | max 256 UTF-8 bytes", display(value)))
        .or_else(|| app.command.as_ref().map(|value| format!(":{value}\nEnter execute | Esc cancel")))
        .unwrap_or_else(|| "Enter open/detail | / filter page | s sort field | m columns | h/l fields | Esc back | n/p pages/chunks | r refresh | c connections | : commands | ? help | q quit".into());
    frame.render_widget(Paragraph::new(prompt).wrap(Wrap { trim: false }), command);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};
    use std::io::Write;

    #[test]
    fn empty_help_and_narrow_frames_render_without_panics() {
        let mut config = tempfile::NamedTempFile::new().unwrap();
        write!(config, "[connections]").unwrap();
        let mut app = App::new(
            onetui_core::config::Config::load(config.path()).unwrap(),
            None,
        );
        for (width, height) in [(1, 1), (20, 6), (100, 30)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            terminal.draw(|frame| draw(frame, &app)).unwrap();
            app.act(Action::Help);
            terminal.draw(|frame| draw(frame, &app)).unwrap();
        }
    }

    #[test]
    fn dynamic_row_columns_and_bounded_detail_render_on_narrow_frames() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(file, "[connections.pg]\nkind='postgres'\nurl_env='UNUSED'").unwrap();
        let mut app = App::new(
            onetui_core::config::Config::load(file.path()).unwrap(),
            Some("pg"),
        );
        let request = app.request.take().unwrap();
        app.view.resource =
            onetui_core::Resource::new("postgres.rows", vec!["public".into(), "test".into()]);
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
                    "[connections.pg]\nkind='postgres'\nurl_env='ONETUI_PTY_DSN'"
                )
                .unwrap();
                Some("pg")
            } else {
                write!(file, "[connections]").unwrap();
                None
            };
            let app = App::new(
                onetui_core::config::Config::load(file.path()).unwrap(),
                alias,
            );
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(run(app, Duration::from_secs(1)))
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
                    text.contains("invalid")
                        && text.contains("PostgreSQL")
                        && text.contains("string")
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
