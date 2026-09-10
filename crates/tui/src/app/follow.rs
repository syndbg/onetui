use super::*;
use std::time::Duration;
use tokio::time::Instant;

impl App {
    pub(super) fn start_follow(&mut self) {
        self.invalidate();
        self.following = true;
        self.queue_follow(true);
    }

    fn queue_follow(&mut self, reset: bool) {
        self.generation += 1;
        self.loading = true;
        self.follow_due = None;
        self.request = Some(Request {
            follow: true,
            query: None,
            id: self.generation,
            alias: self.view.alias.clone().expect("follow alias"),
            resource: self.view.resource.clone(),
            offset: 0,
            continuation: if reset {
                None
            } else {
                self.view.page.continuation.clone()
            },
            reset,
            queued_at: Instant::now(),
        });
    }

    pub(crate) fn follow_tick(&mut self) {
        if self.following
            && !self.loading
            && self.follow_due.is_some_and(|due| due <= Instant::now())
        {
            self.queue_follow(false);
        }
    }

    pub(super) fn complete_follow(&mut self, request: &Request, result: Result<Page>) {
        let result = result.and_then(|page| {
            anyhow::ensure!(
                page.rows.len() <= PAGE_SIZE as usize && page.bytes() <= PAGE_BYTES,
                "Live batch exceeds 100 rows or 1 MiB; following stopped"
            );
            anyhow::ensure!(
                page.continuation
                    .as_ref()
                    .is_some_and(|token| !token.is_empty() && token.len() <= 4096),
                "Live batch is missing a bounded continuation; following stopped"
            );
            projections(&page).map_err(anyhow::Error::msg)?;
            Ok(page)
        });
        match result {
            Ok(mut page) => {
                if !request.reset {
                    let same_columns = self
                        .view
                        .page
                        .columns
                        .iter()
                        .map(|c| (&c.name, &c.datatype))
                        .eq(page.columns.iter().map(|c| (&c.name, &c.datatype)));
                    if !same_columns {
                        self.following = false;
                        self.error = Some(
                            "Live columns changed; following stopped, displayed data retained"
                                .into(),
                        );
                        return;
                    }
                }
                let changed = request.reset || !page.rows.is_empty();
                if request.reset {
                    self.view.page = Page::default();
                    self.view.previous.clear();
                    self.view.position = None;
                    self.view.offset = 0;
                    self.view.live_evicted = 0;
                    self.view.sort = None;
                    self.view.filter.clear();
                }
                let selected = self.view.selected_index();
                let was_last = self.view.selected + 1 >= self.view.visible.len();
                let rows = std::mem::take(&mut self.view.page.rows);
                let mut combined = rows;
                combined.append(&mut page.rows);
                page.rows = combined;
                page.next = false;
                let mut removed = 0;
                while page.rows.len() > PAGE_SIZE as usize
                    || page.bytes() > PAGE_BYTES
                    || projections(&page).is_err()
                {
                    page.rows.remove(0);
                    removed += 1;
                }
                self.view.live_evicted = self.view.live_evicted.saturating_add(removed);
                self.view.live = true;
                self.view.page = page;
                if changed || removed > 0 {
                    self.view.rebuild(
                        selected.and_then(|i| i.checked_sub(removed)),
                        self.config.display,
                    );
                    if was_last {
                        self.view.selected = self.view.visible.len().saturating_sub(1);
                    }
                }
                self.error = None;
                self.follow_due = Some(Instant::now() + Duration::from_secs(1));
            }
            Err(error) => {
                self.following = false;
                self.follow_due = None;
                self.error = Some(display(&error.to_string()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use onetui_core::Column;

    fn batch(start: usize, count: usize, bytes: usize) -> Page {
        Page {
            columns: vec![Column {
                name: "value".into(),
                datatype: "bytes".into(),
            }],
            rows: (start..start + count)
                .map(|i| Row {
                    cells: vec![Some(if bytes > 0 {
                        Value::Bytes(vec![0; bytes])
                    } else {
                        i.to_string().into()
                    })],
                    target: None,
                })
                .collect(),
            continuation: Some((start + count).to_string()),
            ..Page::default()
        }
    }

    fn deliver(app: &mut App, page: Page) {
        let request = app.request.take().unwrap();
        assert!(request.follow);
        app.complete(&request, Ok(page));
    }

    fn tick(app: &mut App) {
        app.follow_due = Some(Instant::now());
        app.follow_tick();
    }

    #[test]
    fn follow_is_bounded_cancellable_and_keeps_stopped_data() {
        let config = Config::parse(
            "[connections.a]\nkind='fake'",
            crate::test_provider::CATALOG,
        )
        .unwrap();
        let mut app = App::new(config, None);
        assert!(!app.available(Action::Follow));
        app.view = View::new(Some("a".into()), Resource::new("fake.rows", vec![]));
        app.act(Action::Follow);
        assert!(app.following && app.loading);
        deliver(&mut app, batch(0, 0, 0));
        assert!(app.following && app.view.live && app.view.page.rows.is_empty());
        assert!(!app.available(Action::Next) && !app.available(Action::Previous));
        for start in [0, 100, 200] {
            tick(&mut app);
            assert_eq!(
                app.request.as_ref().unwrap().continuation,
                Some(start.to_string())
            );
            deliver(&mut app, batch(start, 100, 0));
        }
        assert_eq!(app.view.page.rows.len(), 100);
        assert_eq!(app.view.live_evicted, 200);
        assert_eq!(app.view.page.rows[0].cells[0], Some("200".into()));
        let previews = app.view.previews.as_ptr();
        tick(&mut app);
        deliver(&mut app, batch(300, 0, 0));
        assert_eq!(
            app.view.previews.as_ptr(),
            previews,
            "quiet polls must not reformat data"
        );
        tick(&mut app);
        let late = app.request.take().unwrap();
        app.act(Action::Cancel);
        assert!(!app.following && !app.quit);
        app.complete(&late, Ok(batch(300, 5, 0)));
        assert_eq!(app.view.page.rows.len(), 100);
        assert_eq!(app.view.live_evicted, 200);
        app.act(Action::Follow);
        assert!(
            app.request.as_ref().unwrap().continuation.is_none(),
            "restart must establish a fresh end"
        );
        deliver(&mut app, batch(0, 0, 0));
        for start in [0, 1, 2] {
            tick(&mut app);
            deliver(&mut app, batch(start, 1, PAGE_BYTES / 5));
            assert!(app.view.page.bytes() <= PAGE_BYTES);
            assert!(projections(&app.view.page).is_ok());
        }
        assert!(
            app.view.live_evicted > 0,
            "byte/projection limits must also evict old records"
        );
        app.act(Action::Open);
        assert!(!app.following && app.detail);
        app.act(Action::Back);
        app.act(Action::Refresh);
        assert!(!app.request.as_ref().unwrap().follow);
        let request = app.request.take().unwrap();
        app.complete(&request, Ok(batch(0, 1, 0)));
        assert!(!app.view.live);
    }

    #[test]
    fn follow_error_or_invalid_batch_retains_cursor_and_stops_timer() {
        let config = Config::parse(
            "[connections.a]\nkind='fake'",
            crate::test_provider::CATALOG,
        )
        .unwrap();
        let mut app = App::new(config, None);
        app.view = View::new(Some("a".into()), Resource::new("fake.rows", vec![]));
        app.act(Action::Follow);
        deliver(&mut app, batch(0, 0, 0));
        tick(&mut app);
        deliver(&mut app, batch(0, 1, 0));
        tick(&mut app);
        let request = app.request.take().unwrap();
        app.complete(&request, Err(anyhow::anyhow!("native broker error")));
        assert_eq!(app.error.as_deref(), Some("native broker error"));
        assert!(!app.following && app.follow_due.is_none());
        assert_eq!(app.view.page.continuation.as_deref(), Some("1"));
        app.act(Action::Follow);
        let mut invalid = batch(0, 0, 0);
        invalid.continuation = None;
        deliver(&mut app, invalid);
        assert!(!app.following);
        assert_eq!(app.view.page.rows.len(), 1);
    }

    #[test]
    fn quiet_batch_cursor_growth_rebuilds_rows_after_eviction() {
        let config = Config::parse(
            "[connections.a]\nkind='fake'",
            crate::test_provider::CATALOG,
        )
        .unwrap();
        let mut app = App::new(config, None);
        app.view = View::new(Some("a".into()), Resource::new("fake.rows", vec![]));
        app.act(Action::Follow);
        deliver(&mut app, batch(0, 0, 0));
        tick(&mut app);
        let mut large = batch(0, 1, 0);
        large.rows[0].cells[0] = Some("x".repeat(PAGE_BYTES - 2048).into());
        deliver(&mut app, large);
        assert_eq!(app.view.visible.len(), 1);
        tick(&mut app);
        let mut quiet = batch(1, 0, 0);
        quiet.continuation = Some("c".repeat(4096));
        deliver(&mut app, quiet);
        assert_eq!(app.view.live_evicted, 1);
        assert!(
            app.view.page.rows.is_empty()
                && app.view.visible.is_empty()
                && app.view.previews.is_empty()
        );
    }
}
