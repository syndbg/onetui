use ratatui::Frame;
use ratatui::layout::{Alignment, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Clear, Paragraph, Wrap};

/// Draw a modal centered over `area`: the cells under it are cleared, then `lines` are
/// wrapped inside `block`. The height fits the wrapped lines, the borders and one spare
/// row, and never exceeds `area`; the width is `max_width` capped the same way.
pub fn render(
    frame: &mut Frame,
    area: Rect,
    block: Block,
    lines: Vec<Line>,
    max_width: u16,
    alignment: Alignment,
) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let width = area.width.min(max_width);
    let inner = usize::from(width.saturating_sub(2).max(1));
    let rows = lines
        .iter()
        .map(|line| line.width().div_ceil(inner).max(1))
        .sum::<usize>();
    let height = area.height.min(u16::try_from(rows + 3).unwrap_or(u16::MAX));
    let popup = Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    );
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines)
            .alignment(alignment)
            .wrap(Wrap { trim: false })
            .block(block),
        popup,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};

    fn draw(width: u16, height: u16, lines: Vec<Line<'static>>) -> Vec<String> {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal
            .draw(|frame| {
                let area = frame.area();
                frame.render_widget(
                    Paragraph::new("x".repeat(usize::from(width)).repeat(usize::from(height)))
                        .wrap(Wrap { trim: false }),
                    area,
                );
                render(
                    frame,
                    area,
                    Block::bordered().title(" T "),
                    lines,
                    20,
                    Alignment::Left,
                );
            })
            .unwrap();
        let buffer = terminal.backend().buffer();
        (0..height)
            .map(|y| (0..width).map(|x| buffer[(x, y)].symbol()).collect())
            .collect()
    }

    #[test]
    fn popup_is_centered_clears_below_and_grows_with_wrapped_text() {
        // Two short lines: 2 rows + borders + spare = 5 rows, centered in 11.
        let rows = draw(30, 11, vec![Line::raw("one"), Line::raw("two")]);
        assert!(rows[3].contains("┌ T "), "{rows:#?}");
        assert!(rows[4].contains("│one"));
        assert!(rows[5].contains("│two"));
        // The spare row is cleared, not the content below it.
        assert!(rows[6].contains("│                  │"));
        assert!(rows[7].contains('└'));
        // A line wider than the inner width wraps and makes the popup taller.
        let rows = draw(30, 11, vec![Line::raw("a".repeat(25))]);
        assert!(rows[3].contains('┌') && rows[4].contains(&"a".repeat(18)));
        assert!(rows[5].contains("aaaaaaa"));
        assert!(rows[7].contains('└'));
    }

    #[test]
    fn popup_never_exceeds_the_area() {
        let rows = draw(10, 3, (0..20).map(|_| Line::raw("line")).collect());
        assert!(
            rows[0].starts_with('┌') && rows[2].starts_with('└'),
            "{rows:#?}"
        );
        // An empty area draws nothing and does not panic.
        let mut terminal = Terminal::new(TestBackend::new(4, 4)).unwrap();
        terminal
            .draw(|frame| {
                render(
                    frame,
                    Rect::new(0, 0, 0, 0),
                    Block::bordered(),
                    vec![Line::raw("x")],
                    10,
                    Alignment::Center,
                );
            })
            .unwrap();
    }
}
