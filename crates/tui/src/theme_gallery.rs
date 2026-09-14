use std::{fmt::Write, path::PathBuf};

use onetui_core::{Column, Page, Resource, Row, Value, config::Config, provider::ConnectionStatus};
use onetui_theme::Theme;
use ratatui::{
    Terminal,
    backend::TestBackend,
    style::{Color, Modifier},
};
use unicode_width::UnicodeWidthStr;

const WIDTH: u16 = 144;
const HEIGHT: u16 = 32;

fn render(theme: Theme) -> String {
    let config = Config::parse(
        "[connections.demo_pg]\nkind='postgres'\nurl_env='UNUSED_GALLERY_DSN'",
        &[onetui_postgres::PostgresProvider],
    )
    .unwrap();
    let mut app = crate::App::new(config, Some("demo_pg"));
    app.config.theme = theme;
    let request = app.request.take().unwrap();
    app.view.resource = Resource::new("postgres.rows", vec!["demo".into(), "customers".into()]);
    app.complete(&request, Ok(Page {
        columns: [("id", "bigint"), ("name", "text"), ("country", "text"), ("profile", "jsonb")]
            .into_iter().map(|(name, datatype)| Column { name: name.into(), datatype: datatype.into() }).collect(),
        rows: [
            ("Mina", "JP", "starter"), ("Jose", "BR", "team"),
            ("Zoe", "BG", "enterprise"), ("Sam", "DE", "team"),
            ("Avery", "US", "starter"), ("Noor", "NL", "team"),
            ("Alex", "GB", "enterprise"), ("Robin", "SE", "starter"),
            ("Rene", "FR", "team"), ("Sasha", "CA", "starter"),
            ("Kai", "NZ", "enterprise"), ("Morgan", "AU", "team"),
        ].into_iter().enumerate().map(|(i, (name, country, plan))| Row {
            cells: vec![Some((i + 1).to_string().into()), Some(name.into()), Some(country.into()),
                if i == 4 { None } else { Some(Value::Json(serde_json::json!({"plan":plan,"active":i%3!=0,"tags":["demo","synthetic"]}).to_string())) }],
            target: None,
        }).collect(),
        notice: "Synthetic demo data | Independent reads, not a snapshot".into(),
        ..Page::default()
    }));
    app.view.selected = 3;
    app.connection_status = Some(ConnectionStatus::Connected);
    let mut terminal = Terminal::new(TestBackend::new(WIDTH, HEIGHT)).unwrap();
    terminal.draw(|frame| crate::ui::draw(frame, &app)).unwrap();
    let name = serde_json::to_value(theme)
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    let mut svg = format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{}\" height=\"{}\" viewBox=\"0 0 {} {}\" role=\"img\" aria-label=\"OneTUI {name} theme\">\n<title>OneTUI {name} theme with synthetic customer data</title>\n<rect width=\"100%\" height=\"100%\" fill=\"{}\"/>\n<g font-family=\"'DejaVu Sans Mono',Consolas,monospace\" font-size=\"16\">\n",
        WIDTH * 10 + 32,
        HEIGHT * 22 + 32,
        WIDTH * 10 + 32,
        HEIGHT * 22 + 32,
        rgb(Color::Rgb(
            theme.palette().background[0],
            theme.palette().background[1],
            theme.palette().background[2]
        ))
    );
    for (y, row) in terminal
        .backend()
        .buffer()
        .content
        .chunks(WIDTH as usize)
        .enumerate()
    {
        let mut x = 0;
        for run in row.chunk_by(|a, b| a.fg == b.fg && a.bg == b.bg && a.modifier == b.modifier) {
            let cell = &run[0];
            // Fail if the sample or renderer needs SVG features this exporter does not support.
            assert!((cell.modifier - Modifier::BOLD).is_empty());
            assert!(run.iter().all(|c| c.symbol().width() == 1));
            let text: String = run.iter().map(|c| c.symbol()).collect();
            let (left, top, width) = (16 + x * 10, 16 + y * 22, run.len() * 10);
            writeln!(
                svg,
                "<rect x=\"{left}\" y=\"{top}\" width=\"{width}\" height=\"22\" fill=\"{}\"/>",
                rgb(cell.bg)
            )
            .unwrap();
            if !text.trim().is_empty() {
                writeln!(svg, "<text x=\"{left}\" y=\"{}\" fill=\"{}\" font-weight=\"{}\" textLength=\"{width}\" lengthAdjust=\"spacingAndGlyphs\" xml:space=\"preserve\">{}</text>", top + 17, rgb(cell.fg), if cell.modifier.contains(Modifier::BOLD) { "bold" } else { "normal" }, escape(&text)).unwrap();
            }
            x += run.len();
        }
    }
    svg.push_str("</g>\n</svg>\n");
    svg
}

fn rgb(color: Color) -> String {
    let Color::Rgb(r, g, b) = color else {
        panic!("Gallery requires explicit RGB colors: {color:?}")
    };
    format!("#{r:02x}{g:02x}{b:02x}")
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/assets/themes")
}

#[test]
fn snapshots_match_renderer() {
    assert_eq!(escape("a<&>b"), "a&lt;&amp;&gt;b");
    for theme in Theme::ALL {
        let name = serde_json::to_value(theme)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        let expected = std::fs::read_to_string(directory().join(format!("{name}.svg")))
            .expect("Missing theme preview. Run make theme-gallery");
        assert_eq!(
            expected,
            render(theme),
            "{name} preview is stale. Run make theme-gallery"
        );
    }
}

#[test]
#[ignore = "regenerates committed theme previews. Run make theme-gallery"]
fn export() {
    std::fs::create_dir_all(directory()).unwrap();
    for theme in Theme::ALL {
        let name = serde_json::to_value(theme)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        std::fs::write(directory().join(format!("{name}.svg")), render(theme)).unwrap();
    }
}
