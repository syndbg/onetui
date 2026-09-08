use onetui_theme::Theme;

#[test]
fn names_round_trip_and_palettes_are_distinct() {
    let names = [
        "catppuccin",
        "gruvbox",
        "solarized",
        "nord",
        "dracula",
        "tokyo-night",
        "one-dark",
        "rose-pine",
        "monokai",
        "flexoki",
    ];
    for (i, (theme, name)) in Theme::ALL.into_iter().zip(names).enumerate() {
        let encoded = serde_json::to_string(&theme).unwrap();
        assert_eq!(encoded, format!("\"{name}\""));
        assert_eq!(serde_json::from_str::<Theme>(&encoded).unwrap(), theme);
        assert!(
            Theme::ALL[..i]
                .iter()
                .all(|other| other.palette() != theme.palette())
        );
    }
    assert_eq!(
        serde_json::to_string(&Theme::default()).unwrap(),
        "\"catppuccin\""
    );
    for invalid in [
        "\"\"",
        "\"dark\"",
        "\"light\"",
        "\"MONOKAI\"",
        "\"rose_pine\"",
        "null",
        "42",
        "true",
        "{}",
        "[]",
    ] {
        assert!(serde_json::from_str::<Theme>(invalid).is_err());
    }
}

fn luminance(rgb: [u8; 3]) -> f64 {
    let [r, g, b] = rgb.map(|c| {
        let c = f64::from(c) / 255.0;
        if c <= 0.04045 {
            c / 12.92
        } else {
            ((c + 0.055) / 1.055).powf(2.4)
        }
    });
    0.2126 * r + 0.7152 * g + 0.0722 * b
}

fn contrast(fg: [u8; 3], bg: [u8; 3]) -> f64 {
    let a = luminance(fg);
    let b = luminance(bg);
    (a.max(b) + 0.05) / (a.min(b) + 0.05)
}

#[test]
fn palettes_keep_text_and_selection_readable() {
    for theme in Theme::ALL {
        let p = theme.palette();
        for background in [p.background, p.surface] {
            assert!(contrast(p.text, background) >= 4.5, "{theme:?}: text");
            assert!(contrast(p.muted, background) >= 3.0, "{theme:?}: muted");
        }
        assert!(
            contrast(p.selection_fg, p.selection_bg) >= 4.5,
            "{theme:?}: selection"
        );
        assert_ne!(p.selection_bg, p.background);
        assert_ne!(p.selection_bg, p.surface);
        for status in [p.success, p.warning, p.error] {
            assert!(contrast(status, p.background) >= 3.0, "{theme:?}: status");
        }
    }
}
