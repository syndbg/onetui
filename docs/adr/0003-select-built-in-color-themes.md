---
status: accepted
date: 2026-09-07
---

# ADR-0003: Select built-in color themes

## Decision

Keep built-in color schemes in a separate `onetui-theme` workspace crate at `crates/theme`. Select them with a `Theme` enum and an exhaustive match, following the compile-time selection used by [providers](0002-use-static-enum-dispatch-for-built-in-providers.md).

Themes supply palette data, not rendering behavior. One renderer uses the selected palette for every datasource and screen. No strategy trait, trait objects, boxing or theme-specific copies of widgets are needed.

Include Catppuccin, Gruvbox, Solarized, Nord, Dracula, Tokyo Night, One Dark, Rosé Pine, Monokai and Flexoki. Default to `catppuccin`, preserving the current colors. Each name selects one fixed palette; additional light/dark flavors are outside this decision. This decision is accepted; theme selection is not implemented yet.

## Current behavior and package boundary

The colors are currently constants in [the TUI renderer](../../crates/tui/src/ui.rs). [Configuration](../../crates/core/src/config.rs) accepts connection definitions only. Moving the palette into a crate gives configuration and rendering one source for the supported names without making core depend on Ratatui.

`onetui-theme` owns the serializable selector, its default, supported names and immutable palettes. It uses Serde, already a workspace dependency, and plain RGB values. It has no Ratatui, Tokio or datasource dependencies. Core uses the selector when parsing configuration; TUI translates palette values into Ratatui colors and styles. Database connectors do not depend on themes.

Palette fields describe their purpose: background, surface, text, muted text, border, title, key hint, identifier, table heading, selection foreground/background, success, warning and error. Widgets choose roles rather than literal colors. Two roles can share a color without becoming the same role.

The selection looks like this; palette contents are omitted:

```rust
#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Theme {
    #[default]
    Catppuccin,
    Gruvbox,
    Solarized,
    Nord,
    Dracula,
    TokyoNight,
    OneDark,
    RosePine,
    Monokai,
    Flexoki,
}

impl Theme {
    pub const fn palette(self) -> &'static Palette {
        match self {
            Self::Catppuccin => &CATPPUCCIN,
            Self::Gruvbox => &GRUVBOX,
            Self::Solarized => &SOLARIZED,
            Self::Nord => &NORD,
            Self::Dracula => &DRACULA,
            Self::TokyoNight => &TOKYO_NIGHT,
            Self::OneDark => &ONE_DARK,
            Self::RosePine => &ROSE_PINE,
            Self::Monokai => &MONOKAI,
            Self::Flexoki => &FLEXOKI,
        }
    }
}
```

Each palette is a complete struct value. Adding a role requires every palette to supply it; adding a variant requires updating the match. This is the same static selection principle as provider dispatch, without the async interfaces and session lifecycle that only providers need.

## Configuration contract

Add an optional top-level `theme` string to the existing configuration file:

```toml
theme = "monokai"

# Existing [connections.<alias>] tables follow unchanged.
```

Accepted values are exactly `catppuccin`, `gruvbox`, `solarized`, `nord`, `dracula`, `tokyo-night`, `one-dark`, `rose-pine`, `monokai` and `flexoki`; omission selects `catppuccin`. Names are case-sensitive. Empty strings, unknown names and non-string values fail configuration validation, including headless `--check`. The earlier proposed `dark` and `light` names were never implemented and are not aliases. Theme selection does not open a datasource or resolve credentials.

Use the existing file selection rules: `--config PATH`, otherwise `$XDG_CONFIG_HOME/onetui/config.toml`, otherwise `$HOME/.config/onetui/config.toml`. There is no additional theme file or merge layer. For a home-directory file, use `onetui --config "$HOME/onetui.toml" --connection local_pg` with the theme and connection defined in that file.

`onetui schema` must describe the setting, accepted names, default and example using the theme crate's metadata. It remains an offline description of supported configuration, not a dump of the user's selected theme or connection secrets. Read the theme once at startup; there is no CLI override, live reload or theme-switch key in this decision.

## Alternatives and consequences

Keeping palettes inside TUI would suffice for rendering alone, but configuration also needs the selector and its supported values. A small dependency-free color model in the theme crate avoids a core-to-TUI dependency. The TUI keeps the small Ratatui conversion at its boundary.

A trait-based strategy would add an interface where the only difference is data. Per-theme renderers would duplicate layout and input behavior. Runtime plugins and user-defined palette files are unnecessary for the built-in schemes; they would also require validation and compatibility rules that enum selection does not need.

Explicit selection avoids terminal-background detection and its startup queries. Users choose the theme that fits their terminal. Supporting terminal-default colors or custom palettes would require a later extension, not unused variants now.

Every palette must keep selection, labels and status readable. Color supplements text and markers: errors, read-only mode, loading and the selected row remain identifiable without distinguishing hues. Theme selection must not change navigation, cached data, connection lifetime or terminal restoration.
