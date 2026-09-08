# onetui-theme

Built-in theme selection and semantic RGB palettes. Serde is the only runtime dependency. Core parses the selector; TUI converts RGB values into Ratatui styles. Providers do not depend on this crate.

## Usage

Set `theme = "monokai"` before any table in the existing OneTUI TOML file. Omission selects `catppuccin`. Names below are exact and case-sensitive; empty/unknown names and non-string values are errors, including with `--check`. No environment expansion, CLI override, separate theme file or live reload. Restart to apply changes. The earlier proposed `dark` and `light` names are not accepted.

Run `onetui schema` for the installed binary's accepted names, default and example. See [configuration](../../README.md#configuration) for file selection and connection setup.

## Palettes

Each theme uses one fixed dark palette. These are OneTUI role mappings, not copies of editor syntax-highlighting rules. Catppuccin preserves the original TUI colors. Muted labels use brighter upstream shades where comment colors would be hard to read; selection uses a contrasting accent background and dark foreground. One Dark's secondary surface is a darker shade of its background.

| Config value | Palette | Source / terms |
| --- | --- | --- |
| `catppuccin` | Catppuccin Mocha | [Source](https://github.com/catppuccin/palette/blob/07d02aa110ef9eb7e7427afca5c73ba9cf7f8ebd/palette.json), MIT |
| `gruvbox` | Gruvbox dark, medium contrast | [Source](https://github.com/morhetz/gruvbox/blob/5d15b2765f59754d7ac263c88a0f6e3e58124951/colors/gruvbox.vim), MIT/X11 |
| `solarized` | Solarized dark | [Source](https://github.com/altercation/solarized/blob/62f656a02f93c5190a8753159e34b385588d5ff3/README.md), MIT |
| `nord` | Nord | [Source](https://github.com/nordtheme/nord/blob/1cef71605416a222e57225b544540ce0fcec18d4/src/nord.scss), MIT |
| `dracula` | Dracula OSS | [Source](https://github.com/dracula/dracula-theme/blob/5962daae54e4608d281cb6f4eee2349e605d9e3c/README.md), MIT |
| `tokyo-night` | Tokyo Night, night | [Source](https://github.com/folke/tokyonight.nvim/blob/cdc07ac78467a233fd62c493de29a17e0cf2b2b6/lua/tokyonight/colors/night.lua), Apache-2.0 |
| `one-dark` | Atom One Dark | [Source](https://github.com/atom/one-dark-syntax/blob/9c96f4454362267ac45322063e193ccf9d2debb1/styles/colors.less), MIT |
| `rose-pine` | Rosé Pine main | [Source](https://github.com/rose-pine/neovim/blob/ff483051a47e27d84bdef47703538df1ed9f4a47/lua/rose-pine/palette.lua), MIT |
| `monokai` | Classic Monokai, VS Code port | [Source](https://github.com/microsoft/vscode/blob/aa7291eba7d1e34288afb2cd4e99293cfa828dd2/extensions/theme-monokai/themes/monokai-color-theme.json), MIT |
| `flexoki` | Flexoki dark | [Source](https://github.com/kepano/flexoki/blob/8d723bac4a9ac46adfdf99d42155286977aac72a/README.md), MIT |

Tokyo Night's night palette inherits its accent colors from [storm.lua](https://github.com/folke/tokyonight.nvim/blob/cdc07ac78467a233fd62c493de29a17e0cf2b2b6/lua/tokyonight/colors/storm.lua). Flexoki's numeric base/accent values are documented in its [palette specification](https://stephango.com/flexoki). Source revisions and license notices were checked on 2026-09-09. [Third-party notices](../../THIRD_PARTY_NOTICES.md) accompany source and release archives.

## Validation

```sh
cargo test -p onetui-theme --locked
```

Package tests check all ten serialized names, strict rejection, distinct palettes and RGB contrast. Main text and selection have a minimum 4.5:1 calculated contrast; muted labels and status text have a minimum 3:1 on their tested backgrounds. These checks do not measure a physical terminal or guarantee accessibility on every display. TUI tests own rendering, input states and terminal cleanup.
