// Palette colors adapted to OneTUI roles. Sources and notices: ../README.md and ../../../THIRD_PARTY_NOTICES.md.
use crate::Palette;

const fn rgb(hex: u32) -> [u8; 3] {
    [(hex >> 16) as u8, (hex >> 8) as u8, hex as u8]
}

pub const CATPPUCCIN: Palette = Palette {
    background: rgb(0x1e1e2e),
    surface: rgb(0x181825),
    text: rgb(0xcdd6f4),
    muted: rgb(0xa6adc8),
    border: rgb(0xb4befe),
    title: rgb(0x89dceb),
    key_hint: rgb(0x89dceb),
    identifier: rgb(0x89b4fa),
    table_heading: rgb(0xf9e2af),
    selection_fg: rgb(0x1e1e2e),
    selection_bg: rgb(0xb4befe),
    success: rgb(0xa6e3a1),
    warning: rgb(0xf9e2af),
    error: rgb(0xf38ba8),
};

pub const GRUVBOX: Palette = Palette {
    background: rgb(0x282828),
    surface: rgb(0x3c3836),
    text: rgb(0xebdbb2),
    muted: rgb(0xbdae93),
    border: rgb(0xd3869b),
    title: rgb(0x8ec07c),
    key_hint: rgb(0x8ec07c),
    identifier: rgb(0x83a598),
    table_heading: rgb(0xfabd2f),
    selection_fg: rgb(0x282828),
    selection_bg: rgb(0xd3869b),
    success: rgb(0xb8bb26),
    warning: rgb(0xfabd2f),
    error: rgb(0xfb4934),
};

pub const SOLARIZED: Palette = Palette {
    background: rgb(0x002b36),
    surface: rgb(0x073642),
    text: rgb(0x93a1a1),
    muted: rgb(0x839496),
    border: rgb(0x93a1a1),
    title: rgb(0x2aa198),
    key_hint: rgb(0x2aa198),
    identifier: rgb(0x268bd2),
    table_heading: rgb(0xb58900),
    selection_fg: rgb(0x002b36),
    selection_bg: rgb(0x93a1a1),
    success: rgb(0x859900),
    warning: rgb(0xb58900),
    error: rgb(0xdc322f),
};

pub const NORD: Palette = Palette {
    background: rgb(0x2e3440),
    surface: rgb(0x3b4252),
    text: rgb(0xeceff4),
    muted: rgb(0x81a1c1),
    border: rgb(0x88c0d0),
    title: rgb(0x8fbcbb),
    key_hint: rgb(0x88c0d0),
    identifier: rgb(0x81a1c1),
    table_heading: rgb(0xebcb8b),
    selection_fg: rgb(0x2e3440),
    selection_bg: rgb(0x88c0d0),
    success: rgb(0xa3be8c),
    warning: rgb(0xebcb8b),
    error: rgb(0xbf616a),
};

pub const DRACULA: Palette = Palette {
    background: rgb(0x282a36),
    surface: rgb(0x44475a),
    text: rgb(0xf8f8f2),
    muted: rgb(0xbd93f9),
    border: rgb(0xbd93f9),
    title: rgb(0x8be9fd),
    key_hint: rgb(0x8be9fd),
    identifier: rgb(0xbd93f9),
    table_heading: rgb(0xf1fa8c),
    selection_fg: rgb(0x282a36),
    selection_bg: rgb(0xbd93f9),
    success: rgb(0x50fa7b),
    warning: rgb(0xf1fa8c),
    error: rgb(0xff5555),
};

pub const TOKYO_NIGHT: Palette = Palette {
    background: rgb(0x1a1b26),
    surface: rgb(0x16161e),
    text: rgb(0xc0caf5),
    muted: rgb(0xa9b1d6),
    border: rgb(0xbb9af7),
    title: rgb(0x7dcfff),
    key_hint: rgb(0x7dcfff),
    identifier: rgb(0x7aa2f7),
    table_heading: rgb(0xe0af68),
    selection_fg: rgb(0x1a1b26),
    selection_bg: rgb(0xbb9af7),
    success: rgb(0x9ece6a),
    warning: rgb(0xe0af68),
    error: rgb(0xf7768e),
};

pub const ONE_DARK: Palette = Palette {
    background: rgb(0x282c34),
    surface: rgb(0x21252b),
    text: rgb(0xabb2bf),
    muted: rgb(0x828997),
    border: rgb(0xc678dd),
    title: rgb(0x56b6c2),
    key_hint: rgb(0x56b6c2),
    identifier: rgb(0x61afef),
    table_heading: rgb(0xe5c07b),
    selection_fg: rgb(0x282c34),
    selection_bg: rgb(0xc678dd),
    success: rgb(0x98c379),
    warning: rgb(0xe5c07b),
    error: rgb(0xe06c75),
};

pub const ROSE_PINE: Palette = Palette {
    background: rgb(0x191724),
    surface: rgb(0x1f1d2e),
    text: rgb(0xe0def4),
    muted: rgb(0x908caa),
    border: rgb(0xc4a7e7),
    title: rgb(0xebbcba),
    key_hint: rgb(0x9ccfd8),
    identifier: rgb(0x9ccfd8),
    table_heading: rgb(0xf6c177),
    selection_fg: rgb(0x191724),
    selection_bg: rgb(0xc4a7e7),
    success: rgb(0x95b1ac),
    warning: rgb(0xf6c177),
    error: rgb(0xeb6f92),
};

pub const MONOKAI: Palette = Palette {
    background: rgb(0x272822),
    surface: rgb(0x1e1f1c),
    text: rgb(0xf8f8f2),
    muted: rgb(0x90908a),
    border: rgb(0xae81ff),
    title: rgb(0x66d9ef),
    key_hint: rgb(0x66d9ef),
    identifier: rgb(0x66d9ef),
    table_heading: rgb(0xe6db74),
    selection_fg: rgb(0x272822),
    selection_bg: rgb(0xae81ff),
    success: rgb(0xa6e22e),
    warning: rgb(0xe6db74),
    error: rgb(0xf92672),
};

pub const FLEXOKI: Palette = Palette {
    background: rgb(0x100f0f),
    surface: rgb(0x1c1b1a),
    text: rgb(0xcecdc3),
    muted: rgb(0x878580),
    border: rgb(0x8b7ec8),
    title: rgb(0x3aa99f),
    key_hint: rgb(0x3aa99f),
    identifier: rgb(0x4385be),
    table_heading: rgb(0xd0a215),
    selection_fg: rgb(0x100f0f),
    selection_bg: rgb(0x8b7ec8),
    success: rgb(0x879a39),
    warning: rgb(0xd0a215),
    error: rgb(0xd14d41),
};
