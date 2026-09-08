//! Built-in palettes. RGB roles stay independent of terminal rendering.

use serde::{Deserialize, Serialize};

mod palettes;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
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
    pub const ALL: [Self; 10] = [
        Self::Catppuccin,
        Self::Gruvbox,
        Self::Solarized,
        Self::Nord,
        Self::Dracula,
        Self::TokyoNight,
        Self::OneDark,
        Self::RosePine,
        Self::Monokai,
        Self::Flexoki,
    ];

    pub const fn palette(self) -> &'static Palette {
        match self {
            Self::Catppuccin => &palettes::CATPPUCCIN,
            Self::Gruvbox => &palettes::GRUVBOX,
            Self::Solarized => &palettes::SOLARIZED,
            Self::Nord => &palettes::NORD,
            Self::Dracula => &palettes::DRACULA,
            Self::TokyoNight => &palettes::TOKYO_NIGHT,
            Self::OneDark => &palettes::ONE_DARK,
            Self::RosePine => &palettes::ROSE_PINE,
            Self::Monokai => &palettes::MONOKAI,
            Self::Flexoki => &palettes::FLEXOKI,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Palette {
    pub background: [u8; 3],
    pub surface: [u8; 3],
    pub text: [u8; 3],
    pub muted: [u8; 3],
    pub border: [u8; 3],
    pub title: [u8; 3],
    pub key_hint: [u8; 3],
    pub identifier: [u8; 3],
    pub table_heading: [u8; 3],
    pub selection_fg: [u8; 3],
    pub selection_bg: [u8; 3],
    pub success: [u8; 3],
    pub warning: [u8; 3],
    pub error: [u8; 3],
}
