use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Add,
    Follow,
    Query,
    History,
    Up,
    Down,
    Open,
    Back,
    Connections,
    Next,
    Previous,
    Refresh,
    Help,
    Quit,
    Cancel,
    Columns,
    Left,
    Right,
    Filter,
    Sort,
    Themes,
    Display,
    ScrollLeft,
    ScrollRight,
    PageUp,
    PageDown,
    HalfPageUp,
    HalfPageDown,
}

#[derive(Serialize)]
pub struct ActionDescriptor {
    pub id: Action,
    pub keys: &'static [&'static str],
    pub description: &'static str,
}

pub const ACTIONS: &[ActionDescriptor] = &[
    ActionDescriptor {
        id: Action::Add,
        keys: &["a"],
        description: "Add a connection: choose a datasource, fill fields, F2 saves to the displayed config path; Esc discards",
    },
    ActionDescriptor {
        id: Action::Follow,
        keys: &["f"],
        description: "Start live following from the current end; press again to stop and retain data",
    },
    ActionDescriptor {
        id: Action::Query,
        keys: &["e"],
        description: "Edit a native read-only query above retained rows; Enter/F5 executes, Shift-Enter inserts a line, Esc returns",
    },
    ActionDescriptor {
        id: Action::History,
        keys: &["H"],
        description: "Browse recent queries submitted on this connection; Enter opens one for editing",
    },
    ActionDescriptor {
        id: Action::Themes,
        keys: &["T"],
        description: "Choose a theme: j/k previews, Enter keeps for this session, Esc restores; config is unchanged",
    },
    ActionDescriptor {
        id: Action::Display,
        keys: &["v"],
        description: "Display menu: field format; session pretty-print, highlight, word-wrap and Unicode controls",
    },
    ActionDescriptor {
        id: Action::Filter,
        keys: &["/"],
        description: "Filter this page as you type; Enter keeps, empty clears, Esc restores the previous filter",
    },
    ActionDescriptor {
        id: Action::Sort,
        keys: &["s"],
        description: "Sort selected field locally: lexical ascending, descending, then source order",
    },
    ActionDescriptor {
        id: Action::Columns,
        keys: &["m"],
        description: "Open column metadata for the selected relation or current row view",
    },
    ActionDescriptor {
        id: Action::Left,
        keys: &["h", "Left"],
        description: "Select the previous field",
    },
    ActionDescriptor {
        id: Action::Right,
        keys: &["l", "Right"],
        description: "Select the next field",
    },
    ActionDescriptor {
        id: Action::Up,
        keys: &["k", "Up"],
        description: "Select the previous item; scroll up in detail; preview the previous theme",
    },
    ActionDescriptor {
        id: Action::Down,
        keys: &["j", "Down"],
        description: "Select the next item; scroll down in detail; preview the next theme",
    },
    ActionDescriptor {
        id: Action::Open,
        keys: &["Enter"],
        description: "Open resource or all row fields; Enter on a field opens its full value; keep theme preview",
    },
    ActionDescriptor {
        id: Action::Back,
        keys: &["Esc"],
        description: "Return to the retained parent view; close help/detail first; restore theme when its menu is open",
    },
    ActionDescriptor {
        id: Action::Connections,
        keys: &["c"],
        description: "Return to the connection picker and cancel active work",
    },
    ActionDescriptor {
        id: Action::Next,
        keys: &["n"],
        description: "Fetch the next page; next text chunk in detail",
    },
    ActionDescriptor {
        id: Action::Previous,
        keys: &["p"],
        description: "Return to the previous page from cache or refetch its bookmark; previous text chunk in detail",
    },
    ActionDescriptor {
        id: Action::Refresh,
        keys: &["r"],
        description: "Reload the current resource from its first page",
    },
    ActionDescriptor {
        id: Action::Help,
        keys: &["?"],
        description: "Show actions available in this context",
    },
    ActionDescriptor {
        id: Action::Quit,
        keys: &["q"],
        description: "Ask before quitting and restoring the terminal",
    },
    ActionDescriptor {
        id: Action::Cancel,
        keys: &["Ctrl-c"],
        description: "Restore theme when its menu is open; otherwise cancel active work or quit when idle",
    },
    ActionDescriptor {
        id: Action::ScrollLeft,
        keys: &["H"],
        description: "Scroll content left when word-wrap is off",
    },
    ActionDescriptor {
        id: Action::ScrollRight,
        keys: &["L"],
        description: "Scroll content right when word-wrap is off",
    },
    ActionDescriptor {
        id: Action::PageUp,
        keys: &["PageUp"],
        description: "Scroll up one screen within loaded data or detail; no datasource fetch",
    },
    ActionDescriptor {
        id: Action::PageDown,
        keys: &["PageDown"],
        description: "Scroll down one screen within loaded data or detail; no datasource fetch",
    },
    ActionDescriptor {
        id: Action::HalfPageUp,
        keys: &["Ctrl-u"],
        description: "Scroll up half a screen within loaded data or detail; no datasource fetch",
    },
    ActionDescriptor {
        id: Action::HalfPageDown,
        keys: &["Ctrl-d"],
        description: "Scroll down half a screen within loaded data or detail; no datasource fetch",
    },
];

#[derive(Serialize)]
pub struct ResourceDescriptor {
    pub id: &'static str,
    pub description: &'static str,
    pub columns: &'static [&'static str],
    pub paging: bool,
    pub actions: &'static [ResourceAction],
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionSource {
    Current,
    SelectedTarget,
}

#[derive(Serialize)]
pub struct ResourceAction {
    pub id: Action,
    pub target: &'static str,
    pub source: ActionSource,
}

impl ResourceAction {
    pub fn target(
        &self,
        current: &crate::Resource,
        row: Option<&crate::Row>,
    ) -> Option<crate::Resource> {
        let source = match self.source {
            ActionSource::Current => current,
            ActionSource::SelectedTarget => row?.target.as_ref()?,
        };
        Some(crate::Resource::new(self.target, source.path.clone()))
    }
}

pub const CONNECTIONS: ResourceDescriptor = ResourceDescriptor {
    id: "connections",
    description: "Configured aliases; secrets resolved only when selected",
    columns: &["alias", "datasource", "browsing"],
    paging: false,
    actions: &[],
};
