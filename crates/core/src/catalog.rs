use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
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
}

#[derive(Serialize)]
pub struct ActionDescriptor {
    pub id: Action,
    pub keys: &'static [&'static str],
    pub description: &'static str,
}

pub const ACTIONS: &[ActionDescriptor] = &[
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
        description: "Select the previous item; scroll up in detail",
    },
    ActionDescriptor {
        id: Action::Down,
        keys: &["j", "Down"],
        description: "Select the next item; scroll down in detail",
    },
    ActionDescriptor {
        id: Action::Open,
        keys: &["Enter"],
        description: "Open the selected resource or row detail",
    },
    ActionDescriptor {
        id: Action::Back,
        keys: &["Esc"],
        description: "Return to the retained parent view; close help/detail first",
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
        description: "Return to a retained previous page; previous text chunk in detail",
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
        description: "Quit and restore the terminal",
    },
    ActionDescriptor {
        id: Action::Cancel,
        keys: &["Ctrl-c"],
        description: "Cancel active work; quit when idle",
    },
];

#[derive(Serialize)]
pub struct ResourceDescriptor {
    pub id: &'static str,
    pub description: &'static str,
    pub columns: &'static [&'static str],
}

pub const CONNECTIONS: ResourceDescriptor = ResourceDescriptor {
    id: "connections",
    description: "Configured aliases; secrets resolved only when selected",
    columns: &["alias", "datasource", "browsing"],
};
