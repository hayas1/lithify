use std::convert::Infallible;
use std::fmt::{self, Display, Formatter};

use chrono::NaiveDate;

use lithify_config::Unreadable;
use lithify_core::artifact::UnbuildableArtifact;
use lithify_core::listener::OutOfOrder;
use lithify_core::model::TableId;
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;
use lithify_protocol::sink::bigquery::client::Unqueryable;

#[derive(Debug)]
pub enum Unrunnable {
    Configuration(Box<Unreadable>),
    Unqueryable(duckdb::Error),
    Unanswerable(Unqueryable),
    OutOfOrder(OutOfOrder),
    Unbuildable(UnbuildableArtifact),
    Unwritable(std::io::Error),
    Unserializable(serde_json::Error),
    UnknownSink(String),
    Unreached,
    Undeclared {
        sink: String,
        artifact: &'static str,
    },
    CaptureChanged {
        logged: String,
        configured: String,
    },
    WithoutSource,
    Unsharded(NaiveDate),
    Unclosed(Window),
    Overtaken {
        date: NaiveDate,
        built_until: Ts<Event>,
    },
    Baseless {
        window: Window,
        tables: Vec<TableId>,
    },
    UnknownTables(Vec<TableId>),
    Uncredentialed(&'static str),
}

impl Display for Unrunnable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(cause) => write!(formatter, "{cause}"),
            Self::Unqueryable(cause) => write!(formatter, "{cause}"),
            Self::Unanswerable(cause) => write!(formatter, "{cause}"),
            Self::OutOfOrder(cause) => write!(formatter, "{cause}"),
            Self::Unbuildable(cause) => write!(formatter, "{cause}"),
            Self::Unwritable(cause) => write!(formatter, "{cause}"),
            Self::Unserializable(cause) => write!(formatter, "{cause}"),
            Self::UnknownSink(name) => {
                write!(formatter, "nothing in this pipeline is sunk into {name}")
            }
            Self::WithoutSource => write!(formatter, "this pipeline captures no server"),
            Self::Undeclared { sink, artifact } => {
                write!(formatter, "{sink} places no {artifact}")
            }
            Self::Unreached => write!(
                formatter,
                "the change log lies in a warehouse other than the one it is sunk into"
            ),
            Self::CaptureChanged { logged, configured } => write!(
                formatter,
                "the build log was captured as {logged} but the pipeline is captured as {configured} now"
            ),
            Self::Unsharded(date) => write!(formatter, "the schedule dates no shard {date}"),
            Self::Unclosed(window) => write!(
                formatter,
                "the window {} .. {} has not closed yet",
                window.from(),
                window.to()
            ),
            Self::Overtaken { date, built_until } => write!(
                formatter,
                "the build log has built past the shard dated {date}, up to {built_until}; rebuild that shard with `lithify build --snapshot {date}` instead"
            ),
            Self::Baseless { window, tables } => write!(
                formatter,
                "nothing was built for {} closing at {}, which the window to {} folds onto; build the windows before it first",
                tables
                    .iter()
                    .map(TableId::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                window.from(),
                window.to()
            ),
            Self::Uncredentialed(warehouse) => write!(
                formatter,
                "a {warehouse} sink is read with a credential, but none was given"
            ),
            Self::UnknownTables(tables) => write!(
                formatter,
                "the window holds no table named {}",
                tables
                    .iter()
                    .map(TableId::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

impl std::error::Error for Unrunnable {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Configuration(cause) => Some(cause.as_ref()),
            Self::Unqueryable(cause) => Some(cause),
            Self::Unanswerable(cause) => Some(cause),
            Self::OutOfOrder(cause) => Some(cause),
            Self::Unbuildable(cause) => Some(cause),
            Self::Unwritable(cause) => Some(cause),
            Self::Unserializable(cause) => Some(cause),
            Self::UnknownSink(_)
            | Self::Unreached
            | Self::Undeclared { .. }
            | Self::WithoutSource
            | Self::CaptureChanged { .. }
            | Self::Unsharded(_)
            | Self::Unclosed(_)
            | Self::Overtaken { .. }
            | Self::Baseless { .. }
            | Self::UnknownTables(_)
            | Self::Uncredentialed(_) => None,
        }
    }
}

impl From<Unreadable> for Unrunnable {
    fn from(cause: Unreadable) -> Self {
        Self::Configuration(Box::new(cause))
    }
}

impl From<duckdb::Error> for Unrunnable {
    fn from(cause: duckdb::Error) -> Self {
        Self::Unqueryable(cause)
    }
}

impl From<Unqueryable> for Unrunnable {
    fn from(cause: Unqueryable) -> Self {
        Self::Unanswerable(cause)
    }
}

impl From<OutOfOrder> for Unrunnable {
    fn from(cause: OutOfOrder) -> Self {
        Self::OutOfOrder(cause)
    }
}

impl From<UnbuildableArtifact> for Unrunnable {
    fn from(cause: UnbuildableArtifact) -> Self {
        Self::Unbuildable(cause)
    }
}

impl From<std::io::Error> for Unrunnable {
    fn from(cause: std::io::Error) -> Self {
        Self::Unwritable(cause)
    }
}

impl From<serde_json::Error> for Unrunnable {
    fn from(cause: serde_json::Error) -> Self {
        Self::Unserializable(cause)
    }
}

impl From<Infallible> for Unrunnable {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}
