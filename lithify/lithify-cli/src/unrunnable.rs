use std::convert::Infallible;
use std::fmt::{self, Display, Formatter};

use lithify_config::Unreadable;
use lithify_core::artifact::UnbuildableArtifact;
use lithify_core::listener::OutOfOrder;
use lithify_protocol::sink::bigquery::client::Unqueryable;

#[derive(Debug)]
pub enum Unrunnable {
    Configuration(Unreadable),
    Unqueryable(duckdb::Error),
    Unanswerable(Unqueryable),
    OutOfOrder(OutOfOrder),
    Unbuildable(UnbuildableArtifact),
    Unwritable(std::io::Error),
    Unserializable(serde_json::Error),
    UnknownSink(String),
    WithoutSource,
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
        }
    }
}

impl std::error::Error for Unrunnable {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Configuration(cause) => Some(cause),
            Self::Unqueryable(cause) => Some(cause),
            Self::Unanswerable(cause) => Some(cause),
            Self::OutOfOrder(cause) => Some(cause),
            Self::Unbuildable(cause) => Some(cause),
            Self::Unwritable(cause) => Some(cause),
            Self::Unserializable(cause) => Some(cause),
            Self::UnknownSink(_) | Self::WithoutSource => None,
        }
    }
}

impl From<Unreadable> for Unrunnable {
    fn from(cause: Unreadable) -> Self {
        Self::Configuration(cause)
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
