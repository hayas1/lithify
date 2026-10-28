use std::convert::Infallible;
use std::fmt::{self, Display, Formatter};

use lithify_core::artifact::{ArtifactName, UnbuildableArtifact};
use lithify_core::listener::OutOfOrder;
use lithify_pipeline::Unreadable;
use lithify_pipeline::build_log::UnreadableBuildLog;
use lithify_pipeline::runner::Unsteppable;
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
        artifact: ArtifactName,
    },
    SnapshotPlacedTwice(String),
    Unbased {
        artifact: ArtifactName,
        base: ArtifactName,
    },
    WithoutSource,
    Unsteppable(Unsteppable),
    Unlogged(UnreadableBuildLog),
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
            Self::SnapshotPlacedTwice(sink) => {
                write!(formatter, "{sink} places more than one snapshot")
            }
            Self::Unbased { artifact, base } => {
                write!(
                    formatter,
                    "{artifact} is built from {base}, which is no snapshot"
                )
            }
            Self::Unreached => write!(
                formatter,
                "the change log lies in a warehouse other than the one it is sunk into"
            ),
            Self::Unsteppable(cause) => write!(formatter, "{cause}"),
            Self::Unlogged(cause) => write!(formatter, "{cause}"),
            Self::Uncredentialed(warehouse) => write!(
                formatter,
                "a {warehouse} sink is read with a credential, but none was given"
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
            Self::Unsteppable(cause) => Some(cause),
            Self::Unlogged(cause) => Some(cause),
            Self::UnknownSink(_)
            | Self::Unreached
            | Self::Undeclared { .. }
            | Self::SnapshotPlacedTwice(_)
            | Self::Unbased { .. }
            | Self::WithoutSource
            | Self::Uncredentialed(_) => None,
        }
    }
}

impl From<Unreadable> for Unrunnable {
    fn from(cause: Unreadable) -> Self {
        Self::Configuration(Box::new(cause))
    }
}

impl From<UnreadableBuildLog> for Unrunnable {
    fn from(cause: UnreadableBuildLog) -> Self {
        Self::Unlogged(cause)
    }
}

impl From<Unsteppable> for Unrunnable {
    fn from(cause: Unsteppable) -> Self {
        Self::Unsteppable(cause)
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
