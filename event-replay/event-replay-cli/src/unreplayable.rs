use std::fmt::{self, Display, Formatter};

use event_replay_config::Unreadable;
use event_replay_core::artifact::UnbuildableArtifact;
use event_replay_core::listener::OutOfOrder;
use event_replay_core::position::RecordedAt;
use event_replay_protocol::sink::bigquery::client::Unqueryable;

#[derive(Debug)]
pub enum Unreplayable {
    Configuration(Unreadable),
    Unqueryable(duckdb::Error),
    Unanswerable(Unqueryable),
    OutOfOrder(OutOfOrder),
    Unbuildable(UnbuildableArtifact),
    Unshardable(RecordedAt),
    Unwritable(std::io::Error),
    UnknownSink(String),
    WithoutSource,
    WithoutTopic(String),
}

impl Display for Unreplayable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(cause) => write!(formatter, "{cause}"),
            Self::Unqueryable(cause) => write!(formatter, "{cause}"),
            Self::Unanswerable(cause) => write!(formatter, "{cause}"),
            Self::OutOfOrder(cause) => write!(formatter, "{cause}"),
            Self::Unbuildable(cause) => write!(formatter, "{cause}"),
            Self::Unshardable(at) => write!(
                formatter,
                "{} milliseconds since the epoch is no instant to name a shard after",
                at.milliseconds_since_epoch()
            ),
            Self::Unwritable(cause) => write!(formatter, "{cause}"),
            Self::UnknownSink(name) => {
                write!(formatter, "nothing in this pipeline is sunk into {name}")
            }
            Self::WithoutSource => write!(formatter, "this pipeline captures no server"),
            Self::WithoutTopic(source) => {
                write!(
                    formatter,
                    "{source} names no topic its changes landed under"
                )
            }
        }
    }
}

impl std::error::Error for Unreplayable {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Configuration(cause) => Some(cause),
            Self::Unqueryable(cause) => Some(cause),
            Self::Unanswerable(cause) => Some(cause),
            Self::OutOfOrder(cause) => Some(cause),
            Self::Unbuildable(cause) => Some(cause),
            Self::Unwritable(cause) => Some(cause),
            Self::Unshardable(_)
            | Self::UnknownSink(_)
            | Self::WithoutSource
            | Self::WithoutTopic(_) => None,
        }
    }
}

impl From<Unreadable> for Unreplayable {
    fn from(cause: Unreadable) -> Self {
        Self::Configuration(cause)
    }
}

impl From<duckdb::Error> for Unreplayable {
    fn from(cause: duckdb::Error) -> Self {
        Self::Unqueryable(cause)
    }
}

impl From<Unqueryable> for Unreplayable {
    fn from(cause: Unqueryable) -> Self {
        Self::Unanswerable(cause)
    }
}

impl From<OutOfOrder> for Unreplayable {
    fn from(cause: OutOfOrder) -> Self {
        Self::OutOfOrder(cause)
    }
}

impl From<UnbuildableArtifact> for Unreplayable {
    fn from(cause: UnbuildableArtifact) -> Self {
        Self::Unbuildable(cause)
    }
}

impl From<std::io::Error> for Unreplayable {
    fn from(cause: std::io::Error) -> Self {
        Self::Unwritable(cause)
    }
}
