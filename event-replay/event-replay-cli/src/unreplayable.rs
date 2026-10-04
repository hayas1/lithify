use std::fmt::{self, Display, Formatter};

use event_replay_config::Unreadable;
use event_replay_core::listener::OutOfOrder;
use event_replay_protocol::sink::bigquery::client::Unqueryable;

#[derive(Debug)]
pub enum Unreplayable {
    Configuration(Unreadable),
    Unqueryable(duckdb::Error),
    Unanswerable(Unqueryable),
    OutOfOrder(OutOfOrder),
    UnknownSink(String),
    WithoutSource,
}

impl Display for Unreplayable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(cause) => write!(formatter, "{cause}"),
            Self::Unqueryable(cause) => write!(formatter, "{cause}"),
            Self::Unanswerable(cause) => write!(formatter, "{cause}"),
            Self::OutOfOrder(cause) => write!(formatter, "{cause}"),
            Self::UnknownSink(name) => {
                write!(formatter, "nothing in this pipeline is sunk into {name}")
            }
            Self::WithoutSource => write!(formatter, "this pipeline captures no server"),
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
            Self::UnknownSink(_) | Self::WithoutSource => None,
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
