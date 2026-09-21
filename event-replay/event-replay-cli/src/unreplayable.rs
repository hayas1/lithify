use std::fmt::{self, Display, Formatter};

use event_replay_config::Unreadable;
use event_replay_core::listener::OutOfOrder;

#[derive(Debug)]
pub enum Unreplayable {
    Unreadable(Unreadable),
    Unqueryable(duckdb::Error),
    OutOfOrder(OutOfOrder),
    WithoutReadableSink,
}

impl Display for Unreplayable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreadable(cause) => write!(formatter, "{cause}"),
            Self::Unqueryable(cause) => write!(formatter, "{cause}"),
            Self::OutOfOrder(cause) => write!(formatter, "{cause}"),
            Self::WithoutReadableSink => {
                write!(formatter, "no sink holds the change log in object storage")
            }
        }
    }
}

impl std::error::Error for Unreplayable {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Unreadable(cause) => Some(cause),
            Self::Unqueryable(cause) => Some(cause),
            Self::OutOfOrder(cause) => Some(cause),
            Self::WithoutReadableSink => None,
        }
    }
}

impl From<Unreadable> for Unreplayable {
    fn from(cause: Unreadable) -> Self {
        Self::Unreadable(cause)
    }
}

impl From<duckdb::Error> for Unreplayable {
    fn from(cause: duckdb::Error) -> Self {
        Self::Unqueryable(cause)
    }
}

impl From<OutOfOrder> for Unreplayable {
    fn from(cause: OutOfOrder) -> Self {
        Self::OutOfOrder(cause)
    }
}
