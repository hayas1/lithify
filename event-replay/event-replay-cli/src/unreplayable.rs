use std::fmt::{self, Display, Formatter};

use event_replay_config::Unreadable;
use event_replay_protocol::sink::bigquery::client::Unqueryable;
use event_replay_protocol::sink::reader::UnreadableChangeLog;

#[derive(Debug)]
pub enum Unreplayable {
    Configuration(Unreadable),
    ChangeLog(UnreadableChangeLog),
    Warehouse(Unqueryable),
    UnknownSink(String),
    WithoutSource,
}

impl Display for Unreplayable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(cause) => write!(formatter, "{cause}"),
            Self::ChangeLog(cause) => write!(formatter, "{cause}"),
            Self::Warehouse(cause) => write!(formatter, "{cause}"),
            Self::UnknownSink(name) => write!(formatter, "the host is not sunk into {name}"),
            Self::WithoutSource => write!(formatter, "the host is captured from no server"),
        }
    }
}

impl std::error::Error for Unreplayable {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Configuration(cause) => Some(cause),
            Self::ChangeLog(cause) => Some(cause),
            Self::Warehouse(cause) => Some(cause),
            Self::UnknownSink(_) | Self::WithoutSource => None,
        }
    }
}

impl From<Unreadable> for Unreplayable {
    fn from(cause: Unreadable) -> Self {
        Self::Configuration(cause)
    }
}

impl From<UnreadableChangeLog> for Unreplayable {
    fn from(cause: UnreadableChangeLog) -> Self {
        Self::ChangeLog(cause)
    }
}

impl From<Unqueryable> for Unreplayable {
    fn from(cause: Unqueryable) -> Self {
        Self::Warehouse(cause)
    }
}
