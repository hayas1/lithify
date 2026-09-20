use serde_json::Value;

use crate::position::{Dialect, Position};
use crate::schema::MalformedEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(u64);

impl Position for Offset {}

impl From<u64> for Offset {
    fn from(offset: u64) -> Self {
        Self(offset)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Log;

impl Dialect for Log {
    type Position = Offset;

    fn position(&self, source: Value) -> Result<Self::Position, MalformedEvent> {
        source
            .get("offset")
            .and_then(Value::as_u64)
            .map(Offset::from)
            .ok_or(MalformedEvent::UnknownDialect(source))
    }
}
