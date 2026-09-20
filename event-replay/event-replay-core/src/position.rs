use std::fmt;

use serde_json::Value;

use crate::schema::{MalformedEvent, SchemaChange};

pub trait Position: PartialOrd + Clone + fmt::Debug {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordedAt(i64);

impl RecordedAt {
    pub fn milliseconds_since_epoch(&self) -> i64 {
        self.0
    }
}

impl From<i64> for RecordedAt {
    fn from(milliseconds_since_epoch: i64) -> Self {
        Self(milliseconds_since_epoch)
    }
}

pub trait Dialect {
    type Position: Position;

    fn position(&self, source: Value) -> Result<Self::Position, MalformedEvent>;

    fn schema_change(
        &self,
        envelope: Value,
    ) -> Result<SchemaChange<Self::Position>, MalformedEvent> {
        SchemaChange::read(envelope, |source| self.position(source))
    }
}
