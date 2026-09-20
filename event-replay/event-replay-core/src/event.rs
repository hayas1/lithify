use std::fmt;

use crate::model::{TableId, TableModel};
use crate::position::RecordedAt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableChange {
    Created(TableModel),
    Altered(TableModel),
    Dropped(TableId),
}

impl TableChange {
    pub fn id(&self) -> &TableId {
        match self {
            Self::Created(table) | Self::Altered(table) => table.id(),
            Self::Dropped(id) => id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaChange<P> {
    position: P,
    recorded_at: RecordedAt,
    statement: String,
    tables: Vec<TableChange>,
}

impl<P> SchemaChange<P> {
    pub fn position(&self) -> &P {
        &self.position
    }

    pub fn recorded_at(&self) -> &RecordedAt {
        &self.recorded_at
    }

    pub fn statement(&self) -> &str {
        &self.statement
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableChange> {
        self.tables.iter()
    }

    pub fn new(
        position: P,
        recorded_at: RecordedAt,
        statement: &str,
        tables: Vec<TableChange>,
    ) -> Self {
        Self {
            position,
            recorded_at,
            statement: statement.to_owned(),
            tables,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MalformedEvent {
    Undecodable(String),
    UnknownTableChange(String),
    UnknownTableId(String),
}

impl fmt::Display for MalformedEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Undecodable(problem) => write!(formatter, "undecodable event: {problem}"),
            Self::UnknownTableChange(kind) => write!(formatter, "unknown table change {kind}"),
            Self::UnknownTableId(id) => write!(formatter, "unknown table id {id}"),
        }
    }
}

impl std::error::Error for MalformedEvent {}
