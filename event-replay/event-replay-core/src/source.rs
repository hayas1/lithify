use std::fmt;

use crate::event::SchemaChange;
use crate::position::Position;
use crate::sink::TablePath;
use crate::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPath(Vec<String>);

impl ColumnPath {
    pub fn parts(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }
}

impl From<&str> for ColumnPath {
    fn from(path: &str) -> Self {
        Self(path.split('.').map(str::to_owned).collect())
    }
}

impl fmt::Display for ColumnPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.join("."))
    }
}

pub trait ChangeLog {
    type Position: Position;

    fn key(&self) -> ColumnPath;

    fn operation(&self) -> ColumnPath;

    fn before(&self) -> ColumnPath;

    fn after(&self) -> ColumnPath;

    fn recorded_at(&self) -> ColumnPath;

    fn ordering(&self) -> Vec<ColumnPath>;
}

pub trait SchemaChangeStream<L: ChangeLog> {
    type Error: std::error::Error;
    type Changes<'a>: SchemaChanges<L, Error = Self::Error>
    where
        Self: 'a,
        L: 'a;

    fn schema_changes<'a>(
        &'a self,
        log: &'a L,
        table: &TablePath,
        window: &Window,
    ) -> Result<Self::Changes<'a>, Self::Error>;
}

pub type Streamed<P, E> = Result<SchemaChange<P>, E>;

pub trait SchemaChanges<L: ChangeLog> {
    type Error: std::error::Error;

    fn stream(
        &mut self,
    ) -> Result<impl Iterator<Item = Streamed<L::Position, Self::Error>>, Self::Error>;
}
