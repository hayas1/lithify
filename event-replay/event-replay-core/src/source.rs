use std::fmt;

use crate::event::{MalformedEvent, SchemaChange};
use crate::listener::{CatalogModel, OutOfOrder, SchemaChangeListener};
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

pub trait CdcFormat {
    type Config;
}

pub trait ChangeLog {
    type Position: Position;

    fn envelope(&self) -> Option<ColumnPath>;

    fn recorded_at(&self) -> ColumnPath;

    fn ordering(&self) -> Vec<ColumnPath>;
}

pub trait SchemaChangeLog: ChangeLog {
    type Record;

    fn schema_change(
        &self,
        record: Self::Record,
    ) -> Result<SchemaChange<Self::Position>, MalformedEvent>;
}

pub trait RowChangeLog: ChangeLog {
    fn operation(&self) -> ColumnPath;

    fn before(&self) -> ColumnPath;

    fn after(&self) -> ColumnPath;
}

pub trait ChangeLogs {
    type Position: Position;
    type Schema: SchemaChangeLog<Position = Self::Position>;
    type Row: RowChangeLog<Position = Self::Position>;

    fn schema_changes(&self) -> &Self::Schema;

    fn row_changes(&self) -> &Self::Row;
}

pub trait SchemaChangeStream<L: SchemaChangeLog> {
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

    fn catalog<E>(
        &self,
        log: &L,
        table: &TablePath,
        window: &Window,
        catalog: &mut CatalogModel<L::Position>,
        listener: &mut impl SchemaChangeListener<L::Position>,
    ) -> Result<(), E>
    where
        E: From<Self::Error> + From<OutOfOrder>,
    {
        let mut changes = self.schema_changes(log, table, window)?;

        for change in changes.stream()? {
            catalog.accept(&change?, listener)?;
        }

        Ok(())
    }
}

pub type Streamed<P, E> = Result<SchemaChange<P>, E>;

pub trait SchemaChanges<L: SchemaChangeLog> {
    type Error: std::error::Error;

    fn stream(
        &mut self,
    ) -> Result<impl Iterator<Item = Streamed<L::Position, Self::Error>>, Self::Error>;
}
