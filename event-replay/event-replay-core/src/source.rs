use std::fmt;

use crate::event::{MalformedEvent, SchemaChange};
use crate::listener::{CatalogModel, OutOfOrder, SchemaChangeListener};
use crate::model::{TableId, TableModel};
use crate::position::{Position, RecordedAt};
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

// The log stays a parameter of the trait rather than of its methods: each warehouse decodes the
// records of a log its own way (DuckDB with FromSql, BigQuery with TryFrom<Row>), a bound a method
// could not take on per warehouse, and naming the row instead would need a higher-ranked bound
// over the lifetime of a DuckDB row.
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

    fn each_schema_change<E>(
        &self,
        log: &L,
        table: &TablePath,
        window: &Window,
        mut visit: impl FnMut(SchemaChange<L::Position>) -> Result<(), E>,
    ) -> Result<(), E>
    where
        Self::Error: Into<E>,
    {
        let mut changes = self
            .schema_changes(log, table, window)
            .map_err(Into::into)?;

        for change in changes.stream().map_err(Into::into)? {
            visit(change.map_err(Into::into)?)?;
        }

        Ok(())
    }

    fn catalog<E, V>(
        &self,
        context: &V::Context,
        log: &L,
        table: &TablePath,
        window: &Window,
        catalog: &mut CatalogModel<L::Position>,
        listener: &mut V,
    ) -> Result<(), E>
    where
        V: SchemaChangeListener<L::Position>,
        Self::Error: Into<E>,
        E: From<OutOfOrder> + From<V::Error>,
    {
        self.each_schema_change(log, table, window, |change| {
            catalog.accept::<E, V>(context, &change, listener)
        })
    }

    fn catalog_at<E>(
        &self,
        log: &L,
        schema_changes: &TablePath,
        at: RecordedAt,
    ) -> Result<CatalogModel<L::Position>, E>
    where
        Self::Error: Into<E>,
        E: From<OutOfOrder>,
    {
        let mut catalog = CatalogModel::default();

        self.each_schema_change::<E>(log, schema_changes, &Window::until(at), |change| {
            Ok(catalog.accept::<OutOfOrder, ()>(&(), &change, &mut ())?)
        })?;

        Ok(catalog)
    }

    fn replay<E, V>(
        &self,
        context: &V::Context,
        log: &L,
        schema_changes: &TablePath,
        window: &Window,
        listener: &mut V,
    ) -> Result<CatalogModel<L::Position>, E>
    where
        V: SchemaChangeListener<L::Position>,
        Self::Error: Into<E>,
        E: From<OutOfOrder> + From<V::Error>,
    {
        let mut catalog = self.catalog_at::<E>(log, schema_changes, *window.from())?;

        self.catalog::<E, V>(context, log, schema_changes, window, &mut catalog, listener)?;

        Ok(catalog)
    }

    fn table<E>(
        &self,
        log: &L,
        schema_changes: &TablePath,
        table: &TableId,
        at: RecordedAt,
    ) -> Result<Option<TableModel>, E>
    where
        Self::Error: Into<E>,
        E: From<OutOfOrder>,
    {
        let catalog = self.catalog_at::<E>(log, schema_changes, at)?;

        Ok(catalog.table(table).cloned())
    }
}

pub type Streamed<P, E> = Result<SchemaChange<P>, E>;

pub trait SchemaChanges<L: SchemaChangeLog> {
    type Error: std::error::Error;

    fn stream(
        &mut self,
    ) -> Result<impl Iterator<Item = Streamed<L::Position, Self::Error>>, Self::Error>;
}
