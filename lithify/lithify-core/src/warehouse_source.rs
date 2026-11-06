use crate::event::{MalformedEvent, SchemaChange};
use crate::listener::{CatalogModel, OutOfOrder};
use crate::model::{TableId, TableModel};
use crate::position::Position;
use crate::sink::TablePath;
use crate::source::{ChangeLog, ColumnPath, SchemaHistory};
use crate::ts::{Event, Ts};
use crate::window::Window;

pub trait SchemaChangeLog: ChangeLog {
    type Record;

    fn ddl_landed_table(&self) -> TablePath;

    fn schema_change(
        &self,
        record: Self::Record,
    ) -> Result<SchemaChange<Self::Position>, MalformedEvent>;
}

pub trait RowChangeLog: ChangeLog {
    fn dml_landed_table(&self, table: &TableId) -> TablePath;

    fn operation(&self) -> ColumnPath;

    fn before(&self) -> ColumnPath;

    fn after(&self) -> ColumnPath;
}

pub trait ChangeLogs {
    type Position: Position;
    type SchemaChangeLog: SchemaChangeLog<Position = Self::Position>;
    type RowChangeLog: RowChangeLog<Position = Self::Position>;

    fn schema_changes(&self) -> &Self::SchemaChangeLog;

    fn row_changes(&self) -> &Self::RowChangeLog;
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

    async fn schema_changes<'a>(
        &'a self,
        log: &'a L,
        window: &Window,
    ) -> Result<Self::Changes<'a>, Self::Error>;

    async fn each_schema_change<E>(
        &self,
        log: &L,
        window: &Window,
        mut visit: impl FnMut(SchemaChange<L::Position>) -> Result<(), E>,
    ) -> Result<(), E>
    where
        Self::Error: Into<E>,
    {
        let mut changes = self.schema_changes(log, window).await.map_err(Into::into)?;

        for change in changes.stream().map_err(Into::into)? {
            visit(change.map_err(Into::into)?)?;
        }

        Ok(())
    }

    async fn history<E>(&self, log: &L, window: &Window) -> Result<SchemaHistory<L::Position>, E>
    where
        Self::Error: Into<E>,
    {
        Ok(SchemaHistory::new(
            self.collected::<E>(log, &Window::until(*window.from()))
                .await?,
            self.collected::<E>(log, window).await?,
        ))
    }

    async fn collected<E>(
        &self,
        log: &L,
        window: &Window,
    ) -> Result<Vec<SchemaChange<L::Position>>, E>
    where
        Self::Error: Into<E>,
    {
        let mut changes = Vec::new();

        self.each_schema_change::<E>(log, window, |change| {
            changes.push(change);
            Ok(())
        })
        .await?;

        Ok(changes)
    }

    async fn catalog_at<E>(&self, log: &L, at: Ts<Event>) -> Result<CatalogModel<L::Position>, E>
    where
        Self::Error: Into<E>,
        E: From<OutOfOrder>,
    {
        Ok(self
            .history::<E>(log, &Window::new(at, at))
            .await?
            .replay::<OutOfOrder, ()>(&(), &mut ())?)
    }

    async fn table<E>(
        &self,
        log: &L,
        table: &TableId,
        at: Ts<Event>,
    ) -> Result<Option<TableModel>, E>
    where
        Self::Error: Into<E>,
        E: From<OutOfOrder>,
    {
        let catalog = self.catalog_at::<E>(log, at).await?;

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
