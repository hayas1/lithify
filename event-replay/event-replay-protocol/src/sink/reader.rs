use std::fmt::{self, Display, Formatter};

use event_replay_core::listener::{CatalogModel, OutOfOrder, SchemaChangeListener};
use event_replay_core::sink::TablePath;
use event_replay_core::source::{SchemaChangeStream, SchemaChanges};
use event_replay_core::window::Window;

use crate::sink::bigquery::client::{BigQueryClient, Unqueryable};
use crate::sink::duckdb::store::ObjectStore;
use crate::source::debezium::{Debezium, DebeziumDdl, Dialect};

pub enum ChangeLogReader {
    DuckDb(ObjectStore),
    BigQuery(BigQueryClient),
}

impl ChangeLogReader {
    pub fn read<D: Dialect>(
        &self,
        log: &Debezium<D>,
        table: &TablePath,
        window: &Window,
        catalog: &mut CatalogModel<D::Position>,
        listener: &mut impl SchemaChangeListener<D::Position>,
    ) -> Result<(), UnreadableChangeLog>
    where
        D::Source: google_cloud_bigquery::query::FromSql,
        DebeziumDdl<D::Source>: duckdb::types::FromSql,
    {
        match self {
            Self::DuckDb(store) => {
                Self::drain(&store.client(table)?, log, table, window, catalog, listener)
            }
            Self::BigQuery(client) => Self::drain(client, log, table, window, catalog, listener),
        }
    }

    fn drain<D: Dialect, S>(
        client: &S,
        log: &Debezium<D>,
        table: &TablePath,
        window: &Window,
        catalog: &mut CatalogModel<D::Position>,
        listener: &mut impl SchemaChangeListener<D::Position>,
    ) -> Result<(), UnreadableChangeLog>
    where
        S: SchemaChangeStream<Debezium<D>>,
        UnreadableChangeLog: From<S::Error>,
    {
        let mut changes = client.schema_changes(log, table, window)?;

        for change in changes.stream()? {
            catalog.accept(&change?, listener)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum UnreadableChangeLog {
    Unqueryable(duckdb::Error),
    Unanswerable(Unqueryable),
    OutOfOrder(OutOfOrder),
}

impl Display for UnreadableChangeLog {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unqueryable(cause) => write!(formatter, "{cause}"),
            Self::Unanswerable(cause) => write!(formatter, "{cause}"),
            Self::OutOfOrder(cause) => write!(formatter, "{cause}"),
        }
    }
}

impl std::error::Error for UnreadableChangeLog {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Unqueryable(cause) => Some(cause),
            Self::Unanswerable(cause) => Some(cause),
            Self::OutOfOrder(cause) => Some(cause),
        }
    }
}

impl From<duckdb::Error> for UnreadableChangeLog {
    fn from(cause: duckdb::Error) -> Self {
        Self::Unqueryable(cause)
    }
}

impl From<Unqueryable> for UnreadableChangeLog {
    fn from(cause: Unqueryable) -> Self {
        Self::Unanswerable(cause)
    }
}

impl From<OutOfOrder> for UnreadableChangeLog {
    fn from(cause: OutOfOrder) -> Self {
        Self::OutOfOrder(cause)
    }
}
