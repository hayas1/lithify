use std::fmt::{self, Display, Formatter};

use google_cloud_auth::credentials::anonymous;
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::error::{QueryError, RowError};
use google_cloud_bigquery::query::Row;
use google_cloud_gax::client_builder;
use tokio::runtime::Runtime;

use event_replay_core::event::MalformedEvent;
use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::source::{
    RowChangeLog, SchemaChangeLog, SchemaChangeStream, SchemaChanges, Streamed,
};
use event_replay_core::window::Window;

use crate::sink::bigquery::BigQueryWarehouse;
use crate::sink::bigquery::config::{Config, Credentials};

pub trait BigQueryLogFn {
    type Output;

    fn call_bigquery<L>(self, log: L) -> Self::Output
    where
        L: SchemaChangeLog + RowChangeLog,
        L::Record: TryFrom<Row, Error = RowError>;
}

pub trait BigQueryCapture {
    fn with_bigquery_log<F: BigQueryLogFn>(&self, f: F) -> F::Output;
}

pub struct BigQueryClient {
    warehouse: BigQueryWarehouse,
    client: BigQuery,
    runtime: Runtime,
}

impl BigQueryClient {
    pub fn open(config: &Config) -> Result<Self, Unqueryable> {
        let runtime = Runtime::new().map_err(Unqueryable::Unrunnable)?;
        let builder = BigQuery::builder()
            .with_project_id(config.project())
            .with_endpoint(config.endpoint());
        let builder = match config.credentials() {
            Credentials::Anonymous => builder.with_credentials(anonymous::Builder::new().build()),
            Credentials::Adc => builder,
        };
        let client = runtime
            .block_on(builder.build())
            .map_err(Unqueryable::Unbuildable)?;

        Ok(Self {
            warehouse: BigQueryWarehouse::new(config.project()),
            client,
            runtime,
        })
    }

    fn records<R>(&self, query: &str) -> Result<Vec<R>, Unqueryable>
    where
        R: TryFrom<Row, Error = RowError>,
    {
        self.runtime.block_on(async {
            let mut read = self.client.query(query).until_done().await?.read();
            let mut records = Vec::new();

            while let Some(row) = read.next().await.transpose()? {
                records.push(R::try_from(row)?);
            }

            Ok(records)
        })
    }
}

pub struct BigQuerySchemaChanges<'a, L: SchemaChangeLog> {
    records: Vec<L::Record>,
    log: &'a L,
}

impl<L: SchemaChangeLog> SchemaChanges<L> for BigQuerySchemaChanges<'_, L> {
    type Error = Unqueryable;

    fn stream(
        &mut self,
    ) -> Result<impl Iterator<Item = Streamed<L::Position, Self::Error>>, Self::Error> {
        let log = self.log;

        Ok(std::mem::take(&mut self.records)
            .into_iter()
            .map(move |record| log.schema_change(record).map_err(Unqueryable::Malformed)))
    }
}

impl<L> SchemaChangeStream<L> for BigQueryClient
where
    L: SchemaChangeLog,
    L::Record: TryFrom<Row, Error = RowError>,
{
    type Error = Unqueryable;
    type Changes<'a>
        = BigQuerySchemaChanges<'a, L>
    where
        L: 'a;

    fn schema_changes<'a>(
        &'a self,
        log: &'a L,
        table: &TablePath,
        window: &Window,
    ) -> Result<Self::Changes<'a>, Self::Error> {
        Ok(BigQuerySchemaChanges {
            records: self.records(&self.warehouse.select_schema_changes(log, table, window))?,
            log,
        })
    }
}

#[derive(Debug)]
pub enum Unqueryable {
    Unrunnable(std::io::Error),
    Unbuildable(client_builder::Error),
    Unanswered(QueryError),
    Unreadable(RowError),
    Malformed(MalformedEvent),
}

impl Display for Unqueryable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unrunnable(cause) => write!(formatter, "{cause}"),
            Self::Unbuildable(cause) => write!(formatter, "{cause}"),
            Self::Unanswered(cause) => write!(formatter, "{cause}"),
            Self::Unreadable(cause) => write!(formatter, "{cause}"),
            Self::Malformed(cause) => write!(formatter, "{cause}"),
        }
    }
}

impl std::error::Error for Unqueryable {}

impl From<QueryError> for Unqueryable {
    fn from(cause: QueryError) -> Self {
        Self::Unanswered(cause)
    }
}

impl From<RowError> for Unqueryable {
    fn from(cause: RowError) -> Self {
        Self::Unreadable(cause)
    }
}
