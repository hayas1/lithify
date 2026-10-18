use std::fmt::{self, Display, Formatter};

use google_cloud_auth::credentials::anonymous;
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::error::{QueryError, RowError};
use google_cloud_bigquery::query::Row;
use google_cloud_gax::client_builder;
use tokio::runtime::Runtime;

use lithify_core::build_log::{BuildLog, BuildLogQuery, UnreadableBuildLog};
use lithify_core::event::MalformedEvent;
use lithify_core::sink::{
    ChangeLogSql, Namespace, OpenWarehouse, TablePath, WarehouseClient, WarehouseSql,
};
use lithify_core::source::{
    ChangeLogs, SchemaChangeLog, SchemaChangeStream, SchemaChanges, Streamed,
};
use lithify_core::sql::SqlString;
use lithify_core::window::Window;

use crate::sink::bigquery::BigQueryWarehouse;
use crate::sink::bigquery::config::{Config, Credentials, Location};
use crate::sink::bigquery::from_sql::{BigQueryBuildLogRow, BigQueryCount};

pub trait BigQueryChangeLogsFn {
    type Output;

    fn call_bigquery<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: TryFrom<Row, Error = RowError>;
}

pub trait BigQueryCapture {
    fn with_bigquery_change_logs<F: BigQueryChangeLogsFn>(
        &self,
        topic_prefix: &str,
        f: F,
    ) -> F::Output;
}

pub struct BigQueryClient {
    warehouse: BigQueryWarehouse,
    client: BigQuery,
    runtime: Runtime,
}

impl BigQueryClient {
    pub fn open(config: &Config, change_log: &Location) -> Result<Self, Unqueryable> {
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
            warehouse: BigQueryWarehouse::new(config.project(), Namespace::from(change_log)),
            client,
            runtime,
        })
    }

    fn records<R>(&self, query: &SqlString) -> Result<Vec<R>, Unqueryable>
    where
        R: TryFrom<Row, Error = RowError>,
    {
        self.runtime.block_on(async {
            let mut read = self.client.query(query.as_str()).until_done().await?.read();
            let mut records = Vec::new();

            while let Some(row) = read.next().await.transpose()? {
                records.push(R::try_from(row)?);
            }

            Ok(records)
        })
    }
}

impl OpenWarehouse for BigQueryWarehouse {
    type Config = Config;
    type Location = Location;
    type Client = BigQueryClient;
    type Credential = ();
    type Error = Unqueryable;

    fn open(config: &Config, _: &(), change_log: &Location) -> Result<BigQueryClient, Unqueryable> {
        BigQueryClient::open(config, change_log)
    }
}

impl WarehouseClient for BigQueryClient {
    type Warehouse = BigQueryWarehouse;
    type Error = Unqueryable;

    fn warehouse(&self) -> &Self::Warehouse {
        &self.warehouse
    }

    fn execute(&self, statement: &SqlString) -> Result<(), Self::Error> {
        self.runtime.block_on(async {
            self.client.query(statement.as_str()).until_done().await?;

            Ok(())
        })
    }

    fn exists(&self, table: &TablePath) -> Result<bool, Self::Error> {
        let counted: Vec<BigQueryCount> = self.records(&self.warehouse.select_existence(table))?;

        Ok(counted.iter().any(|count| count.0 > 0))
    }

    fn build_logs(
        &self,
        namespace: &Namespace,
        query: &BuildLogQuery,
    ) -> Result<Vec<BuildLog>, Self::Error> {
        self.records::<BigQueryBuildLogRow>(&query.select(&self.warehouse, namespace))?
            .into_iter()
            .map(|row| BuildLog::try_from(row.0).map_err(Unqueryable::Misrecorded))
            .collect()
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
        window: &Window,
    ) -> Result<Self::Changes<'a>, Self::Error> {
        Ok(BigQuerySchemaChanges {
            records: self.records(&self.warehouse.select_schema_changes(log, window))?,
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
    Misrecorded(UnreadableBuildLog),
}

impl Display for Unqueryable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unrunnable(cause) => write!(formatter, "{cause}"),
            Self::Unbuildable(cause) => write!(formatter, "{cause}"),
            Self::Unanswered(cause) => write!(formatter, "{cause}"),
            Self::Unreadable(cause) => write!(formatter, "{cause}"),
            Self::Malformed(cause) => write!(formatter, "{cause}"),
            Self::Misrecorded(cause) => write!(formatter, "{cause}"),
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
