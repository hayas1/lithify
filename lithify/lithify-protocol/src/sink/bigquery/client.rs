use std::fmt::{self, Display, Formatter};

use google_cloud_auth::credentials::anonymous;
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::error::{QueryError, RowError};
use google_cloud_bigquery::query::Row;
use google_cloud_gax::client_builder;

use lithify_core::build_log::{BuildLogQuery, BuildLogRow, UnreadableBuildLog};
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
use crate::sink::execution::{Apply, DryRun};

pub trait BigQueryChangeLogsFn {
    type Output;

    async fn call_bigquery<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: TryFrom<Row, Error = RowError>;
}

pub trait BigQueryCapture {
    async fn with_bigquery_change_logs<F: BigQueryChangeLogsFn>(
        &self,
        topic_prefix: &str,
        f: F,
    ) -> F::Output;
}

pub trait BigQueryExecution {
    async fn execute(&self, client: &BigQuery, statement: &SqlString) -> Result<(), Unqueryable>;
}

impl BigQueryExecution for Apply {
    async fn execute(&self, client: &BigQuery, statement: &SqlString) -> Result<(), Unqueryable> {
        client.query(statement.as_str()).until_done().await?;

        Ok(())
    }
}

// TODO: send the statement to BigQuery as a dry run, so it is checked and its scan estimated
// rather than only recorded.
impl BigQueryExecution for DryRun {
    async fn execute(&self, _: &BigQuery, statement: &SqlString) -> Result<(), Unqueryable> {
        self.record(statement);

        Ok(())
    }
}

pub struct BigQueryClient<R = Apply> {
    warehouse: BigQueryWarehouse,
    client: BigQuery,
    execution: R,
}

impl BigQueryClient {
    pub async fn open(config: &Config, change_log: &Location) -> Result<Self, Unqueryable> {
        let builder = BigQuery::builder()
            .with_project_id(config.project())
            .with_endpoint(config.endpoint());
        let builder = match config.credentials() {
            Credentials::Anonymous => builder.with_credentials(anonymous::Builder::new().build()),
            Credentials::Adc => builder,
        };
        let client = builder.build().await.map_err(Unqueryable::Unbuildable)?;

        Ok(Self {
            warehouse: BigQueryWarehouse::new(
                config.project(),
                Namespace::from(change_log.clone()),
            ),
            client,
            execution: Apply,
        })
    }
}

impl<R> BigQueryClient<R> {
    pub fn execution(&self) -> &R {
        &self.execution
    }

    pub fn executing<S>(self, execution: S) -> BigQueryClient<S> {
        BigQueryClient {
            warehouse: self.warehouse,
            client: self.client,
            execution,
        }
    }

    async fn records<T>(&self, query: &SqlString) -> Result<Vec<T>, Unqueryable>
    where
        T: TryFrom<Row, Error = RowError>,
    {
        let mut read = self.client.query(query.as_str()).until_done().await?.read();
        let mut records = Vec::new();

        while let Some(row) = read.next().await.transpose()? {
            records.push(T::try_from(row)?);
        }

        Ok(records)
    }
}

impl OpenWarehouse for BigQueryWarehouse {
    type Config = Config;
    type Location = Location;
    type Client = BigQueryClient;
    type Credential = ();
    type Error = Unqueryable;

    async fn open(
        config: &Config,
        _: &(),
        change_log: &Location,
    ) -> Result<BigQueryClient, Unqueryable> {
        BigQueryClient::open(config, change_log).await
    }
}

impl<R: BigQueryExecution> WarehouseClient for BigQueryClient<R> {
    type Warehouse = BigQueryWarehouse;
    type Error = Unqueryable;

    fn warehouse(&self) -> &Self::Warehouse {
        &self.warehouse
    }

    async fn execute(&self, statement: &SqlString) -> Result<(), Self::Error> {
        self.execution.execute(&self.client, statement).await
    }

    async fn exists(&self, table: &TablePath) -> Result<bool, Self::Error> {
        let counted: Vec<BigQueryCount> = self
            .records(&self.warehouse.select_existence(table))
            .await?;

        Ok(counted.iter().any(|count| count.0 > 0))
    }

    async fn build_log_rows(
        &self,
        namespace: &Namespace,
        query: &BuildLogQuery,
    ) -> Result<Vec<BuildLogRow>, Self::Error> {
        Ok(self
            .records::<BigQueryBuildLogRow>(&query.select(&self.warehouse, namespace))
            .await?
            .into_iter()
            .map(|row| row.0)
            .collect())
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

impl<L, R> SchemaChangeStream<L> for BigQueryClient<R>
where
    L: SchemaChangeLog,
    L::Record: TryFrom<Row, Error = RowError>,
{
    type Error = Unqueryable;
    type Changes<'a>
        = BigQuerySchemaChanges<'a, L>
    where
        L: 'a,
        R: 'a;

    async fn schema_changes<'a>(
        &'a self,
        log: &'a L,
        window: &Window,
    ) -> Result<Self::Changes<'a>, Self::Error> {
        Ok(BigQuerySchemaChanges {
            records: self
                .records(&self.warehouse.select_schema_changes(log, window))
                .await?,
            log,
        })
    }
}

#[derive(Debug)]
pub enum Unqueryable {
    Unbuildable(client_builder::Error),
    Unanswered(QueryError),
    Unreadable(RowError),
    Malformed(MalformedEvent),
    Misrecorded(UnreadableBuildLog),
}

impl Display for Unqueryable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
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
