use std::fmt::{self, Display, Formatter};

use google_cloud_auth::credentials::anonymous;
use google_cloud_bigquery::client::BigQuery;
use google_cloud_bigquery::error::{QueryError, RowError};
use google_cloud_bigquery::query::FromSql;
use google_cloud_gax::client_builder;
use tokio::runtime::Runtime;

use event_replay_core::event::MalformedEvent;
use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::source::{SchemaChangeStream, SchemaChanges, Streamed};
use event_replay_core::window::Window;

use crate::sink::bigquery::BigQueryWarehouse;
use crate::source::debezium::{Debezium, DebeziumDdl, Dialect};

pub struct BigQueryClient {
    warehouse: BigQueryWarehouse,
    client: BigQuery,
    runtime: Runtime,
}

impl BigQueryClient {
    pub fn new(project: &str, endpoint: &str, anonymously: bool) -> Result<Self, Unqueryable> {
        let runtime = Runtime::new().map_err(Unqueryable::Unrunnable)?;
        let builder = BigQuery::builder()
            .with_project_id(project)
            .with_endpoint(endpoint);
        let builder = match anonymously {
            true => builder.with_credentials(anonymous::Builder::new().build()),
            false => builder,
        };
        let client = runtime
            .block_on(builder.build())
            .map_err(Unqueryable::Unbuildable)?;

        Ok(Self {
            warehouse: BigQueryWarehouse::new(project),
            client,
            runtime,
        })
    }

    fn statements<S: FromSql>(&self, query: &str) -> Result<Vec<DebeziumDdl<S>>, Unqueryable> {
        self.runtime.block_on(async {
            let mut read = self.client.query(query).until_done().await?.read();
            let mut statements = Vec::new();

            while let Some(row) = read.next().await.transpose()? {
                statements.push(DebeziumDdl::try_from(row)?);
            }

            Ok(statements)
        })
    }
}

pub struct BigQuerySchemaChanges<'a, D: Dialect> {
    statements: Vec<DebeziumDdl<D::Source>>,
    log: &'a Debezium<D>,
}

impl<D: Dialect> SchemaChanges<Debezium<D>> for BigQuerySchemaChanges<'_, D> {
    type Error = Unqueryable;

    fn stream(
        &mut self,
    ) -> Result<impl Iterator<Item = Streamed<D::Position, Self::Error>>, Self::Error> {
        let log = self.log;

        Ok(std::mem::take(&mut self.statements)
            .into_iter()
            .map(move |statement| log.schema_change(statement).map_err(Unqueryable::Malformed)))
    }
}

impl<D: Dialect> SchemaChangeStream<Debezium<D>> for BigQueryClient
where
    D::Source: FromSql,
{
    type Error = Unqueryable;
    type Changes<'a>
        = BigQuerySchemaChanges<'a, D>
    where
        D: 'a;

    fn schema_changes<'a>(
        &'a self,
        log: &'a Debezium<D>,
        table: &TablePath,
        window: &Window,
    ) -> Result<Self::Changes<'a>, Self::Error> {
        Ok(BigQuerySchemaChanges {
            statements: self
                .statements(&self.warehouse.select_schema_changes(log, table, window))?,
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
