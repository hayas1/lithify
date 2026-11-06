use std::convert::Infallible;
use std::fmt::{self, Display, Formatter};

use chrono::DateTime;
use duckdb::types::{TimeUnit, Value};

use lithify_artifact::snapshot::Snapshot;
use lithify_cli::credential::Credentials;
use lithify_cli::pipeline::{self, CaptureConfig};
use lithify_cli::unrunnable::Unrunnable;
use lithify_core::artifact::UnbuildableArtifact;
use lithify_core::listener::OutOfOrder;
use lithify_core::model::{TableId, TableModel};
use lithify_core::sink::{Namespace, OpenWarehouse, TablePath, WarehouseClient, WarehouseSql};
use lithify_core::source::{ChangeLog, ChangeLogs, ColumnPath, RowChangeLog, SchemaChangeStream};
use lithify_core::sql::SqlString;
use lithify_core::ts::{Event, Processing, Ts};
use lithify_core::window::Window;
use lithify_pipeline::build_log::Status;
use lithify_pipeline::capture::{Capture, ChangeLogsFn};
use lithify_pipeline::runner::Runner;
use lithify_pipeline::warehouse::WarehouseControl;
use lithify_protocol::sink::bigquery;
use lithify_protocol::sink::bigquery::client::BigQueryClient;
use lithify_protocol::sink::duckdb::client::DuckDbClient;
use lithify_protocol::{BigQueryWarehouse, DuckDbWarehouse};

use crate::row::{Cell, Row, Rows};
use crate::run::Unverifiable;

pub trait Sink: WarehouseControl + Sized {
    async fn landed_since(
        &self,
        topic: &TablePath,
        processing_time: &ColumnPath,
        at: Ts<Processing>,
    ) -> Result<u64, Unbuildable>;

    async fn rows(&self, table: &TablePath, model: &TableModel) -> Result<Rows, Unbuildable>;

    async fn landed<C: Capture<Self>>(
        &self,
        capture: &C,
        table: &TableId,
        at: Ts<Processing>,
    ) -> Result<u64, Unbuildable> {
        let (topic, processing_time) = capture
            .with_change_logs(self.warehouse(), LandedTopic { table })
            .await;

        self.landed_since(&topic, &processing_time, at).await
    }

    async fn model<C: Capture<Self>>(
        &self,
        capture: &C,
        table: &TableId,
        at: Ts<Event>,
    ) -> Result<TableModel, Unbuildable>
    where
        Unbuildable: From<<Self as WarehouseClient>::Error>,
    {
        capture
            .with_change_logs(
                self.warehouse(),
                Catalog {
                    client: self,
                    table,
                    at,
                },
            )
            .await?
            .ok_or_else(|| Unbuildable::UnknownTable(table.clone()))
    }

    async fn snapshot<C: Capture<Self>>(
        &self,
        capture: &C,
        table: &TableId,
        artifact: &Snapshot,
        namespace: &Namespace,
        window: &Window,
    ) -> Result<(), Unbuildable>
    where
        Unbuildable: From<<Self as WarehouseClient>::Error>,
    {
        capture
            .with_change_logs(
                self.warehouse(),
                SnapshotBuild {
                    client: self,
                    table,
                    artifact,
                    namespace,
                    window,
                },
            )
            .await
    }

    fn select(&self, table: &TablePath, model: &TableModel) -> SqlString {
        let warehouse = self.warehouse();
        let columns: Vec<String> = model
            .columns()
            .map(|column| warehouse.column_ref(column.name()).to_string())
            .collect();

        SqlString::from(format!(
            "SELECT {} FROM {}",
            columns.join(", "),
            warehouse.table_ref(table)
        ))
    }
}

impl Sink for DuckDbClient {
    async fn landed_since(
        &self,
        topic: &TablePath,
        processing_time: &ColumnPath,
        at: Ts<Processing>,
    ) -> Result<u64, Unbuildable> {
        let warehouse = self.warehouse();
        let landed = self.connection().query_row(
            &format!(
                "SELECT COUNT(*) FROM {} WHERE {} >= {}",
                warehouse.table_ref(topic),
                warehouse.path(processing_time),
                at.milliseconds_since_epoch()
            ),
            [],
            |row| row.get::<_, u64>(0),
        );

        match landed {
            Ok(landed) => Ok(landed),
            Err(unread)
                if unread
                    .to_string()
                    .contains("No files found that match the pattern") =>
            {
                Ok(0)
            }
            Err(unread) => Err(unread.into()),
        }
    }

    async fn rows(&self, table: &TablePath, model: &TableModel) -> Result<Rows, Unbuildable> {
        let mut statement = self
            .connection()
            .prepare(self.select(table, model).as_str())?;
        let read = statement.query_map([], |row| {
            Ok((0..model.columns().count())
                .map(|at| Cell::from(&row.get::<_, Value>(at).unwrap_or(Value::Null)))
                .collect::<Row>())
        })?;

        read.collect::<Result<Rows, _>>().map_err(Unbuildable::from)
    }
}

impl Sink for BigQueryClient {
    async fn landed_since(
        &self,
        _: &TablePath,
        _: &ColumnPath,
        _: Ts<Processing>,
    ) -> Result<u64, Unbuildable> {
        todo!("count what landed once the bigquery client answers queries with rows")
    }

    async fn rows(&self, _: &TablePath, _: &TableModel) -> Result<Rows, Unbuildable> {
        todo!("read the rows once the bigquery client answers queries with rows")
    }
}

pub enum SinkClient {
    DuckDb(DuckDbClient),
    BigQuery(BigQueryClient),
}

impl SinkClient {
    pub async fn connect(
        warehouse: &pipeline::WarehouseConfig,
        change_log: &pipeline::LocationConfig,
        credentials: &Credentials,
    ) -> Result<Self, Unverifiable> {
        match warehouse.reach(change_log).ok_or(Unrunnable::Unreached)? {
            pipeline::Reached::DuckDb(config, change_log) => Ok(Self::DuckDb(
                DuckDbWarehouse::open(config, credentials.duckdb()?, change_log)
                    .await
                    .map_err(Unbuildable::from)?,
            )),
            pipeline::Reached::BigQuery(config, change_log) => Ok(Self::BigQuery(
                BigQueryWarehouse::open(config, &(), change_log)
                    .await
                    .map_err(Unbuildable::from)?,
            )),
        }
    }

    pub async fn execute(&self, statement: &str) -> Result<(), Unbuildable> {
        let statement = SqlString::from(statement);

        match self {
            Self::DuckDb(client) => Ok(client.execute(&statement).await?),
            Self::BigQuery(client) => Ok(client.execute(&statement).await?),
        }
    }

    pub async fn landed(
        &self,
        capture: &CaptureConfig,
        table: &TableId,
        at: Ts<Processing>,
    ) -> Result<u64, Unbuildable> {
        match self {
            Self::DuckDb(client) => client.landed(capture, table, at).await,
            Self::BigQuery(client) => client.landed(capture, table, at).await,
        }
    }

    pub async fn model(
        &self,
        capture: &CaptureConfig,
        table: &TableId,
        at: Ts<Event>,
    ) -> Result<TableModel, Unbuildable> {
        match self {
            Self::DuckDb(client) => client.model(capture, table, at).await,
            Self::BigQuery(client) => client.model(capture, table, at).await,
        }
    }

    pub async fn snapshot(
        &self,
        capture: &CaptureConfig,
        table: &TableId,
        artifact: &Snapshot,
        namespace: &Namespace,
        window: &Window,
    ) -> Result<(), Unbuildable> {
        match self {
            Self::DuckDb(client) => {
                client
                    .snapshot(capture, table, artifact, namespace, window)
                    .await
            }
            Self::BigQuery(client) => {
                client
                    .snapshot(capture, table, artifact, namespace, window)
                    .await
            }
        }
    }

    pub async fn rows(&self, table: &TablePath, model: &TableModel) -> Result<Rows, Unbuildable> {
        match self {
            Self::DuckDb(client) => client.rows(table, model).await,
            Self::BigQuery(client) => client.rows(table, model).await,
        }
    }
}

impl Cell {
    fn instant(unit: TimeUnit, read: i64) -> Self {
        let micros = match unit {
            TimeUnit::Second => read * 1_000_000,
            TimeUnit::Millisecond => read * 1_000,
            TimeUnit::Microsecond => read,
            TimeUnit::Nanosecond => read / 1_000,
        };

        match DateTime::from_timestamp_micros(micros) {
            Some(at) => Self::Read(at.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
            None => Self::Read(micros.to_string()),
        }
    }

    fn date(days: i32) -> Self {
        match DateTime::from_timestamp(i64::from(days) * 86_400, 0) {
            Some(at) => Self::Read(at.format("%Y-%m-%d").to_string()),
            None => Self::Read(days.to_string()),
        }
    }
}

impl From<&Value> for Cell {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Missing,
            Value::Boolean(read) => Self::Read(u8::from(*read).to_string()),
            Value::TinyInt(read) => Self::Read(read.to_string()),
            Value::SmallInt(read) => Self::Read(read.to_string()),
            Value::Int(read) => Self::Read(read.to_string()),
            Value::BigInt(read) => Self::Read(read.to_string()),
            Value::UTinyInt(read) => Self::Read(read.to_string()),
            Value::USmallInt(read) => Self::Read(read.to_string()),
            Value::UInt(read) => Self::Read(read.to_string()),
            Value::UBigInt(read) => Self::Read(read.to_string()),
            Value::Float(read) => Self::Read(read.to_string()),
            Value::Double(read) => Self::Read(read.to_string()),
            Value::Decimal(read) => Self::Read(read.to_string()),
            Value::Text(read) => Self::Read(read.clone()),
            Value::Timestamp(unit, read) => Self::instant(*unit, *read),
            Value::Date32(days) => Self::date(*days),
            read => Self::Read(format!("{read:?}")),
        }
    }
}

struct LandedTopic<'a> {
    table: &'a TableId,
}

impl<K: WarehouseClient> ChangeLogsFn<K> for LandedTopic<'_> {
    type Output = (TablePath, ColumnPath);

    async fn call<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        K: SchemaChangeStream<C::SchemaChangeLog, Error = <K as WarehouseClient>::Error>,
    {
        (
            logs.row_changes().dml_landed_table(self.table),
            logs.row_changes().processing_time(),
        )
    }
}

struct Catalog<'a, K> {
    client: &'a K,
    table: &'a TableId,
    at: Ts<Event>,
}

impl<K> ChangeLogsFn<K> for Catalog<'_, K>
where
    K: WarehouseClient,
    Unbuildable: From<<K as WarehouseClient>::Error>,
{
    type Output = Result<Option<TableModel>, Unbuildable>;

    async fn call<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        K: SchemaChangeStream<C::SchemaChangeLog, Error = <K as WarehouseClient>::Error>,
    {
        self.client
            .table(logs.schema_changes(), self.table, self.at)
            .await
    }
}

struct SnapshotBuild<'a, K> {
    client: &'a K,
    table: &'a TableId,
    artifact: &'a Snapshot,
    namespace: &'a Namespace,
    window: &'a Window,
}

impl<K> ChangeLogsFn<K> for SnapshotBuild<'_, K>
where
    K: WarehouseControl,
    Unbuildable: From<<K as WarehouseClient>::Error>,
{
    type Output = Result<(), Unbuildable>;

    async fn call<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        K: SchemaChangeStream<C::SchemaChangeLog, Error = <K as WarehouseClient>::Error>,
    {
        let built = Runner::new(self.client, &logs)
            .built::<_, Unbuildable>(self.artifact, self.namespace, self.window, &[], |table| {
                table == self.table
            })
            .await?
            .into_iter()
            .find(|built| built.table() == Some(self.table))
            .ok_or_else(|| Unbuildable::UnknownTable(self.table.clone()))?;

        match built.status() {
            Status::Succeeded => Ok(()),
            Status::Failed(cause) => Err(Unbuildable::Unbuilt(cause.clone())),
        }
    }
}

#[derive(Debug)]
pub enum Unbuildable {
    Unqueryable(duckdb::Error),
    Unanswered(bigquery::client::Unqueryable),
    UnknownTable(TableId),
    Unwritable(UnbuildableArtifact),
    Unbuilt(String),
    OutOfOrder(OutOfOrder),
}

impl Display for Unbuildable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unqueryable(cause) => write!(formatter, "{cause}"),
            Self::Unanswered(cause) => write!(formatter, "{cause}"),
            Self::UnknownTable(table) => write!(formatter, "{table} is not in the change log"),
            Self::Unwritable(cause) => write!(formatter, "{cause}"),
            Self::Unbuilt(cause) => write!(formatter, "{cause}"),
            Self::OutOfOrder(cause) => write!(formatter, "{cause}"),
        }
    }
}

impl std::error::Error for Unbuildable {}

impl From<duckdb::Error> for Unbuildable {
    fn from(cause: duckdb::Error) -> Self {
        Self::Unqueryable(cause)
    }
}

impl From<bigquery::client::Unqueryable> for Unbuildable {
    fn from(cause: bigquery::client::Unqueryable) -> Self {
        Self::Unanswered(cause)
    }
}

impl From<UnbuildableArtifact> for Unbuildable {
    fn from(cause: UnbuildableArtifact) -> Self {
        Self::Unwritable(cause)
    }
}

impl From<Infallible> for Unbuildable {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

impl From<OutOfOrder> for Unbuildable {
    fn from(cause: OutOfOrder) -> Self {
        Self::OutOfOrder(cause)
    }
}
