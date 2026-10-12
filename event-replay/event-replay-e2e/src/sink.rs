use std::fmt::{self, Display, Formatter};

use chrono::DateTime;
use duckdb::types::{FromSql, TimeUnit, Value};

use event_replay_cli::pipeline::{self, Capture};
use event_replay_core::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use event_replay_core::era::EraTracker;
use event_replay_core::listener::OutOfOrder;
use event_replay_core::model::{TableId, TableModel};
use event_replay_core::position::RecordedAt;
use event_replay_core::sink::{TablePath, Warehouse, WarehouseClient};
use event_replay_core::source::{
    ChangeLog, ChangeLogs, ColumnPath, RowChangeLog, SchemaChangeLog, SchemaChangeStream,
};
use event_replay_core::sql::SqlString;
use event_replay_core::window::Window;
use event_replay_protocol::DuckDbWarehouse;
use event_replay_protocol::sink::duckdb::client::{
    DuckDbCapture, DuckDbChangeLogsFn, DuckDbClient,
};
use event_replay_protocol::sink::duckdb::store::ObjectStore;
use event_replay_strategy::snapshot::Snapshot;

use crate::row::{Cell, Row, Rows};

pub trait Sink {
    fn execute(&mut self, statement: &str) -> Result<(), Unbuildable>;

    fn landed(
        &mut self,
        capture: &Capture,
        topic_prefix: &str,
        table: &TableId,
        at: RecordedAt,
    ) -> Result<u64, Unbuildable>;

    fn model(
        &mut self,
        capture: &Capture,
        topic_prefix: &str,
        table: &TableId,
        at: RecordedAt,
    ) -> Result<TableModel, Unbuildable>;

    fn snapshot(
        &mut self,
        capture: &Capture,
        topic_prefix: &str,
        table: &TableId,
        artifact: &Snapshot,
        window: &Window,
    ) -> Result<(), Unbuildable>;

    fn rows(&mut self, table: &TablePath, model: &TableModel) -> Result<Rows, Unbuildable>;
}

impl dyn Sink {
    pub fn connect(
        warehouse: &pipeline::Warehouse,
        change_log: &str,
        key_id: &str,
        secret: &str,
    ) -> Result<Box<Self>, Unbuildable> {
        match warehouse {
            pipeline::Warehouse::DuckDb(store) => Ok(Box::new(DuckDb::connect(
                &ObjectStore::open(store, key_id, secret),
                change_log,
            )?)),
            pipeline::Warehouse::BigQuery(_) => Err(Unbuildable::Unverified("bigquery")),
        }
    }
}

pub struct DuckDb {
    client: DuckDbClient,
}

impl DuckDb {
    pub fn connect(store: &ObjectStore, change_log: &str) -> Result<Self, Unbuildable> {
        Ok(Self {
            client: store.client(change_log)?,
        })
    }

    fn instant(unit: TimeUnit, read: i64) -> Cell {
        let micros = match unit {
            TimeUnit::Second => read * 1_000_000,
            TimeUnit::Millisecond => read * 1_000,
            TimeUnit::Microsecond => read,
            TimeUnit::Nanosecond => read / 1_000,
        };

        match DateTime::from_timestamp_micros(micros) {
            Some(at) => Cell::Read(at.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
            None => Cell::Read(micros.to_string()),
        }
    }

    fn date(days: i32) -> Cell {
        match DateTime::from_timestamp(i64::from(days) * 86_400, 0) {
            Some(at) => Cell::Read(at.format("%Y-%m-%d").to_string()),
            None => Cell::Read(days.to_string()),
        }
    }

    fn unwritten(unread: &duckdb::Error) -> bool {
        unread
            .to_string()
            .contains("No files found that match the pattern")
    }

    fn select(&self, table: &TablePath, model: &TableModel) -> String {
        let warehouse = self.client.warehouse();
        let columns: Vec<String> = model
            .columns()
            .map(|column| warehouse.identifier(column.name()).to_string())
            .collect();

        format!(
            "SELECT {} FROM {}",
            columns.join(", "),
            warehouse.table_ref(table)
        )
    }
}

impl Sink for DuckDb {
    fn execute(&mut self, statement: &str) -> Result<(), Unbuildable> {
        Ok(self.client.execute(&SqlString::from(statement))?)
    }

    fn landed(
        &mut self,
        capture: &Capture,
        topic_prefix: &str,
        table: &TableId,
        at: RecordedAt,
    ) -> Result<u64, Unbuildable> {
        let (topic, recorded_at) = capture.with_duckdb_change_logs(topic_prefix, Landing { table });

        let warehouse = self.client.warehouse();
        let landed = self.client.connection().query_row(
            &format!(
                "SELECT COUNT(*) FROM {} WHERE {} >= {}",
                warehouse.change_log_ref(&topic),
                warehouse.path(&recorded_at),
                at.milliseconds_since_epoch()
            ),
            [],
            |row| row.get::<_, u64>(0),
        );

        match landed {
            Ok(landed) => Ok(landed),
            Err(unread) if Self::unwritten(&unread) => Ok(0),
            Err(unread) => Err(unread.into()),
        }
    }

    fn model(
        &mut self,
        capture: &Capture,
        topic_prefix: &str,
        table: &TableId,
        at: RecordedAt,
    ) -> Result<TableModel, Unbuildable> {
        let read = capture.with_duckdb_change_logs(
            topic_prefix,
            Catalog {
                client: &self.client,
                table,
                at,
            },
        )?;

        read.ok_or_else(|| Unbuildable::UnknownTable(table.clone()))
    }

    fn snapshot(
        &mut self,
        capture: &Capture,
        topic_prefix: &str,
        table: &TableId,
        artifact: &Snapshot,
        window: &Window,
    ) -> Result<(), Unbuildable> {
        capture.with_duckdb_change_logs(
            topic_prefix,
            SnapshotBuild {
                client: &self.client,
                table,
                artifact,
                window,
            },
        )
    }

    fn rows(&mut self, table: &TablePath, model: &TableModel) -> Result<Rows, Unbuildable> {
        let mut statement = self
            .client
            .connection()
            .prepare(&self.select(table, model))?;
        let read = statement.query_map([], |row| {
            Ok((0..model.columns().count())
                .map(|at| Cell::from(&row.get::<_, Value>(at).unwrap_or(Value::Null)))
                .collect::<Row>())
        })?;

        read.collect::<Result<Rows, _>>().map_err(Unbuildable::from)
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
            Value::Timestamp(unit, read) => DuckDb::instant(*unit, *read),
            Value::Date32(days) => DuckDb::date(*days),
            read => Self::Read(format!("{read:?}")),
        }
    }
}

struct Landing<'a> {
    table: &'a TableId,
}

impl DuckDbChangeLogsFn for Landing<'_> {
    type Output = (String, ColumnPath);

    fn call_duckdb<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: FromSql,
    {
        (
            logs.row_changes().dml_topic(self.table),
            logs.row_changes().recorded_at(),
        )
    }
}

struct Catalog<'a> {
    client: &'a DuckDbClient,
    table: &'a TableId,
    at: RecordedAt,
}

impl DuckDbChangeLogsFn for Catalog<'_> {
    type Output = Result<Option<TableModel>, Unbuildable>;

    fn call_duckdb<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: FromSql,
    {
        self.client
            .table(logs.schema_changes(), self.table, self.at)
    }
}

struct SnapshotBuild<'a> {
    client: &'a DuckDbClient,
    table: &'a TableId,
    artifact: &'a Snapshot,
    window: &'a Window,
}

impl DuckDbChangeLogsFn for SnapshotBuild<'_> {
    type Output = Result<(), Unbuildable>;

    fn call_duckdb<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: FromSql,
    {
        let mut tracker = EraTracker::new(self.window.clone(), |table: &TableId| {
            Ok::<_, Unbuildable>(self.artifact.builder::<_, DuckDbWarehouse>(
                logs.row_changes(),
                table.clone(),
                self.window,
                None,
            ))
        });
        let catalog = self.client.replay::<Unbuildable, _>(
            self.client.warehouse(),
            logs.schema_changes(),
            self.window,
            &mut tracker,
        )?;
        let snapshot = tracker
            .finish(self.client.warehouse(), &catalog)?
            .into_iter()
            .find(|snapshot| snapshot.table() == self.table)
            .ok_or_else(|| Unbuildable::UnknownTable(self.table.clone()))?;

        self.client.build(&snapshot)
    }
}

#[derive(Debug)]
pub enum Unbuildable {
    Unqueryable(duckdb::Error),
    UnknownTable(TableId),
    Unverified(&'static str),
    Unwritable(UnbuildableArtifact),
    OutOfOrder(OutOfOrder),
}

impl Display for Unbuildable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unqueryable(cause) => write!(formatter, "{cause}"),
            Self::UnknownTable(table) => write!(formatter, "{table} is not in the change log"),
            Self::Unverified(kind) => write!(formatter, "{kind} sinks cannot be verified yet"),
            Self::Unwritable(cause) => write!(formatter, "{cause}"),
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

impl From<UnbuildableArtifact> for Unbuildable {
    fn from(cause: UnbuildableArtifact) -> Self {
        Self::Unwritable(cause)
    }
}

impl From<OutOfOrder> for Unbuildable {
    fn from(cause: OutOfOrder) -> Self {
        Self::OutOfOrder(cause)
    }
}
