use std::fmt::{self, Display, Formatter};

use chrono::DateTime;
use duckdb::Connection;
use duckdb::types::{FromSql, TimeUnit, Value};

use event_replay_cli::pipeline::{self, Capture};
use event_replay_core::artifact::{Artifact, UnbuildableArtifact};
use event_replay_core::listener::{CatalogModel, OutOfOrder, SchemaChangeListener};
use event_replay_core::model::{TableId, TableModel};
use event_replay_core::position::RecordedAt;
use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::source::{ColumnPath, RowChangeLog, SchemaChangeLog, SchemaChangeStream};
use event_replay_core::window::Window;
use event_replay_protocol::DuckDbWarehouse;
use event_replay_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbLogFn};
use event_replay_protocol::sink::duckdb::store::ObjectStore;
use event_replay_strategy::snapshot::{Base, Snapshot};

use crate::row::{Cell, Row, Rows};

pub trait Sink {
    fn execute(&mut self, statement: &str) -> Result<(), Unbuildable>;

    fn landed(
        &mut self,
        changes: &TablePath,
        capture: &Capture,
        at: RecordedAt,
    ) -> Result<u64, Unbuildable>;

    fn model(
        &mut self,
        capture: &Capture,
        statements: &TablePath,
        table: &TableId,
        window: &Window,
    ) -> Result<TableModel, Unbuildable>;

    fn snapshot(
        &mut self,
        capture: &Capture,
        model: &TableModel,
        changes: &TablePath,
        into: &TablePath,
        window: &Window,
    ) -> Result<(), Unbuildable>;

    fn rows(&mut self, table: &TablePath, model: &TableModel) -> Result<Rows, Unbuildable>;
}

impl dyn Sink {
    pub fn connect(
        warehouse: &pipeline::Warehouse,
        key_id: &str,
        secret: &str,
    ) -> Result<Box<Self>, Unbuildable> {
        match warehouse {
            pipeline::Warehouse::DuckDb(store) => Ok(Box::new(DuckDb::connect(
                ObjectStore::open(store, key_id, secret),
            )?)),
            pipeline::Warehouse::BigQuery(_) => Err(Unbuildable::Unverified("bigquery")),
        }
    }
}

pub struct DuckDb {
    connection: Connection,
    store: ObjectStore,
}

impl DuckDb {
    pub fn connect(store: ObjectStore) -> Result<Self, Unbuildable> {
        Ok(Self {
            connection: Connection::open_in_memory()?,
            store,
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

    fn attach(&self, changes: &TablePath) -> Result<(), Unbuildable> {
        Ok(self.store.attach(&self.connection, changes)?)
    }

    fn select(table: &TablePath, model: &TableModel) -> String {
        let columns: Vec<String> = model
            .columns()
            .map(|column| DuckDbWarehouse.identifier(column.name()))
            .collect();

        format!(
            "SELECT {} FROM {}",
            columns.join(", "),
            DuckDbWarehouse.table_ref(table)
        )
    }
}

impl Sink for DuckDb {
    fn execute(&mut self, statement: &str) -> Result<(), Unbuildable> {
        Ok(self.connection.execute_batch(statement)?)
    }

    fn landed(
        &mut self,
        changes: &TablePath,
        capture: &Capture,
        at: RecordedAt,
    ) -> Result<u64, Unbuildable> {
        let recorded_at = capture.with_duckdb_log(ChangeTime);

        if self.attach(changes).is_err() {
            return Ok(0);
        }

        let landed = self.connection.query_row(
            &format!(
                "SELECT COUNT(*) FROM {} WHERE {} >= {}",
                DuckDbWarehouse.table_ref(changes),
                DuckDbWarehouse.path(&recorded_at),
                at.milliseconds_since_epoch()
            ),
            [],
            |row| row.get::<_, u64>(0),
        );

        Ok(landed.unwrap_or_default())
    }

    fn model(
        &mut self,
        capture: &Capture,
        statements: &TablePath,
        table: &TableId,
        window: &Window,
    ) -> Result<TableModel, Unbuildable> {
        let read = capture.with_duckdb_log(Catalog {
            store: &self.store,
            statements,
            table,
            window,
        })?;

        read.ok_or_else(|| Unbuildable::UnknownTable(table.clone()))
    }

    fn snapshot(
        &mut self,
        capture: &Capture,
        model: &TableModel,
        changes: &TablePath,
        into: &TablePath,
        window: &Window,
    ) -> Result<(), Unbuildable> {
        self.attach(changes)?;

        let statements = capture.with_duckdb_log(SnapshotStatements {
            model,
            changes,
            into,
            window,
        })?;

        self.connection.execute_batch(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            DuckDbWarehouse.identifier(into.namespace())
        ))?;

        for statement in &statements {
            self.connection.execute_batch(statement)?;
        }

        Ok(())
    }

    fn rows(&mut self, table: &TablePath, model: &TableModel) -> Result<Rows, Unbuildable> {
        let mut statement = self.connection.prepare(&Self::select(table, model))?;
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

struct ChangeTime;

impl DuckDbLogFn for ChangeTime {
    type Output = ColumnPath;

    fn call_duckdb<L>(self, log: L) -> Self::Output
    where
        L: SchemaChangeLog + RowChangeLog,
        L::Record: FromSql,
    {
        RowChangeLog::recorded_at(&log)
    }
}

struct Catalog<'a> {
    store: &'a ObjectStore,
    statements: &'a TablePath,
    table: &'a TableId,
    window: &'a Window,
}

impl DuckDbLogFn for Catalog<'_> {
    type Output = Result<Option<TableModel>, Unbuildable>;

    fn call_duckdb<L>(self, log: L) -> Self::Output
    where
        L: SchemaChangeLog + RowChangeLog,
        L::Record: FromSql,
    {
        let mut catalog = CatalogModel::default();

        self.store.client(self.statements)?.catalog::<Unbuildable>(
            &log,
            self.statements,
            self.window,
            &mut catalog,
            &mut Bystander,
        )?;

        Ok(catalog.table(self.table).cloned())
    }
}

struct SnapshotStatements<'a> {
    model: &'a TableModel,
    changes: &'a TablePath,
    into: &'a TablePath,
    window: &'a Window,
}

impl DuckDbLogFn for SnapshotStatements<'_> {
    type Output = Result<Vec<String>, Unbuildable>;

    fn call_duckdb<L>(self, log: L) -> Self::Output
    where
        L: SchemaChangeLog + RowChangeLog,
        L::Record: FromSql,
    {
        Ok(Snapshot::new(
            &log,
            self.model.clone(),
            self.changes.clone(),
            self.into.clone(),
            self.window.clone(),
            Base::Captured,
        )
        .statements(&DuckDbWarehouse)?)
    }
}

struct Bystander;

impl<P> SchemaChangeListener<P> for Bystander {}

#[derive(Debug)]
pub enum Unbuildable {
    Unqueryable(duckdb::Error),
    Unwritable(UnbuildableArtifact),
    OutOfOrder(OutOfOrder),
    UnknownTable(TableId),
    Unverified(&'static str),
}

impl Display for Unbuildable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unqueryable(cause) => write!(formatter, "{cause}"),
            Self::Unwritable(cause) => write!(formatter, "{cause}"),
            Self::OutOfOrder(cause) => write!(formatter, "{cause}"),
            Self::UnknownTable(table) => write!(formatter, "{table} is not in the change log"),
            Self::Unverified(kind) => write!(formatter, "{kind} sinks cannot be verified yet"),
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
