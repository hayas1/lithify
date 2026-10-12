use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Utc};

use crate::model::{ColumnType, TableId};
use crate::sink::{TablePath, Warehouse};
use crate::sql::SqlString;
use crate::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    run_id: i64,
    artifact: String,
    schedule: String,
    source: String,
    capture: String,
    version: String,
}

impl Run {
    pub fn new(
        run_id: i64,
        artifact: &str,
        schedule: &str,
        source: &str,
        capture: &str,
        version: &str,
    ) -> Self {
        Self {
            run_id,
            artifact: artifact.to_owned(),
            schedule: schedule.to_owned(),
            source: source.to_owned(),
            capture: capture.to_owned(),
            version: version.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Succeeded,
    Failed(String),
}

impl Display for Status {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Succeeded => formatter.write_str("succeeded"),
            Self::Failed(_) => formatter.write_str("failed"),
        }
    }
}

impl Status {
    fn error<W: Warehouse + ?Sized>(&self, warehouse: &W) -> SqlString {
        match self {
            Self::Succeeded => SqlString::from("NULL"),
            Self::Failed(error) => warehouse.literal(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    status: Status,
    finished_at: DateTime<Utc>,
}

impl Outcome {
    pub fn new(status: Status, finished_at: DateTime<Utc>) -> Self {
        Self {
            status,
            finished_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildLog {
    run: Run,
    origin: TableId,
    base: Option<TablePath>,
    target: TablePath,
    window: Window,
    outcome: Outcome,
}

impl BuildLog {
    pub const COLUMNS: [(&str, ColumnType); 14] = [
        ("run_id", ColumnType::Int64),
        ("artifact", ColumnType::Text),
        ("schedule", ColumnType::Text),
        ("source", ColumnType::Text),
        ("capture", ColumnType::Text),
        ("table_id", ColumnType::Text),
        ("base", ColumnType::Text),
        ("target", ColumnType::Text),
        ("window_from", ColumnType::ZonedTimestamp),
        ("window_to", ColumnType::ZonedTimestamp),
        ("status", ColumnType::Text),
        ("finished_at", ColumnType::ZonedTimestamp),
        ("error", ColumnType::Text),
        ("version", ColumnType::Text),
    ];

    pub fn new(
        run: &Run,
        origin: TableId,
        base: Option<TablePath>,
        target: TablePath,
        window: Window,
        outcome: Outcome,
    ) -> Self {
        Self {
            run: run.clone(),
            origin,
            base,
            target,
            window,
            outcome,
        }
    }

    pub fn path(namespace: &str) -> TablePath {
        TablePath::new(namespace, "_lithify_log")
    }

    pub fn namespace(&self) -> &str {
        self.target.namespace()
    }

    pub fn cells<W: Warehouse + ?Sized>(&self, warehouse: &W) -> [SqlString; 14] {
        [
            SqlString::from(self.run.run_id.to_string()),
            warehouse.literal(&self.run.artifact),
            warehouse.literal(&self.run.schedule),
            warehouse.literal(&self.run.source),
            warehouse.literal(&self.run.capture),
            warehouse.literal(&self.origin.to_string()),
            self.base
                .as_ref()
                .map(|base| warehouse.literal(&base.to_string()))
                .unwrap_or_else(|| SqlString::from("NULL")),
            warehouse.literal(&self.target.to_string()),
            warehouse.timestamp_millis(self.window.from().milliseconds_since_epoch()),
            warehouse.timestamp_millis(self.window.to().milliseconds_since_epoch()),
            warehouse.literal(&self.outcome.status.to_string()),
            warehouse.timestamp_millis(self.outcome.finished_at.timestamp_millis()),
            self.outcome.status.error(warehouse),
            warehouse.literal(&self.run.version),
        ]
    }
}
