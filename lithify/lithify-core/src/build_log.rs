use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Utc};
use ulid::Ulid;

use crate::model::{ColumnType, TableId};
use crate::position::RecordedAt;
use crate::sink::{TablePath, Warehouse};
use crate::sql::SqlString;
use crate::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    run_id: Ulid,
    artifact: String,
    schedule: String,
    source: String,
    capture: String,
    version: String,
}

impl Run {
    pub fn new(
        run_id: Ulid,
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
    pub fn failed(&self) -> bool {
        self.error().is_some()
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Succeeded => None,
            Self::Failed(error) => Some(error),
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
        ("run_id", ColumnType::Text),
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

    pub const ID: &str = "id";

    pub fn run_id(&self) -> Ulid {
        self.run.run_id
    }

    pub fn capture(&self) -> &str {
        &self.run.capture
    }

    pub fn origin(&self) -> &TableId {
        &self.origin
    }

    pub fn base(&self) -> Option<&TablePath> {
        self.base.as_ref()
    }

    pub fn target(&self) -> &TablePath {
        &self.target
    }

    pub fn window(&self) -> &Window {
        &self.window
    }

    pub fn status(&self) -> &Status {
        &self.outcome.status
    }

    pub fn namespace(&self) -> &str {
        self.target.namespace()
    }

    pub fn cells<W: Warehouse + ?Sized>(&self, warehouse: &W) -> [SqlString; 14] {
        [
            warehouse.literal(&self.run.run_id.to_string()),
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
            self.outcome
                .status
                .error()
                .map_or_else(|| SqlString::from("NULL"), |error| warehouse.literal(error)),
            warehouse.literal(&self.run.version),
        ]
    }
}

impl Display for BuildLog {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match &self.outcome.status {
            Status::Succeeded => write!(formatter, "built {} into {}", self.origin, self.target),
            Status::Failed(error) => write!(
                formatter,
                "failed to build {} into {}: {error}",
                self.origin, self.target
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Latest {
    Attempted,
    Succeeded,
    SucceededUntil(RecordedAt),
}

impl Latest {
    pub fn condition<W: Warehouse + ?Sized>(&self, warehouse: &W) -> SqlString {
        let succeeded = format!(
            "{} = {}",
            warehouse.identifier("status"),
            warehouse.literal(&Status::Succeeded.to_string())
        );

        match self {
            Self::Attempted => SqlString::from("TRUE"),
            Self::Succeeded => SqlString::from(succeeded),
            Self::SucceededUntil(to) => SqlString::from(format!(
                "{succeeded} AND {} = {}",
                warehouse.identifier("window_to"),
                warehouse.timestamp_millis(to.milliseconds_since_epoch())
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildLogRow {
    pub run_id: String,
    pub artifact: String,
    pub schedule: String,
    pub source: String,
    pub capture: String,
    pub table_id: String,
    pub base: Option<String>,
    pub target: String,
    pub window_from: i64,
    pub window_to: i64,
    pub status: String,
    pub finished_at: i64,
    pub error: Option<String>,
    pub version: String,
}

impl BuildLogRow {
    fn path(written: &str) -> Result<TablePath, UnreadableBuildLog> {
        written
            .split_once('.')
            .map(|(namespace, name)| TablePath::new(namespace, name))
            .ok_or_else(|| UnreadableBuildLog(format!("{written} names no table")))
    }

    fn status(&self) -> Result<Status, UnreadableBuildLog> {
        match self.status.as_str() {
            "succeeded" => Ok(Status::Succeeded),
            "failed" => Ok(Status::Failed(self.error.clone().unwrap_or_default())),
            unknown => Err(UnreadableBuildLog(format!("{unknown} is no status"))),
        }
    }
}

impl TryFrom<BuildLogRow> for BuildLog {
    type Error = UnreadableBuildLog;

    fn try_from(row: BuildLogRow) -> Result<Self, Self::Error> {
        let run_id = Ulid::from_string(&row.run_id)
            .map_err(|_| UnreadableBuildLog(format!("{} is no run", row.run_id)))?;
        let finished_at = DateTime::from_timestamp_millis(row.finished_at)
            .ok_or_else(|| UnreadableBuildLog(format!("{} is no instant", row.finished_at)))?;

        Ok(Self {
            run: Run::new(
                run_id,
                &row.artifact,
                &row.schedule,
                &row.source,
                &row.capture,
                &row.version,
            ),
            origin: TableId::try_from(row.table_id.as_str())
                .map_err(|_| UnreadableBuildLog(format!("{} names no table", row.table_id)))?,
            base: row.base.as_deref().map(BuildLogRow::path).transpose()?,
            target: BuildLogRow::path(&row.target)?,
            window: Window::new(
                RecordedAt::from(row.window_from),
                RecordedAt::from(row.window_to),
            ),
            outcome: Outcome::new(row.status()?, finished_at),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreadableBuildLog(String);

impl Display for UnreadableBuildLog {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "the build log holds a row it cannot read: {}",
            self.0
        )
    }
}

impl std::error::Error for UnreadableBuildLog {}
