use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Utc};
use ulid::Ulid;

use crate::model::{ColumnType, TableId};
use crate::sink::{Namespace, TablePath, UnsupportedColumn, WarehouseSql};
use crate::sql::SqlString;
use crate::ts::{Event, Ts};
use crate::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    run_id: Ulid,
    started_at: DateTime<Utc>,
    artifact: String,
    schedule: String,
    source: String,
    capture: String,
    version: String,
}

impl Run {
    pub fn new(
        run_id: Ulid,
        started_at: DateTime<Utc>,
        artifact: &str,
        schedule: &str,
        source: &str,
        capture: &str,
        version: &str,
    ) -> Self {
        Self {
            run_id,
            started_at,
            artifact: artifact.to_owned(),
            schedule: schedule.to_owned(),
            source: source.to_owned(),
            capture: capture.to_owned(),
            version: version.to_owned(),
        }
    }

    pub fn advancing(&self, series: &Series) -> Self {
        Self {
            artifact: series.artifact.clone(),
            schedule: series.schedule.clone(),
            ..self.clone()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Series {
    artifact: String,
    schedule: String,
}

impl Series {
    pub fn new(artifact: &str, schedule: &str) -> Self {
        Self {
            artifact: artifact.to_owned(),
            schedule: schedule.to_owned(),
        }
    }

    pub fn artifact(&self) -> &str {
        &self.artifact
    }

    pub fn schedule(&self) -> &str {
        &self.schedule
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
    table_from: Option<TableId>,
    table_to: Option<TableId>,
    base: Option<TablePath>,
    target: Option<TablePath>,
    window: Window,
    outcome: Outcome,
}

impl BuildLog {
    pub const COLUMNS: [(&str, ColumnType); 17] = [
        ("run_id", ColumnType::Text),
        ("started_at", ColumnType::ZonedTimestamp),
        ("artifact", ColumnType::Text),
        ("schedule", ColumnType::Text),
        ("source", ColumnType::Text),
        ("capture", ColumnType::Text),
        ("table_from", ColumnType::Text),
        ("table_to", ColumnType::Text),
        ("lifecycle", ColumnType::Text),
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
        table_from: Option<TableId>,
        table_to: Option<TableId>,
        base: Option<TablePath>,
        target: Option<TablePath>,
        window: Window,
        outcome: Outcome,
    ) -> Self {
        Self {
            run: run.clone(),
            table_from,
            table_to,
            base,
            target,
            window,
            outcome,
        }
    }

    pub fn create_table<W: WarehouseSql + ?Sized>(
        warehouse: &W,
        namespace: &Namespace,
    ) -> Result<SqlString, UnsupportedColumn> {
        let columns = std::iter::once(&(Self::ID, ColumnType::Int64))
            .chain(Self::COLUMNS.iter())
            .map(|(name, column_type)| {
                Ok((
                    warehouse.identifier(name),
                    warehouse.column_type(column_type)?,
                ))
            })
            .collect::<Result<Vec<_>, UnsupportedColumn>>()?;

        Ok(warehouse.create_table(&Self::path(namespace), &columns))
    }

    pub fn insert<W: WarehouseSql + ?Sized>(
        &self,
        warehouse: &W,
        namespace: &Namespace,
    ) -> SqlString {
        let id = warehouse.identifier(Self::ID);
        let path = Self::path(namespace);
        let columns: Vec<_> = std::iter::once(Self::ID)
            .chain(Self::COLUMNS.iter().map(|(name, _)| *name))
            .map(|name| warehouse.identifier(name))
            .collect();
        let select = SqlString::from(format!(
            "SELECT COALESCE(MAX({id}), 0) + 1, {}\nFROM {}",
            self.cells(warehouse).join(", "),
            warehouse.table_ref(&path)
        ));

        warehouse.insert(&path, &columns, &select)
    }

    fn projection<W: WarehouseSql + ?Sized>(warehouse: &W) -> Vec<String> {
        Self::COLUMNS
            .iter()
            .map(|(name, column_type)| {
                let column = warehouse.identifier(name);

                match column_type {
                    ColumnType::ZonedTimestamp => {
                        format!("{} AS {column}", warehouse.unix_millis(&column))
                    }
                    _ => column.to_string(),
                }
            })
            .collect()
    }

    pub fn path(namespace: &Namespace) -> TablePath {
        TablePath::new(namespace.clone(), "_lithify_log")
    }

    pub const ID: &str = "id";

    pub fn run_id(&self) -> Ulid {
        self.run.run_id
    }

    pub fn capture(&self) -> &str {
        &self.run.capture
    }

    pub fn advances(&self, series: &Series) -> bool {
        self.run.artifact == series.artifact && self.run.schedule == series.schedule
    }

    pub fn table_from(&self) -> Option<&TableId> {
        self.table_from.as_ref()
    }

    pub fn table_to(&self) -> Option<&TableId> {
        self.table_to.as_ref()
    }

    pub fn last_success<'a>(&self, succeeded: &'a [BuildLog]) -> Option<&'a BuildLog> {
        [self.table_to.as_ref(), self.table_from.as_ref()]
            .into_iter()
            .flatten()
            .find_map(|table| {
                succeeded
                    .iter()
                    .find(|built| built.table_to() == Some(table))
            })
    }

    pub fn lifecycle(&self) -> Option<Lifecycle> {
        Lifecycle::of(self.table_from.as_ref(), self.table_to.as_ref())
    }

    pub fn table(&self) -> Option<&TableId> {
        self.table_to.as_ref().or(self.table_from.as_ref())
    }

    fn named(&self) -> String {
        match self.table() {
            Some(table) => table.to_string(),
            None => "a table created and dropped within the window".to_owned(),
        }
    }

    pub fn base(&self) -> Option<&TablePath> {
        self.base.as_ref()
    }

    pub fn target(&self) -> Option<&TablePath> {
        self.target.as_ref()
    }

    pub fn window(&self) -> &Window {
        &self.window
    }

    pub fn status(&self) -> &Status {
        &self.outcome.status
    }

    pub fn cells<W: WarehouseSql + ?Sized>(
        &self,
        warehouse: &W,
    ) -> [SqlString; Self::COLUMNS.len()] {
        [
            warehouse.literal(&self.run.run_id.to_string()),
            warehouse.timestamp_millis(self.run.started_at.timestamp_millis()),
            warehouse.literal(&self.run.artifact),
            warehouse.literal(&self.run.schedule),
            warehouse.literal(&self.run.source),
            warehouse.literal(&self.run.capture),
            Self::nullable(warehouse, self.table_from.as_ref()),
            Self::nullable(warehouse, self.table_to.as_ref()),
            Self::nullable(warehouse, self.lifecycle().as_ref()),
            Self::nullable(warehouse, self.base.as_ref()),
            Self::nullable(warehouse, self.target.as_ref()),
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

    pub fn values(&self) -> [Option<String>; Self::COLUMNS.len()] {
        [
            Some(self.run.run_id.to_string()),
            Some(self.run.started_at.to_rfc3339()),
            Some(self.run.artifact.clone()),
            Some(self.run.schedule.clone()),
            Some(self.run.source.clone()),
            Some(self.run.capture.clone()),
            self.table_from.as_ref().map(ToString::to_string),
            self.table_to.as_ref().map(ToString::to_string),
            self.lifecycle().as_ref().map(ToString::to_string),
            self.base.as_ref().map(ToString::to_string),
            self.target.as_ref().map(ToString::to_string),
            Some(self.window.from().to_string()),
            Some(self.window.to().to_string()),
            Some(self.outcome.status.to_string()),
            Some(self.outcome.finished_at.to_rfc3339()),
            self.outcome.status.error().map(str::to_owned),
            Some(self.run.version.clone()),
        ]
    }

    fn nullable<W: WarehouseSql + ?Sized>(
        warehouse: &W,
        value: Option<&impl Display>,
    ) -> SqlString {
        value.map_or_else(
            || SqlString::from("NULL"),
            |value| warehouse.literal(&value.to_string()),
        )
    }
}

impl Display for BuildLog {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let table = self.named();
        let target = self
            .target
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();

        match (&self.outcome.status, self.lifecycle()) {
            (Status::Succeeded, Some(Lifecycle::Dropped)) => write!(formatter, "dropped {table}"),
            (Status::Failed(error), Some(Lifecycle::Dropped)) => {
                write!(formatter, "failed to drop {table}: {error}")
            }
            (Status::Succeeded, _) => write!(formatter, "built {table} into {target}"),
            (Status::Failed(error), _) => {
                write!(formatter, "failed to build {table} into {target}: {error}")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    Created,
    Renamed,
    Dropped,
}

impl Lifecycle {
    pub const DEPARTURES: [Self; 2] = [Self::Renamed, Self::Dropped];

    fn of(from: Option<&TableId>, to: Option<&TableId>) -> Option<Self> {
        match (from, to) {
            (None, _) => Some(Self::Created),
            (Some(_), None) => Some(Self::Dropped),
            (Some(from), Some(to)) if from != to => Some(Self::Renamed),
            (Some(_), Some(_)) => None,
        }
    }
}

impl Display for Lifecycle {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Created => formatter.write_str("created"),
            Self::Renamed => formatter.write_str("renamed"),
            Self::Dropped => formatter.write_str("dropped"),
        }
    }
}

impl TryFrom<&str> for Lifecycle {
    type Error = UnreadableBuildLog;

    fn try_from(written: &str) -> Result<Self, Self::Error> {
        match written {
            "created" => Ok(Self::Created),
            "renamed" => Ok(Self::Renamed),
            "dropped" => Ok(Self::Dropped),
            unknown => Err(UnreadableBuildLog(format!("{unknown} is no lifecycle"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Latest {
    Attempted,
    Succeeded,
    SucceededUntil(Ts<Event>),
}

impl Latest {
    pub const ALIAS: &str = "latest";
    const DEPARTED: &str = "departed";

    pub fn select<W: WarehouseSql + ?Sized>(
        &self,
        warehouse: &W,
        namespace: &Namespace,
    ) -> SqlString {
        let partition = ["artifact", "schedule", "table_to"]
            .map(|name| warehouse.identifier(name))
            .join(", ");
        let log = warehouse.table_ref(&BuildLog::path(namespace));

        SqlString::from(format!(
            "SELECT {}\nFROM {log} AS {}\nWHERE {}\nQUALIFY ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {} DESC) = 1\nORDER BY {partition}",
            BuildLog::projection(warehouse).join(", "),
            warehouse.identifier(Self::ALIAS),
            self.condition(warehouse, &log),
            warehouse.identifier(BuildLog::ID),
        ))
    }

    pub fn condition<W: WarehouseSql + ?Sized>(&self, warehouse: &W, log: &SqlString) -> SqlString {
        let latest = warehouse.identifier(Self::ALIAS);
        let succeeded = Self::succeeded(warehouse, &latest);

        let standing = Self::standing(warehouse, &latest);

        SqlString::from(match self {
            Self::Attempted => {
                format!("{standing} AND {}", Self::undeparted(warehouse, log, false))
            }
            Self::Succeeded => format!(
                "{standing} AND {succeeded} AND {}",
                Self::undeparted(warehouse, log, true)
            ),
            Self::SucceededUntil(to) => format!(
                "{standing} AND {succeeded} AND {} = {}",
                Self::column(warehouse, &latest, "window_to"),
                warehouse.timestamp_millis(to.milliseconds_since_epoch())
            ),
        })
    }

    fn undeparted<W: WarehouseSql + ?Sized>(
        warehouse: &W,
        log: &SqlString,
        succeeded_only: bool,
    ) -> String {
        let [latest, departed] =
            [Self::ALIAS, Self::DEPARTED].map(|alias| warehouse.identifier(alias));
        let of = |alias: &SqlString, column: &str| Self::column(warehouse, alias, column);
        let departures = Lifecycle::DEPARTURES
            .map(|lifecycle| warehouse.literal(&lifecycle.to_string()).to_string())
            .join(", ");
        let mut conditions = vec![
            format!(
                "{} = {}",
                of(&departed, "artifact"),
                of(&latest, "artifact")
            ),
            format!(
                "{} = {}",
                of(&departed, "schedule"),
                of(&latest, "schedule")
            ),
            format!(
                "{} = {}",
                of(&departed, "table_from"),
                of(&latest, "table_to")
            ),
            format!("{} IN ({departures})", of(&departed, "lifecycle")),
            format!(
                "{} >= {}",
                of(&departed, "window_from"),
                of(&latest, "window_to")
            ),
        ];
        if succeeded_only {
            conditions.push(Self::succeeded(warehouse, &departed));
        }

        format!(
            "NOT EXISTS (SELECT 1 FROM {log} AS {departed} WHERE {})",
            conditions.join(" AND ")
        )
    }

    fn standing<W: WarehouseSql + ?Sized>(warehouse: &W, alias: &SqlString) -> String {
        format!("{} IS NOT NULL", Self::column(warehouse, alias, "table_to"))
    }

    fn succeeded<W: WarehouseSql + ?Sized>(warehouse: &W, alias: &SqlString) -> String {
        format!(
            "{} = {}",
            Self::column(warehouse, alias, "status"),
            warehouse.literal(&Status::Succeeded.to_string())
        )
    }

    fn column<W: WarehouseSql + ?Sized>(warehouse: &W, alias: &SqlString, column: &str) -> String {
        format!("{alias}.{}", warehouse.identifier(column))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildLogQuery {
    Latest(Latest),
    // TODO: page through the build log rather than read all of it at once.
    All,
}

impl From<Latest> for BuildLogQuery {
    fn from(latest: Latest) -> Self {
        Self::Latest(latest)
    }
}

impl BuildLogQuery {
    pub fn select<W: WarehouseSql + ?Sized>(
        &self,
        warehouse: &W,
        namespace: &Namespace,
    ) -> SqlString {
        match self {
            Self::Latest(latest) => latest.select(warehouse, namespace),
            Self::All => SqlString::from(format!(
                "SELECT {}\nFROM {}\nORDER BY {} DESC",
                BuildLog::projection(warehouse).join(", "),
                warehouse.table_ref(&BuildLog::path(namespace)),
                warehouse.identifier(BuildLog::ID),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildLogRow {
    pub run_id: String,
    pub started_at: i64,
    pub artifact: String,
    pub schedule: String,
    pub source: String,
    pub capture: String,
    pub table_from: Option<String>,
    pub table_to: Option<String>,
    pub lifecycle: Option<String>,
    pub base: Option<String>,
    pub target: Option<String>,
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
            .rsplit_once('.')
            .map(|(namespace, name)| TablePath::new(namespace, name))
            .ok_or_else(|| UnreadableBuildLog(format!("{written} names no table")))
    }

    fn table(written: Option<&str>) -> Result<Option<TableId>, UnreadableBuildLog> {
        written
            .map(|written| {
                TableId::try_from(written)
                    .map_err(|_| UnreadableBuildLog(format!("{written} names no table")))
            })
            .transpose()
    }

    fn instant(millis: i64) -> Result<DateTime<Utc>, UnreadableBuildLog> {
        DateTime::from_timestamp_millis(millis)
            .ok_or_else(|| UnreadableBuildLog(format!("{millis} is no instant")))
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

        let log = Self {
            run: Run::new(
                run_id,
                BuildLogRow::instant(row.started_at)?,
                &row.artifact,
                &row.schedule,
                &row.source,
                &row.capture,
                &row.version,
            ),
            table_from: BuildLogRow::table(row.table_from.as_deref())?,
            table_to: BuildLogRow::table(row.table_to.as_deref())?,
            base: row.base.as_deref().map(BuildLogRow::path).transpose()?,
            target: row.target.as_deref().map(BuildLogRow::path).transpose()?,
            window: Window::new(
                Ts::from(BuildLogRow::instant(row.window_from)?),
                Ts::from(BuildLogRow::instant(row.window_to)?),
            ),
            outcome: Outcome::new(row.status()?, BuildLogRow::instant(row.finished_at)?),
        };
        let written = row
            .lifecycle
            .as_deref()
            .map(Lifecycle::try_from)
            .transpose()?;

        match written == log.lifecycle() {
            true => Ok(log),
            false => Err(UnreadableBuildLog(format!(
                "{} is logged as {written:?} but its names make it {:?}",
                log.named(),
                log.lifecycle()
            ))),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn logged(from: Option<&str>, to: Option<&str>) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::nil(),
                DateTime::UNIX_EPOCH,
                "snapshot",
                "daily",
                "primary",
                "{}",
                "0.1.0",
            ),
            from.map(|table| TableId::new("shop", table)),
            to.map(|table| TableId::new("shop", table)),
            None,
            to.map(|_| TablePath::new("mysql_shop", "people_20260920")),
            Window::new(Ts::try_from(0).unwrap(), Ts::try_from(1).unwrap()),
            Outcome::new(
                Status::Succeeded,
                DateTime::from_timestamp_millis(1).unwrap(),
            ),
        )
    }

    #[test]
    fn a_build_log_reads_as_what_each_of_its_columns_holds() {
        let values = logged(Some("people"), Some("people")).values();

        assert_eq!(
            BuildLog::COLUMNS
                .iter()
                .map(|(name, _)| *name)
                .zip(values.iter().map(Option::as_deref))
                .collect::<Vec<_>>(),
            [
                ("run_id", Some("00000000000000000000000000")),
                ("started_at", Some("1970-01-01T00:00:00+00:00")),
                ("artifact", Some("snapshot")),
                ("schedule", Some("daily")),
                ("source", Some("primary")),
                ("capture", Some("{}")),
                ("table_from", Some("shop.people")),
                ("table_to", Some("shop.people")),
                ("lifecycle", None),
                ("base", None),
                ("target", Some("mysql_shop.people_20260920")),
                ("window_from", Some("1970-01-01T00:00:00+00:00")),
                ("window_to", Some("1970-01-01T00:00:00.001+00:00")),
                ("status", Some("succeeded")),
                ("finished_at", Some("1970-01-01T00:00:00.001+00:00")),
                ("error", None),
                ("version", Some("0.1.0")),
            ]
        );
    }

    #[test]
    fn a_target_is_read_back_from_the_namespace_and_name_it_was_written_with() {
        assert_eq!(
            BuildLogRow::path("analytics-project.shop.people_20260920").unwrap(),
            TablePath::new("analytics-project.shop", "people_20260920")
        );
    }

    #[test]
    fn a_run_advancing_another_series_is_the_same_run() {
        let run = Run::new(
            Ulid::nil(),
            DateTime::UNIX_EPOCH,
            "snapshot",
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let cut = run.advancing(&Series::new("instant", "09:00"));

        assert_eq!(
            cut,
            Run::new(
                Ulid::nil(),
                DateTime::UNIX_EPOCH,
                "instant",
                "09:00",
                "primary",
                "{}",
                "0.1.0",
            )
        );
    }

    #[test]
    fn a_build_log_advances_the_series_of_the_artifact_and_schedule_it_was_run_for() {
        let logged = logged(Some("people"), Some("people"));

        assert!(logged.advances(&Series::new("snapshot", "daily")));
        assert!(!logged.advances(&Series::new("instant", "daily")));
        assert!(!logged.advances(&Series::new("snapshot", "09:00")));
    }

    #[test]
    fn a_table_the_window_did_not_open_with_was_created_within_it() {
        assert_eq!(
            logged(None, Some("people")).lifecycle(),
            Some(Lifecycle::Created)
        );
        assert_eq!(logged(None, None).lifecycle(), Some(Lifecycle::Created));
    }

    #[test]
    fn a_table_the_window_closed_under_another_name_was_renamed_within_it() {
        assert_eq!(
            logged(Some("people"), Some("persons")).lifecycle(),
            Some(Lifecycle::Renamed)
        );
    }

    #[test]
    fn a_table_the_window_did_not_close_with_was_dropped_within_it() {
        assert_eq!(
            logged(Some("people"), None).lifecycle(),
            Some(Lifecycle::Dropped)
        );
    }

    #[test]
    fn a_table_the_window_opened_and_closed_with_under_one_name_went_through_it_as_it_was() {
        assert_eq!(logged(Some("people"), Some("people")).lifecycle(), None);
    }

    fn row(
        table_from: Option<&str>,
        table_to: Option<&str>,
        lifecycle: Option<&str>,
    ) -> BuildLogRow {
        BuildLogRow {
            run_id: Ulid::nil().to_string(),
            started_at: 0,
            artifact: "snapshot".to_owned(),
            schedule: "daily".to_owned(),
            source: "primary".to_owned(),
            capture: "{}".to_owned(),
            table_from: table_from.map(str::to_owned),
            table_to: table_to.map(str::to_owned),
            lifecycle: lifecycle.map(str::to_owned),
            base: None,
            target: Some("mysql_shop.people_20260920".to_owned()),
            window_from: 0,
            window_to: 1,
            status: "succeeded".to_owned(),
            finished_at: 1,
            error: None,
            version: "0.1.0".to_owned(),
        }
    }

    #[test]
    fn a_row_is_read_with_the_lifecycle_its_names_make() {
        assert_eq!(
            BuildLog::try_from(row(
                Some("shop.people"),
                Some("shop.persons"),
                Some("renamed")
            ))
            .map(|log| log.lifecycle()),
            Ok(Some(Lifecycle::Renamed))
        );
    }

    #[test]
    fn a_row_whose_lifecycle_disagrees_with_its_names_is_unreadable() {
        assert!(
            BuildLog::try_from(row(
                Some("shop.people"),
                Some("shop.people"),
                Some("renamed")
            ))
            .is_err()
        );
    }

    #[test]
    fn a_drop_reads_as_the_table_it_dropped() {
        assert_eq!(logged(Some("pets"), None).to_string(), "dropped shop.pets");
    }

    #[test]
    fn a_drop_that_failed_reads_as_the_table_it_failed_to_drop() {
        let mut failed = logged(Some("pets"), None);
        failed.outcome = Outcome::new(
            Status::Failed("no such table".to_owned()),
            DateTime::from_timestamp_millis(1).unwrap(),
        );

        assert_eq!(
            failed.to_string(),
            "failed to drop shop.pets: no such table"
        );
    }

    #[test]
    fn a_drop_is_read_without_a_target() {
        let mut dropped = row(Some("shop.pets"), None, Some("dropped"));
        dropped.target = None;

        assert_eq!(
            BuildLog::try_from(dropped).map(|log| log.target().cloned()),
            Ok(None)
        );
    }

    #[test]
    fn a_lifecycle_is_written_as_the_build_log_reads_it() {
        for lifecycle in [Lifecycle::Created, Lifecycle::Renamed, Lifecycle::Dropped] {
            assert_eq!(
                Lifecycle::try_from(lifecycle.to_string().as_str()),
                Ok(lifecycle)
            );
        }
    }
}
