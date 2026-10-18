use chrono::NaiveDate;

use serde::Deserialize;

use lithify_core::build_log::{BuildLog, BuildLogQuery, Latest, Status};
use lithify_core::check::Inspection;
use lithify_core::model::TableId;
use lithify_core::schedule::Schedule;

use crate::command::BuildCommand;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineLocation {
    name: String,
    directory: String,
}

impl PipelineLocation {
    pub fn new(name: &str, directory: &str) -> Self {
        Self {
            name: name.to_owned(),
            directory: directory.to_owned(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn directory(&self) -> &str {
        &self.directory
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineOverview {
    location: PipelineLocation,
    sources: Result<Vec<SourceOverview>, String>,
}

impl PipelineOverview {
    pub fn new(location: PipelineLocation, sources: Result<Vec<SourceOverview>, String>) -> Self {
        Self { location, sources }
    }

    pub fn location(&self) -> &PipelineLocation {
        &self.location
    }

    pub fn sources(&self) -> Result<&[SourceOverview], &str> {
        self.sources.as_deref().map_err(String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceOverview {
    name: String,
    sinks: Vec<SinkOverview>,
}

impl SourceOverview {
    pub fn new(name: &str, sinks: Vec<SinkOverview>) -> Self {
        Self {
            name: name.to_owned(),
            sinks,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sinks(&self) -> &[SinkOverview] {
        &self.sinks
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkOverview {
    name: String,
    found: Result<SinkInspection, String>,
}

impl SinkOverview {
    pub fn new(name: &str, found: Result<SinkInspection, String>) -> Self {
        Self {
            name: name.to_owned(),
            found,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn found(&self) -> Result<&SinkInspection, &str> {
        self.found.as_ref().map_err(String::as_str)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BuildLogView {
    #[default]
    Latest,
    All,
}

impl BuildLogView {
    pub const ALL: [Self; 2] = [Self::Latest, Self::All];

    pub fn name(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::All => "all",
        }
    }
}

impl From<BuildLogView> for BuildLogQuery {
    fn from(view: BuildLogView) -> Self {
        match view {
            BuildLogView::Latest => Latest::Attempted.into(),
            BuildLogView::All => Self::All,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkInspection {
    schedule: Schedule,
    logs: Vec<BuildLog>,
    discrepancies: Vec<String>,
}

impl SinkInspection {
    pub fn new(schedule: Schedule, logs: Vec<BuildLog>, discrepancies: Vec<String>) -> Self {
        Self {
            schedule,
            logs,
            discrepancies,
        }
    }

    pub fn rows(&self) -> Vec<TableRow> {
        self.logs
            .iter()
            .filter_map(|log| TableRow::new(log, self.schedule))
            .collect()
    }

    pub fn showing(self, logs: Vec<BuildLog>) -> Self {
        Self { logs, ..self }
    }

    pub fn discrepancies(&self) -> &[String] {
        &self.discrepancies
    }
}

impl From<Inspection> for SinkInspection {
    fn from(inspection: Inspection) -> Self {
        let mut latest = inspection.latest().to_vec();
        latest.sort_by(|left, right| left.table().cmp(&right.table()));

        Self::new(
            inspection.schedule(),
            latest,
            inspection
                .discrepancies()
                .iter()
                .map(ToString::to_string)
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub table: TableId,
    pub status: Status,
    pub values: [Option<String>; BuildLog::COLUMNS.len()],
    pub shard: Option<NaiveDate>,
}

impl TableRow {
    fn new(log: &BuildLog, schedule: Schedule) -> Option<Self> {
        Some(Self {
            table: log.table()?.clone(),
            status: log.status().clone(),
            values: log.values(),
            shard: schedule.date(log.window()),
        })
    }

    pub fn value(&self, column: &str) -> Option<&str> {
        BuildLog::COLUMNS
            .iter()
            .position(|(name, _)| *name == column)
            .and_then(|at| self.values[at].as_deref())
    }

    // TODO: run the rebuild from the page rather than only assembling the command to run it with.
    pub fn rebuild(&self, directory: &str, sink: &str) -> Option<BuildCommand> {
        self.shard.map(|shard| {
            BuildCommand::new(directory, shard)
                .sink(sink)
                .table(self.table.clone())
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use chrono::{DateTime, Utc};
    use ulid::Ulid;

    use lithify_core::build_log::{BuildLog, Outcome, Run, Status};
    use lithify_core::model::TableId;
    use lithify_core::position::RecordedAt;
    use lithify_core::sink::TablePath;
    use lithify_core::window::Window;

    use lithify_core::schedule::Schedule;

    use super::*;
    use crate::command::BuildCommand;

    fn at(rfc3339: &str) -> DateTime<Utc> {
        rfc3339.parse().unwrap()
    }

    fn logged(table: &str, base: Option<&str>, status: Status) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::from_parts(1, 0),
                DateTime::UNIX_EPOCH,
                "snapshot",
                "daily",
                "primary",
                "{}",
                "0.1.0",
            ),
            Some(TableId::new("shop", table)),
            Some(TableId::new("shop", table)),
            base.map(|base| TablePath::new("mysql_shop", base)),
            Some(TablePath::new("mysql_shop", &format!("{table}_20260920"))),
            Window::new(
                RecordedAt::from(at("2026-09-20T00:00:00Z")),
                RecordedAt::from(at("2026-09-21T00:00:00Z")),
            ),
            Outcome::new(status, at("2026-09-21T00:10:00Z")),
        )
    }

    pub(crate) fn inspection() -> SinkInspection {
        SinkInspection::new(
            Schedule::Daily,
            vec![
                logged("people", Some("people_20260919"), Status::Succeeded),
                logged("pets", None, Status::Failed("no such column".to_owned())),
            ],
            vec!["shop.pets failed to build".to_owned()],
        )
    }

    pub(crate) fn overview() -> PipelineOverview {
        PipelineOverview::new(
            PipelineLocation::new("mysql", "mysql"),
            Ok(vec![SourceOverview::new(
                "primary",
                vec![
                    SinkOverview::new("rustfs", Ok(inspection())),
                    SinkOverview::new("warehouse", Err("JobCreationMode".to_owned())),
                ],
            )]),
        )
    }

    #[test]
    fn a_pipeline_holds_each_source_and_each_sink_it_is_sunk_into_whether_or_not_it_was_read() {
        let overview = overview();
        let sinks = overview.sources().unwrap()[0].sinks();

        assert_eq!(sinks[0].found(), Ok(&inspection()));
        assert_eq!(sinks[1].found(), Err("JobCreationMode"));
    }

    #[test]
    fn a_sink_lists_its_builds_in_the_order_they_were_read() {
        let rows = inspection().rows();

        assert_eq!(
            rows.iter().map(|row| row.table.clone()).collect::<Vec<_>>(),
            [TableId::new("shop", "people"), TableId::new("shop", "pets")]
        );
        assert_eq!(rows[1].status, Status::Failed("no such column".to_owned()));
        assert_eq!(rows[0].shard, Some("2026-09-20".parse().unwrap()));
    }

    #[test]
    fn a_row_holds_what_each_column_of_the_build_log_holds() {
        let rows = inspection().rows();
        assert_eq!(
            rows[0].value("window_from"),
            Some("2026-09-20T00:00:00+00:00")
        );
        assert_eq!(
            rows[0].value("window_to"),
            Some("2026-09-21T00:00:00+00:00")
        );
        assert_eq!(rows[1].value("base"), None);
        assert_eq!(rows[1].value("error"), Some("no such column"));
    }

    #[test]
    fn a_table_renamed_within_its_window_is_listed_under_its_new_name_as_renamed() {
        let renamed = BuildLog::new(
            &Run::new(
                Ulid::from_parts(1, 0),
                DateTime::UNIX_EPOCH,
                "snapshot",
                "daily",
                "primary",
                "{}",
                "0.1.0",
            ),
            Some(TableId::new("shop", "people")),
            Some(TableId::new("shop", "persons")),
            None,
            Some(TablePath::new("mysql_shop", "persons_20260920")),
            Window::new(
                RecordedAt::from(at("2026-09-20T00:00:00Z")),
                RecordedAt::from(at("2026-09-21T00:00:00Z")),
            ),
            Outcome::new(Status::Succeeded, at("2026-09-21T00:10:00Z")),
        );
        let rows = SinkInspection::new(Schedule::Daily, vec![renamed], vec![]).rows();

        assert_eq!(rows[0].table, TableId::new("shop", "persons"));
        assert_eq!(rows[0].value("lifecycle"), Some("renamed"));
    }

    #[test]
    fn a_sink_lists_what_check_finds_wrong_with_it() {
        assert_eq!(inspection().discrepancies(), ["shop.pets failed to build"]);
    }

    #[test]
    fn a_table_is_rebuilt_by_building_its_shard_narrowed_to_it() {
        let rows = inspection().rows();

        assert_eq!(
            rows[1].rebuild("mysql", "rustfs"),
            Some(
                BuildCommand::new("mysql", "2026-09-20".parse().unwrap())
                    .sink("rustfs")
                    .table(TableId::new("shop", "pets"))
            )
        );
    }
}
