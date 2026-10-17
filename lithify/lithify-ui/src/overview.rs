use chrono::NaiveDate;

use lithify_config::Schedule;
use lithify_core::build_log::{BuildLog, Lifecycle, Status};
use lithify_core::model::TableId;
use lithify_core::sink::TablePath;

use crate::command::BuildCommand;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineOverview {
    directory: String,
    sources: Result<Vec<SourceOverview>, String>,
}

impl PipelineOverview {
    pub fn new(directory: &str, sources: Result<Vec<SourceOverview>, String>) -> Self {
        Self {
            directory: directory.to_owned(),
            sources,
        }
    }

    pub fn directory(&self) -> &str {
        &self.directory
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
    inspection: Result<SinkInspection, String>,
}

impl SinkOverview {
    pub fn new(name: &str, inspection: Result<SinkInspection, String>) -> Self {
        Self {
            name: name.to_owned(),
            inspection,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn inspection(&self) -> Result<&SinkInspection, &str> {
        self.inspection.as_ref().map_err(String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkInspection {
    schedule: Schedule,
    latest: Vec<BuildLog>,
    discrepancies: Vec<String>,
}

impl SinkInspection {
    pub fn new(schedule: Schedule, latest: Vec<BuildLog>, discrepancies: Vec<String>) -> Self {
        Self {
            schedule,
            latest,
            discrepancies,
        }
    }

    pub fn rows(&self) -> Vec<TableRow> {
        let mut rows: Vec<_> = self
            .latest
            .iter()
            .filter_map(|log| TableRow::new(log, self.schedule))
            .collect();
        rows.sort_by(|left, right| left.table.cmp(&right.table));

        rows
    }

    pub fn discrepancies(&self) -> &[String] {
        &self.discrepancies
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub table: TableId,
    pub lifecycle: Option<Lifecycle>,
    pub status: Status,
    pub window: String,
    pub base: Option<TablePath>,
    pub target: Option<TablePath>,
    pub shard: Option<NaiveDate>,
}

impl TableRow {
    fn new(log: &BuildLog, schedule: Schedule) -> Option<Self> {
        let window = log.window();

        Some(Self {
            table: log.table()?.clone(),
            lifecycle: log.lifecycle(),
            status: log.status().clone(),
            window: format!("{} .. {}", window.from(), window.to()),
            base: log.base().cloned(),
            target: log.target().cloned(),
            shard: schedule.date(window),
        })
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

    use lithify_core::build_log::{BuildLog, Lifecycle, Outcome, Run, Status};
    use lithify_core::model::TableId;
    use lithify_core::position::RecordedAt;
    use lithify_core::sink::TablePath;
    use lithify_core::window::Window;

    use lithify_config::Schedule;

    use super::*;
    use crate::command::BuildCommand;

    fn at(rfc3339: &str) -> DateTime<Utc> {
        rfc3339.parse().unwrap()
    }

    fn logged(table: &str, base: Option<&str>, status: Status) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::from_parts(1, 0),
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
                logged("pets", None, Status::Failed("no such column".to_owned())),
                logged("people", Some("people_20260919"), Status::Succeeded),
            ],
            vec!["shop.pets failed to build".to_owned()],
        )
    }

    pub(crate) fn overview() -> PipelineOverview {
        PipelineOverview::new(
            "mysql",
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

        assert_eq!(sinks[0].inspection(), Ok(&inspection()));
        assert_eq!(sinks[1].inspection(), Err("JobCreationMode"));
    }

    #[test]
    fn a_sink_lists_the_latest_build_of_each_table_in_the_order_of_their_names() {
        assert_eq!(
            inspection().rows(),
            [
                TableRow {
                    table: TableId::new("shop", "people"),
                    lifecycle: None,
                    status: Status::Succeeded,
                    window: "2026-09-20T00:00:00+00:00 .. 2026-09-21T00:00:00+00:00".to_owned(),
                    base: Some(TablePath::new("mysql_shop", "people_20260919")),
                    target: Some(TablePath::new("mysql_shop", "people_20260920")),
                    shard: Some("2026-09-20".parse().unwrap()),
                },
                TableRow {
                    table: TableId::new("shop", "pets"),
                    lifecycle: None,
                    status: Status::Failed("no such column".to_owned()),
                    window: "2026-09-20T00:00:00+00:00 .. 2026-09-21T00:00:00+00:00".to_owned(),
                    base: None,
                    target: Some(TablePath::new("mysql_shop", "pets_20260920")),
                    shard: Some("2026-09-20".parse().unwrap()),
                },
            ]
        );
    }

    #[test]
    fn a_table_renamed_within_its_window_is_listed_under_its_new_name_as_renamed() {
        let renamed = BuildLog::new(
            &Run::new(
                Ulid::from_parts(1, 0),
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
        assert_eq!(rows[0].lifecycle, Some(Lifecycle::Renamed));
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
