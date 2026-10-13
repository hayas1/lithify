use chrono::NaiveDate;

use lithify_config::Schedule;
use lithify_core::build_log::{BuildLog, Status};
use lithify_core::model::TableId;
use lithify_core::sink::TablePath;

use crate::command::BuildCommand;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkOverview {
    name: String,
    schedule: Schedule,
    latest: Vec<BuildLog>,
    discrepancies: Vec<String>,
}

impl SinkOverview {
    pub fn new(
        name: &str,
        schedule: Schedule,
        latest: Vec<BuildLog>,
        discrepancies: Vec<String>,
    ) -> Self {
        Self {
            name: name.to_owned(),
            schedule,
            latest,
            discrepancies,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn rows(&self) -> Vec<TableRow> {
        let mut rows: Vec<_> = self
            .latest
            .iter()
            .map(|log| TableRow::new(log, self.schedule))
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
    pub status: Status,
    pub window: String,
    pub base: Option<TablePath>,
    pub target: TablePath,
    pub shard: Option<NaiveDate>,
}

impl TableRow {
    fn new(log: &BuildLog, schedule: Schedule) -> Self {
        let window = log.window();

        Self {
            table: log.origin().clone(),
            status: log.status().clone(),
            window: format!("{} .. {}", window.from(), window.to()),
            base: log.base().cloned(),
            target: log.target().clone(),
            shard: schedule.date(window),
        }
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
            TableId::new("shop", table),
            base.map(|base| TablePath::new("mysql_shop", base)),
            TablePath::new("mysql_shop", &format!("{table}_20260920")),
            Window::new(
                RecordedAt::from(at("2026-09-20T00:00:00Z")),
                RecordedAt::from(at("2026-09-21T00:00:00Z")),
            ),
            Outcome::new(status, at("2026-09-21T00:10:00Z")),
        )
    }

    pub(crate) fn overview() -> SinkOverview {
        SinkOverview::new(
            "rustfs",
            Schedule::Daily,
            vec![
                logged("pets", None, Status::Failed("no such column".to_owned())),
                logged("people", Some("people_20260919"), Status::Succeeded),
            ],
            vec!["shop.pets failed to build".to_owned()],
        )
    }

    #[test]
    fn a_sink_lists_the_latest_build_of_each_table_in_the_order_of_their_names() {
        assert_eq!(
            overview().rows(),
            [
                TableRow {
                    table: TableId::new("shop", "people"),
                    status: Status::Succeeded,
                    window: "2026-09-20T00:00:00+00:00 .. 2026-09-21T00:00:00+00:00".to_owned(),
                    base: Some(TablePath::new("mysql_shop", "people_20260919")),
                    target: TablePath::new("mysql_shop", "people_20260920"),
                    shard: Some("2026-09-20".parse().unwrap()),
                },
                TableRow {
                    table: TableId::new("shop", "pets"),
                    status: Status::Failed("no such column".to_owned()),
                    window: "2026-09-20T00:00:00+00:00 .. 2026-09-21T00:00:00+00:00".to_owned(),
                    base: None,
                    target: TablePath::new("mysql_shop", "pets_20260920"),
                    shard: Some("2026-09-20".parse().unwrap()),
                },
            ]
        );
    }

    #[test]
    fn a_sink_lists_what_check_finds_wrong_with_it() {
        assert_eq!(overview().discrepancies(), ["shop.pets failed to build"]);
    }

    #[test]
    fn a_table_is_rebuilt_by_building_its_shard_narrowed_to_it() {
        let rows = overview().rows();

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
