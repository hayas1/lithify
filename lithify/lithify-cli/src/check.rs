use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use lithify_config::Schedule;
use lithify_core::build_log::{BuildLog, Latest};
use lithify_core::model::TableId;
use lithify_core::position::RecordedAt;
use lithify_core::sink::{TablePath, WarehouseClient};
use lithify_core::window::Window;

use crate::unrunnable::Unrunnable;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Discrepancy {
    NeverBuilt {
        since: RecordedAt,
    },
    Overdue {
        table: TableId,
        built_until: RecordedAt,
        due_until: RecordedAt,
    },
    Failed {
        table: TableId,
        window: Window,
        error: String,
    },
    Missing {
        table: TableId,
        target: TablePath,
    },
    CaptureChanged {
        logged: String,
        configured: String,
    },
}

impl Display for Discrepancy {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NeverBuilt { since } => write!(formatter, "nothing is built since {since}"),
            Self::Overdue {
                table,
                built_until,
                due_until,
            } => write!(
                formatter,
                "{table} is built until {built_until} but due until {due_until}"
            ),
            Self::Failed {
                table,
                window,
                error,
            } => write!(
                formatter,
                "{table} failed to build the window from {} to {}: {error}",
                window.from(),
                window.to()
            ),
            Self::Missing { table, target } => write!(
                formatter,
                "{table} is logged as built into {target} but {target} is not there"
            ),
            Self::CaptureChanged { logged, configured } => write!(
                formatter,
                "the last run captured as {logged} but the pipeline is captured as {configured} now"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inspection {
    schedule: Schedule,
    latest: Vec<BuildLog>,
    discrepancies: Vec<Discrepancy>,
}

impl Inspection {
    pub fn schedule(&self) -> Schedule {
        self.schedule
    }

    pub fn latest(&self) -> &[BuildLog] {
        &self.latest
    }

    pub fn discrepancies(&self) -> &[Discrepancy] {
        &self.discrepancies
    }
}

pub struct Expectation {
    schedule: Schedule,
    since: RecordedAt,
    capture: String,
    now: RecordedAt,
}

impl Expectation {
    pub fn new(schedule: Schedule, since: RecordedAt, capture: &str, now: RecordedAt) -> Self {
        Self {
            schedule,
            since,
            capture: capture.to_owned(),
            now,
        }
    }

    pub fn read<K>(&self, client: &K, namespace: &str) -> Result<Inspection, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        if !client.exists(&BuildLog::path(namespace))? {
            return Ok(Inspection {
                schedule: self.schedule,
                latest: Vec::new(),
                discrepancies: self.discrepancies(&[], &[], &BTreeSet::new()),
            });
        }

        let latest = client.latest_build_logs(namespace, &Latest::Attempted)?;
        let succeeded = client.latest_build_logs(namespace, &Latest::Succeeded)?;
        let mut present = BTreeSet::new();
        for target in succeeded.iter().filter_map(BuildLog::target) {
            if client.exists(target)? {
                present.insert(target.clone());
            }
        }

        Ok(Inspection {
            schedule: self.schedule,
            discrepancies: self.discrepancies(&latest, &succeeded, &present),
            latest,
        })
    }

    // TODO: expect the tables the catalog holds at the end of the last closed window rather than
    // only the ones the build log has seen, which leaves out a table never built at all.
    pub fn discrepancies(
        &self,
        latest: &[BuildLog],
        succeeded: &[BuildLog],
        present: &BTreeSet<TablePath>,
    ) -> Vec<Discrepancy> {
        let Some(last_run) = latest.iter().map(BuildLog::run_id).max() else {
            return match self.due_until(self.since) {
                Some(_) => vec![Discrepancy::NeverBuilt { since: self.since }],
                None => vec![],
            };
        };

        let built_last = latest.iter().filter(|log| log.run_id() == last_run);

        self.capture_changed(built_last)
            .into_iter()
            .chain(
                latest
                    .iter()
                    .filter_map(|log| log.table_to().map(|table| (table, log)))
                    .flat_map(|(table, log)| {
                        let built = log.last_success(succeeded);

                        self.missing_from(table, built, present).into_iter().chain(
                            self.failed(table, log)
                                .or_else(|| self.overdue(table, built)),
                        )
                    }),
            )
            .collect()
    }

    fn due_until(&self, built_until: RecordedAt) -> Option<RecordedAt> {
        self.schedule
            .windows(built_until, self.now)
            .last()
            .map(|window| *window.to())
    }

    fn capture_changed<'b>(
        &self,
        mut built_last: impl Iterator<Item = &'b BuildLog>,
    ) -> Option<Discrepancy> {
        built_last
            .find(|log| log.capture() != self.capture)
            .map(|log| Discrepancy::CaptureChanged {
                logged: log.capture().to_owned(),
                configured: self.capture.clone(),
            })
    }

    fn missing_from(
        &self,
        table: &TableId,
        built: Option<&BuildLog>,
        present: &BTreeSet<TablePath>,
    ) -> Option<Discrepancy> {
        built.and_then(|built| self.missing(table, built, present))
    }

    fn failed(&self, table: &TableId, log: &BuildLog) -> Option<Discrepancy> {
        log.status().error().map(|error| Discrepancy::Failed {
            table: table.clone(),
            window: log.window().clone(),
            error: error.to_owned(),
        })
    }

    fn missing(
        &self,
        table: &TableId,
        built: &BuildLog,
        present: &BTreeSet<TablePath>,
    ) -> Option<Discrepancy> {
        let target = built.target()?;

        (!present.contains(target)).then(|| Discrepancy::Missing {
            table: table.clone(),
            target: target.clone(),
        })
    }

    fn overdue(&self, table: &TableId, built: Option<&BuildLog>) -> Option<Discrepancy> {
        let built_until = built.map_or(self.since, |built| *built.window().to());

        self.due_until(built_until)
            .map(|due_until| Discrepancy::Overdue {
                table: table.clone(),
                built_until,
                due_until,
            })
    }
}

#[cfg(test)]
mod tests {
    use ulid::Ulid;

    use chrono::{DateTime, Utc};

    use duckdb::Connection;

    use lithify_core::build_log::{Outcome, Run, Status};
    use lithify_core::sql::SqlString;
    use lithify_protocol::DuckDbWarehouse;
    use lithify_protocol::sink::duckdb::client::DuckDbClient;

    use super::*;

    fn at(rfc3339: &str) -> RecordedAt {
        RecordedAt::from(rfc3339.parse::<DateTime<Utc>>().unwrap())
    }

    fn day(date: &str) -> Window {
        let from = at(&format!("{date}T00:00:00Z"));

        Window::new(
            from,
            RecordedAt::from(from.milliseconds_since_epoch() + 86_400_000),
        )
    }

    fn logged(run: u64, table: &str, window: Window, status: Status) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::from_parts(run, 0),
                "snapshot",
                "daily",
                "primary",
                "{}",
                "0.1.0",
            ),
            Some(TableId::new("shop", table)),
            Some(TableId::new("shop", table)),
            None,
            Some(TablePath::new(
                "mysql_shop",
                &format!("{table}_{}", window.from().milliseconds_since_epoch()),
            )),
            window,
            Outcome::new(status, DateTime::from_timestamp_millis(0).unwrap()),
        )
    }

    fn expected(now: &str) -> Expectation {
        Expectation::new(Schedule::Daily, at("2026-09-19T00:00:00Z"), "{}", at(now))
    }

    fn present(logs: &[BuildLog]) -> BTreeSet<TablePath> {
        logs.iter().filter_map(BuildLog::target).cloned().collect()
    }

    #[test]
    fn a_pipeline_built_up_to_the_last_closed_window_holds_no_discrepancy() {
        let people = logged(2, "people", day("2026-09-20"), Status::Succeeded);
        let built = [people];

        assert_eq!(
            expected("2026-09-21T05:00:00Z").discrepancies(&built, &built, &present(&built)),
            []
        );
    }

    #[test]
    fn a_pipeline_never_built_is_due_once_a_window_has_closed() {
        assert_eq!(
            expected("2026-09-21T05:00:00Z").discrepancies(&[], &[], &BTreeSet::new()),
            [Discrepancy::NeverBuilt {
                since: at("2026-09-19T00:00:00Z")
            }]
        );
        assert_eq!(
            expected("2026-09-19T05:00:00Z").discrepancies(&[], &[], &BTreeSet::new()),
            []
        );
    }

    #[test]
    fn a_table_built_short_of_the_last_closed_window_is_overdue() {
        let people = logged(2, "people", day("2026-09-19"), Status::Succeeded);
        let built = [people];

        assert_eq!(
            expected("2026-09-21T05:00:00Z").discrepancies(&built, &built, &present(&built)),
            [Discrepancy::Overdue {
                table: TableId::new("shop", "people"),
                built_until: at("2026-09-20T00:00:00Z"),
                due_until: at("2026-09-21T00:00:00Z"),
            }]
        );
    }

    #[test]
    fn a_table_whose_last_build_failed_is_reported_as_failed_rather_than_overdue() {
        let before = logged(1, "people", day("2026-09-19"), Status::Succeeded);
        let failed = logged(
            2,
            "people",
            day("2026-09-20"),
            Status::Failed("no era".to_owned()),
        );

        assert_eq!(
            expected("2026-09-21T05:00:00Z").discrepancies(
                std::slice::from_ref(&failed),
                std::slice::from_ref(&before),
                &present(std::slice::from_ref(&before))
            ),
            [Discrepancy::Failed {
                table: TableId::new("shop", "people"),
                window: day("2026-09-20"),
                error: "no era".to_owned(),
            }]
        );
    }

    #[test]
    fn a_pipeline_whose_capture_changed_since_its_last_run_is_reported_as_such() {
        let people = logged(2, "people", day("2026-09-20"), Status::Succeeded);
        let built = [people];

        assert_eq!(
            Expectation::new(
                Schedule::Daily,
                at("2026-09-19T00:00:00Z"),
                r#"{"debezium":{"dialect":"postgres"}}"#,
                at("2026-09-21T05:00:00Z")
            )
            .discrepancies(&built, &built, &present(&built)),
            [Discrepancy::CaptureChanged {
                logged: "{}".to_owned(),
                configured: r#"{"debezium":{"dialect":"postgres"}}"#.to_owned(),
            }]
        );
    }

    #[test]
    fn a_table_logged_as_built_whose_target_is_gone_is_missing() {
        let people = logged(2, "people", day("2026-09-20"), Status::Succeeded);
        let built = [people.clone()];

        assert_eq!(
            expected("2026-09-21T05:00:00Z").discrepancies(&built, &built, &BTreeSet::new()),
            [Discrepancy::Missing {
                table: TableId::new("shop", "people"),
                target: people.target().unwrap().clone(),
            }]
        );
    }

    #[test]
    fn a_table_the_last_run_did_not_build_is_still_expected() {
        let left = logged(1, "pets", day("2026-09-19"), Status::Succeeded);
        let people = logged(2, "people", day("2026-09-20"), Status::Succeeded);
        let built = [left, people];

        assert_eq!(
            expected("2026-09-21T05:00:00Z").discrepancies(&built, &built, &present(&built)),
            [Discrepancy::Overdue {
                table: TableId::new("shop", "pets"),
                built_until: at("2026-09-20T00:00:00Z"),
                due_until: at("2026-09-21T00:00:00Z"),
            }]
        );
    }

    #[test]
    fn a_discrepancy_reads_as_what_went_wrong_with_which_table() {
        assert_eq!(
            Discrepancy::Overdue {
                table: TableId::new("shop", "people"),
                built_until: at("2026-09-20T00:00:00Z"),
                due_until: at("2026-09-21T00:00:00Z"),
            }
            .to_string(),
            "shop.people is built until 2026-09-20T00:00:00+00:00 but due until 2026-09-21T00:00:00+00:00"
        );
    }

    fn logging(logs: &[BuildLog]) -> DuckDbClient {
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new("mysql_shop_cdc"),
        );
        client.create_build_log::<Unrunnable>("mysql_shop").unwrap();
        for log in logs {
            client.append_build_log("mysql_shop", log).unwrap();
        }

        client
    }

    #[test]
    fn a_warehouse_is_checked_against_the_tables_it_holds() {
        let people = logged(2, "people", day("2026-09-20"), Status::Succeeded);
        let pets = logged(2, "pets", day("2026-09-20"), Status::Succeeded);
        let client = logging(&[people.clone(), pets.clone()]);
        client
            .execute(&SqlString::from(format!(
                "CREATE TABLE \"mysql_shop\".\"{}\" AS SELECT 1 AS id",
                people.target().unwrap().name()
            )))
            .unwrap();

        assert_eq!(
            expected("2026-09-21T05:00:00Z")
                .read(&client, "mysql_shop")
                .unwrap()
                .discrepancies(),
            [Discrepancy::Missing {
                table: TableId::new("shop", "pets"),
                target: pets.target().unwrap().clone(),
            }]
        );
    }

    #[test]
    fn a_warehouse_holding_no_build_log_was_never_built() {
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new("mysql_shop_cdc"),
        );

        assert_eq!(
            expected("2026-09-21T05:00:00Z")
                .read(&client, "mysql_shop")
                .unwrap()
                .discrepancies(),
            [Discrepancy::NeverBuilt {
                since: at("2026-09-19T00:00:00Z")
            }]
        );
    }

    #[test]
    fn a_warehouse_is_inspected_for_the_latest_build_of_each_table() {
        let failed = logged(
            3,
            "people",
            day("2026-09-20"),
            Status::Failed("no such column".to_owned()),
        );
        let client = logging(&[
            logged(2, "people", day("2026-09-20"), Status::Succeeded),
            failed.clone(),
        ]);

        assert_eq!(
            expected("2026-09-21T05:00:00Z")
                .read(&client, "mysql_shop")
                .unwrap()
                .latest(),
            [failed]
        );
    }
}
