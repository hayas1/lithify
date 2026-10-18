use chrono::{DateTime, NaiveDate, Utc};

use lithify_config::infra::{Sink, Source};
use lithify_core::build_log::{Latest, Series};
use lithify_core::sink::{Namespace, WarehouseClient};
use lithify_core::ts::{Event, Ts};

use crate::artifacts::{ArtifactConfig, DeclaredSnapshot};
use crate::course::Itinerary;
use crate::pipeline::{CaptureConfig, WarehouseConfig};
use crate::plan::Tables;
use crate::resumption::Resumption;
use crate::shard::Shard;
use crate::unrunnable::Unrunnable;

pub struct CatchUp<'a> {
    sink: &'a Sink<WarehouseConfig, ArtifactConfig>,
    source: &'a Source<CaptureConfig>,
    now: DateTime<Utc>,
    to: Option<NaiveDate>,
}

impl<'a> CatchUp<'a> {
    pub fn new(
        sink: &'a Sink<WarehouseConfig, ArtifactConfig>,
        source: &'a Source<CaptureConfig>,
        now: DateTime<Utc>,
        to: Option<NaiveDate>,
    ) -> Self {
        Self {
            sink,
            source,
            now,
            to,
        }
    }

    pub fn itinerary<K>(&self, client: &K) -> Result<Itinerary, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        // TODO: rebuild only the tables a window is missing rather than every table.
        let resumption = self.resumption(client)?;

        Ok(Itinerary::new(
            resumption.windows(
                DeclaredSnapshot::of(self.sink)?.schedule(),
                self.until(&resumption)?,
            ),
            resumption.previous(client, &self.namespace()?, &self.series()?)?,
            Tables::Every,
        ))
    }

    fn until(&self, resumption: &Resumption) -> Result<Ts<Event>, Unrunnable> {
        let Some(date) = self.to else {
            return Ok(Ts::from(self.now));
        };
        let until = *Shard::new(self.sink, self.source, self.now)
            .closed(date)?
            .to();

        match resumption.built_past(until) {
            Some(built_until) => Err(Unrunnable::Overtaken { date, built_until }),
            None => Ok(until),
        }
    }

    fn namespace(&self) -> Result<Namespace, Unrunnable> {
        Ok(DeclaredSnapshot::of(self.sink)?.namespace().clone())
    }

    fn series(&self) -> Result<Series, Unrunnable> {
        Ok(DeclaredSnapshot::of(self.sink)?.series())
    }

    fn resumption<K>(&self, client: &K) -> Result<Resumption, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        Resumption::read(
            &client.build_logs(&self.namespace()?, &Latest::Attempted.into())?,
            &client.build_logs(&self.namespace()?, &Latest::Succeeded.into())?,
            self.source.since(),
            &self.source.capture().to_json()?,
            &self.series()?,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::course::Course;
    use crate::course::tests::settle;
    use crate::plan::tests::{
        BUILT_CHANGE_LOGS, CAPTURE, PEOPLE, built_before, logged, pipeline, rows,
    };
    use crate::unrunnable::Unrunnable;

    #[test]
    fn a_pipeline_catches_up_window_by_window_folding_each_onto_the_one_before() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(
            &pipeline,
            &client,
            &Course::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, COALESCE(base, '') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                ("mysql_shop.people_20260919".to_owned(), String::new()),
                (
                    "mysql_shop.people_20260920".to_owned(),
                    "mysql_shop.people_20260919".to_owned()
                ),
                (
                    "mysql_shop.people_20260921".to_owned(),
                    "mysql_shop.people_20260920".to_owned()
                ),
            ]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260921 ORDER BY id"
            ),
            [
                ("7".to_owned(), "kept".to_owned()),
                ("8".to_owned(), "kept".to_owned())
            ]
        );
    }

    #[test]
    fn a_pipeline_built_for_the_first_time_builds_only_the_shard_closing_at_the_last_midnight() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        settle(
            &pipeline,
            &client,
            &Course::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, strftime(window_from, '%Y-%m-%d') || '..' || strftime(window_to, '%Y-%m-%d') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [(
                "mysql_shop.people_20260921".to_owned(),
                "1970-01-01..2026-09-22".to_owned()
            )]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260921 ORDER BY id"
            ),
            [("8".to_owned(), "kept".to_owned())]
        );
    }

    #[test]
    fn a_pipeline_caught_up_to_a_date_builds_no_shard_dated_after_it() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, COALESCE(base, '') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                ("mysql_shop.people_20260919".to_owned(), String::new()),
                (
                    "mysql_shop.people_20260920".to_owned(),
                    "mysql_shop.people_20260919".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn a_pipeline_built_for_the_first_time_up_to_a_date_is_caught_up_from_there_after() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();
        settle(
            &pipeline,
            &client,
            &Course::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, strftime(window_from, '%Y-%m-%d') || '..' || strftime(window_to, '%Y-%m-%d') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                (
                    "mysql_shop.people_20260920".to_owned(),
                    "1970-01-01..2026-09-21".to_owned()
                ),
                (
                    "mysql_shop.people_20260921".to_owned(),
                    "2026-09-21..2026-09-22".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn a_pipeline_is_not_caught_up_to_a_date_whose_shard_has_not_closed() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        let unclosed = settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-22"),
            "2026-09-22T05:00:00Z",
        );

        assert!(matches!(unclosed, Err(Unrunnable::Unclosed(_))));
    }

    #[test]
    fn a_pipeline_is_not_caught_up_to_a_date_before_its_source_began() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        let unsharded = settle(
            &pipeline,
            &client,
            &caught_up_to("1969-12-31"),
            "2026-09-22T05:00:00Z",
        );

        assert!(matches!(unsharded, Err(Unrunnable::Unsharded(_))));
    }

    #[test]
    fn a_pipeline_is_not_caught_up_to_a_date_the_build_log_has_already_built_past() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);
        settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        let overtaken = settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-19"),
            "2026-09-22T05:00:00Z",
        );

        assert!(matches!(
            overtaken,
            Err(Unrunnable::Overtaken { date, .. }) if date == "2026-09-19".parse().unwrap()
        ));
    }

    #[test]
    fn a_pipeline_caught_up_to_the_date_it_was_last_built_to_builds_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);
        settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        let journal = settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(journal.entries().count(), 0);
    }

    fn caught_up_to(date: &str) -> Course {
        Course::CatchUp {
            to: Some(date.parse().unwrap()),
        }
    }

    #[test]
    fn a_pipeline_built_for_the_first_time_before_its_first_midnight_builds_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        let journal = settle(
            &pipeline,
            &client,
            &Course::CatchUp { to: None },
            "1970-01-01T23:00:00Z",
        )
        .unwrap();

        assert_eq!(journal.entries().count(), 0);
    }

    #[test]
    fn a_pipeline_that_has_caught_up_builds_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        let journal = settle(
            &pipeline,
            &client,
            &Course::CatchUp { to: None },
            "2026-09-20T23:00:00Z",
        )
        .unwrap();

        assert_eq!(journal.entries().count(), 0);
    }

    #[test]
    fn a_window_a_table_failed_in_stops_the_pipeline_from_catching_up_past_it() {
        let pipeline = pipeline();
        let client = logged(
            BUILT_CHANGE_LOGS,
            &[
                built_before("people", CAPTURE),
                built_before("pets", CAPTURE),
            ],
        );

        let journal = settle(
            &pipeline,
            &client,
            &Course::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert!(!journal.complete());
        assert_eq!(
            rows(
                &client,
                "SELECT DISTINCT strftime(window_from, '%Y%m%d'), 'built' FROM mysql_shop._lithify_log ORDER BY 1"
            ),
            [
                ("20260919".to_owned(), "built".to_owned()),
                ("20260920".to_owned(), "built".to_owned())
            ]
        );
    }

    #[test]
    fn a_pipeline_whose_capture_changed_since_its_last_run_is_not_caught_up() {
        let pipeline = pipeline();
        let client = logged(
            PEOPLE,
            &[built_before(
                "people",
                r#"{"debezium":{"dialect":"postgres"}}"#,
            )],
        );

        let changed = settle(
            &pipeline,
            &client,
            &Course::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        );

        assert!(matches!(changed, Err(Unrunnable::CaptureChanged { .. })));
    }
}
