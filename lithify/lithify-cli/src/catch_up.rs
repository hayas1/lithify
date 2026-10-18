use chrono::{DateTime, Utc};

use lithify_config::ArtifactKind;
use lithify_config::infra::{Sink, Source};
use lithify_core::build_log::Latest;
use lithify_core::position::RecordedAt;
use lithify_core::sink::WarehouseClient;

use crate::course::Itinerary;
use crate::pipeline::{Capture, WarehouseConfig};
use crate::plan::Tables;
use crate::resumption::Resumption;
use crate::unrunnable::Unrunnable;

pub struct CatchUp<'a> {
    sink: &'a Sink<WarehouseConfig>,
    source: &'a Source<Capture>,
    now: DateTime<Utc>,
}

impl<'a> CatchUp<'a> {
    pub fn new(
        sink: &'a Sink<WarehouseConfig>,
        source: &'a Source<Capture>,
        now: DateTime<Utc>,
    ) -> Self {
        Self { sink, source, now }
    }

    pub fn itinerary<K>(&self, client: &K) -> Result<Itinerary, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        // TODO: rebuild only the tables a window is missing rather than every table.
        let resumption = self.resumption(client)?;

        Ok(Itinerary::new(
            resumption.windows(self.sink.artifacts().schedule(), RecordedAt::from(self.now)),
            resumption.previous(client, self.namespace())?,
            Tables::Every,
        ))
    }

    fn namespace(&self) -> &str {
        self.sink.artifacts().namespace(ArtifactKind::Snapshot)
    }

    fn resumption<K>(&self, client: &K) -> Result<Resumption, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        Resumption::read(
            &client.build_logs(self.namespace(), &Latest::Attempted.into())?,
            &client.build_logs(self.namespace(), &Latest::Succeeded.into())?,
            self.source.since(),
            &self.source.capture().to_json()?,
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

        settle(&pipeline, &client, &Course::CatchUp, "2026-09-22T05:00:00Z").unwrap();

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

        settle(&pipeline, &client, &Course::CatchUp, "2026-09-22T05:00:00Z").unwrap();

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
    fn a_pipeline_built_for_the_first_time_before_its_first_midnight_builds_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        let journal = settle(&pipeline, &client, &Course::CatchUp, "1970-01-01T23:00:00Z").unwrap();

        assert_eq!(journal.entries().count(), 0);
    }

    #[test]
    fn a_pipeline_that_has_caught_up_builds_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        let journal = settle(&pipeline, &client, &Course::CatchUp, "2026-09-20T23:00:00Z").unwrap();

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

        let journal = settle(&pipeline, &client, &Course::CatchUp, "2026-09-22T05:00:00Z").unwrap();

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

        let changed = settle(&pipeline, &client, &Course::CatchUp, "2026-09-22T05:00:00Z");

        assert!(matches!(changed, Err(Unrunnable::CaptureChanged { .. })));
    }
}
