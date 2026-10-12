use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use lithify_config::ArtifactKind;
use lithify_config::infra::{Sink, Source};
use lithify_core::build_log::Latest;
use lithify_core::position::RecordedAt;
use lithify_core::sink::WarehouseClient;
use lithify_core::window::Window;

use crate::journal::Journal;
use crate::pipeline::{Capture, Warehouse};
use crate::plan::Base;
use crate::resumption::Resumption;
use crate::settlement::Settlement;
use crate::unrunnable::Unrunnable;

pub struct CatchUp<'a> {
    sink: &'a Sink<Warehouse>,
    source: &'a Source<Capture>,
    onto: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
}

impl<'a> CatchUp<'a> {
    pub fn new(
        sink: &'a Sink<Warehouse>,
        source: &'a Source<Capture>,
        onto: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            sink,
            source,
            onto,
            now,
        }
    }

    pub fn settle<K, S>(
        &self,
        client: &K,
        settlement: &S,
        journal: &mut Journal,
        mut plan: impl FnMut(&Window, &Base) -> Result<S::Output, Unrunnable>,
    ) -> Result<(), Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
        S: Settlement,
    {
        // TODO: rebuild only the tables a window is missing rather than every table, and take a
        // `--table` to name them, once a plan can be narrowed to some of the tables it replays.
        let start = self.resumption(client)?.start();
        let mut base = self.base(client, start)?;

        for window in self
            .sink
            .artifacts()
            .schedule()
            .windows(start, RecordedAt::from(self.now))
        {
            journal.window(&window);
            if !settlement.report(&window, plan(&window, &base)?, journal)? {
                break;
            }
            base = Base::Shard(window);
        }

        Ok(())
    }

    fn namespace(&self) -> &str {
        self.sink.artifacts().namespace(ArtifactKind::Snapshot)
    }

    fn resumption<K>(&self, client: &K) -> Result<Resumption, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        client.create_build_log::<Unrunnable>(self.namespace())?;

        Resumption::read(
            &client.latest_build_logs(self.namespace(), &Latest::Attempted)?,
            &client.latest_build_logs(self.namespace(), &Latest::Succeeded)?,
            self.source.since(),
            &self.source.capture().to_json()?,
        )
    }

    fn base<K>(&self, client: &K, start: RecordedAt) -> Result<Base, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        match self.onto {
            Some(onto) => Ok(Base::Shard(Window::new(RecordedAt::from(onto), start))),
            // TODO: follow a table renamed within the window before, whose log is keyed by the
            // name it opened that window with rather than the one it opens this window with.
            None => Ok(Base::Recorded(
                client
                    .latest_build_logs(self.namespace(), &Latest::SucceededUntil(start))?
                    .into_iter()
                    .map(|log| (log.origin().clone(), log.target().clone()))
                    .collect::<BTreeMap<_, _>>(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use duckdb::Connection;
    use ulid::Ulid;

    use lithify_config::Pipeline;
    use lithify_core::build_log::{BuildLog, Outcome, Run, Status};
    use lithify_core::model::TableId;
    use lithify_core::position::RecordedAt;
    use lithify_core::sink::{TablePath, WarehouseClient};
    use lithify_core::sql::SqlString;
    use lithify_protocol::DuckDbWarehouse;
    use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbClient};

    use super::*;
    use crate::pipeline::{Capture, Warehouse};
    use crate::plan::SnapshotPlan;
    use crate::plan::tests::{BUILT_CHANGE_LOGS, pipeline, rows};
    use crate::settlement::Build;

    const CAPTURE: &str = r#"{"debezium":{"dialect":"mysql"}}"#;

    fn at(rfc3339: &str) -> DateTime<Utc> {
        rfc3339.parse().unwrap()
    }

    fn built_before(table: &str, capture: &str) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::from_parts(1, 0),
                "snapshot",
                "daily",
                "primary",
                capture,
                "0.1.0",
            ),
            TableId::new("shop", table),
            None,
            TablePath::new("mysql_shop", &format!("{table}_20260919")),
            Window::new(
                RecordedAt::from(at("2026-09-19T00:00:00Z")),
                RecordedAt::from(at("2026-09-20T00:00:00Z")),
            ),
            Outcome::new(Status::Succeeded, at("2026-09-20T00:10:00Z")),
        )
    }

    fn logged(change_logs: &str, before: &[BuildLog]) -> DuckDbClient {
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new("mysql_shop_cdc"),
        );
        client.execute(&SqlString::from(change_logs)).unwrap();
        client
            .execute(&SqlString::from(
                "CREATE SCHEMA mysql_shop;
                 CREATE TABLE mysql_shop.people_20260919 AS SELECT 7::BIGINT AS id;
                 CREATE TABLE mysql_shop.pets_20260919 AS SELECT 7::BIGINT AS id",
            ))
            .unwrap();
        client.create_build_log::<Unrunnable>("mysql_shop").unwrap();
        for log in before {
            client.append_build_log(log).unwrap();
        }

        client
    }

    fn catch_up(
        pipeline: &Pipeline<Capture, Warehouse>,
        client: &DuckDbClient,
        now: &str,
    ) -> Result<Journal, Unrunnable> {
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "rustfs")
            .unwrap();
        let settlement = Build::new(sink, source).unwrap();
        let mut journal = Journal::default();

        CatchUp::new(sink, source, None, at(now)).settle(
            client,
            &settlement,
            &mut journal,
            |window, base| {
                source.capture().with_duckdb_change_logs(
                    source.topic_prefix().unwrap(),
                    SnapshotPlan::new(client, sink, window, base, &settlement),
                )
            },
        )?;

        Ok(journal)
    }

    const PEOPLE: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789948800001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value)";

    #[test]
    fn a_pipeline_catches_up_window_by_window_folding_each_onto_the_one_before() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        catch_up(&pipeline, &client, "2026-09-22T05:00:00Z").unwrap();

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
    fn a_pipeline_that_has_caught_up_builds_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        let journal = catch_up(&pipeline, &client, "2026-09-20T23:00:00Z").unwrap();

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

        let journal = catch_up(&pipeline, &client, "2026-09-22T05:00:00Z").unwrap();

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

        let changed = catch_up(&pipeline, &client, "2026-09-22T05:00:00Z");

        assert!(matches!(changed, Err(Unrunnable::CaptureChanged { .. })));
    }
}
