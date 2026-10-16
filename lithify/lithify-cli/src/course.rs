use chrono::{DateTime, NaiveDate, Utc};

use lithify_config::ArtifactKind;
use lithify_config::infra::{Sink, Source};
use lithify_core::build_log::BuildLog;
use lithify_core::sink::WarehouseClient;
use lithify_core::window::Window;

use crate::catch_up::CatchUp;
use crate::journal::Journal;
use crate::pipeline::{Capture, Warehouse};
use crate::plan::Tables;
use crate::settlement::Settlement;
use crate::shard::Shard;
use crate::unrunnable::Unrunnable;

pub enum Course {
    CatchUp,
    Shard { date: NaiveDate, tables: Tables },
}

impl Course {
    pub fn itinerary<K>(
        &self,
        sink: &Sink<Warehouse>,
        source: &Source<Capture>,
        now: DateTime<Utc>,
        client: &K,
    ) -> Result<Itinerary, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        client
            .create_build_log::<Unrunnable>(sink.artifacts().namespace(ArtifactKind::Snapshot))?;

        match self {
            Self::CatchUp => CatchUp::new(sink, source, now).itinerary(client),
            Self::Shard { date, tables } => {
                Shard::new(sink, source, now).itinerary(client, *date, tables)
            }
        }
    }
}

pub struct Itinerary {
    windows: Vec<Window>,
    previous: Vec<BuildLog>,
    tables: Tables,
}

impl Itinerary {
    pub fn new(windows: Vec<Window>, previous: Vec<BuildLog>, tables: Tables) -> Self {
        Self {
            windows,
            previous,
            tables,
        }
    }

    pub fn settle<S: Settlement>(
        self,
        settlement: &S,
        journal: &mut Journal,
        mut plan: impl FnMut(&Window, &[BuildLog], &Tables) -> Result<S::Output, Unrunnable>,
    ) -> Result<(), Unrunnable> {
        let mut previous = self.previous;

        for window in self.windows {
            journal.window(&window);
            match settlement.report(&window, plan(&window, &previous, &self.tables)?, journal)? {
                Some(built) => previous = built,
                None => break,
            }
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use lithify_config::Pipeline;
    use lithify_core::model::TableId;
    use lithify_core::sql::SqlString;
    use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbClient};

    use super::*;
    use lithify_config::Schedule;
    use lithify_core::position::RecordedAt;

    use crate::check::Expectation;
    use crate::plan::SnapshotPlan;
    use crate::plan::tests::{
        BUILT_CHANGE_LOGS, CAPTURE, PEOPLE, at, built_before, logged, pipeline, rows,
    };
    use crate::settlement::Build;

    const NOW: &str = "2026-09-22T05:00:00Z";

    const PETS_CREATED_ON_20260920: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'gtid': NULL}, 'ts_ms': 1789862400005::BIGINT,
            'ddl': 'CREATE TABLE pets (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"pets\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.pets\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400009::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 9::BIGINT}})
        ) AS t(value)";

    const PETS_DROPPED_ON_20260920: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE pets (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"pets\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL}, 'ts_ms': 1789862400005::BIGINT,
            'ddl': 'DROP TABLE pets',
            'tableChanges': [{'type': 'DROP', 'id': '\"shop\".\"pets\"', 'table': NULL}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value)";

    const PEOPLE_RENAMED_ON_20260920: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL}, 'ts_ms': 1789862400005::BIGINT,
            'ddl': 'RENAME TABLE people TO persons',
            'tableChanges': [{'type': 'ALTER', 'id': '\"shop\".\"persons\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.persons\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789948800009::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 5::BIGINT, 'gtid': NULL},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 9::BIGINT}})
        ) AS t(value)";

    fn shard(date: &str, tables: &[TableId]) -> Course {
        Course::Shard {
            date: date.parse().unwrap(),
            tables: match tables {
                [] => Tables::Every,
                named => Tables::Named(named.iter().cloned().collect()),
            },
        }
    }

    pub(crate) fn settle(
        pipeline: &Pipeline<Capture, Warehouse>,
        client: &DuckDbClient,
        course: &Course,
        now: &str,
    ) -> Result<Journal, Unrunnable> {
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "rustfs")
            .unwrap();
        let settlement = Build::new(sink, source).unwrap();
        let mut journal = Journal::default();

        course.itinerary(sink, source, at(now), client)?.settle(
            &settlement,
            &mut journal,
            |window, previous, tables| {
                source.capture().with_duckdb_change_logs(
                    source.topic_prefix().unwrap(),
                    SnapshotPlan::new(client, sink, window, previous, tables, &settlement),
                )
            },
        )?;

        Ok(journal)
    }

    fn built(client: &DuckDbClient) -> Vec<(String, String)> {
        rows(
            client,
            "SELECT target, COALESCE(base, '') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id",
        )
    }

    fn folded(target: &str, base: &str) -> (String, String) {
        (target.to_owned(), base.to_owned())
    }

    #[test]
    fn a_shard_folds_onto_the_one_the_build_log_recorded_closing_at_its_opening() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(&pipeline, &client, &shard("2026-09-20", &[]), NOW).unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
            ]
        );
    }

    #[test]
    fn the_shard_the_change_log_opens_in_is_built_from_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        settle(&pipeline, &client, &shard("1970-01-01", &[]), NOW).unwrap();

        assert_eq!(built(&client), [folded("mysql_shop.people_19700101", "")]);
    }

    #[test]
    fn the_shard_the_change_log_opens_in_is_built_before_anything_was_logged() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);
        client
            .execute(&SqlString::from("DROP TABLE mysql_shop._lithify_log"))
            .unwrap();

        settle(&pipeline, &client, &shard("1970-01-01", &[]), NOW).unwrap();

        assert_eq!(built(&client), [folded("mysql_shop.people_19700101", "")]);
    }

    #[test]
    fn a_shard_nothing_was_recorded_closing_before_is_not_built() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        let baseless = settle(&pipeline, &client, &shard("2026-09-20", &[]), NOW);

        assert!(matches!(
            baseless,
            Err(Unrunnable::Baseless { tables, .. }) if tables == [TableId::new("shop", "people")]
        ));
    }

    #[test]
    fn a_shard_is_refused_naming_the_table_nothing_was_built_for_closing_at_its_opening() {
        let pipeline = pipeline();
        let client = logged(BUILT_CHANGE_LOGS, &[built_before("people", CAPTURE)]);

        let baseless = settle(&pipeline, &client, &shard("2026-09-20", &[]), NOW);

        assert!(matches!(
            baseless,
            Err(Unrunnable::Baseless { tables, .. }) if tables == [TableId::new("shop", "pets")]
        ));
        assert_eq!(built(&client), [folded("mysql_shop.people_20260919", "")]);
    }

    #[test]
    fn a_shard_narrowed_away_from_a_table_without_a_base_is_built() {
        let pipeline = pipeline();
        let client = logged(BUILT_CHANGE_LOGS, &[built_before("people", CAPTURE)]);

        settle(
            &pipeline,
            &client,
            &shard("2026-09-20", &[TableId::new("shop", "people")]),
            NOW,
        )
        .unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
            ]
        );
    }

    fn ids(client: &DuckDbClient, table: &str) -> Vec<String> {
        rows(
            client,
            &format!("SELECT id::VARCHAR, '' FROM mysql_shop.{table} ORDER BY id"),
        )
        .into_iter()
        .map(|(id, _)| id)
        .collect()
    }

    fn caught_up(changes: &str, now: &str) -> DuckDbClient {
        let client = logged(changes, &[built_before("people", CAPTURE)]);
        settle(&pipeline(), &client, &Course::CatchUp, now).unwrap();

        client
    }

    fn windows(journal: Journal) -> Vec<String> {
        journal
            .entries()
            .filter(|entry| entry.starts_with("window"))
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn a_catch_up_after_a_narrowed_shard_rebuilds_the_tables_the_shard_left_behind() {
        let pipeline = pipeline();
        let client = caught_up(PETS_CREATED_ON_20260920, "2026-09-21T05:00:00Z");

        settle(
            &pipeline,
            &client,
            &shard("2026-09-21", &[TableId::new("shop", "people")]),
            NOW,
        )
        .unwrap();
        settle(&pipeline, &client, &Course::CatchUp, NOW).unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
                folded("mysql_shop.pets_20260920", ""),
                folded("mysql_shop.people_20260921", "mysql_shop.people_20260920"),
                folded("mysql_shop.people_20260921", "mysql_shop.people_20260920"),
                folded("mysql_shop.pets_20260921", "mysql_shop.pets_20260920"),
            ]
        );
    }

    #[test]
    fn a_catch_up_rewound_by_a_shard_rebuilds_what_folded_onto_it_as_a_straight_one_did() {
        let pipeline = pipeline();
        let straight = caught_up(PETS_CREATED_ON_20260920, NOW);
        let rewound = caught_up(PETS_CREATED_ON_20260920, NOW);

        settle(&pipeline, &rewound, &shard("2026-09-20", &[]), NOW).unwrap();
        let rebuilt = settle(&pipeline, &rewound, &Course::CatchUp, NOW).unwrap();

        assert_eq!(
            windows(rebuilt),
            ["window 2026-09-21T00:00:00+00:00 .. 2026-09-22T00:00:00+00:00"]
        );
        assert_eq!(
            built(&rewound)[built(&straight).len()..],
            [
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
                folded("mysql_shop.pets_20260920", ""),
                folded("mysql_shop.people_20260921", "mysql_shop.people_20260920"),
                folded("mysql_shop.pets_20260921", "mysql_shop.pets_20260920"),
            ]
        );
        for table in ["people_20260921", "pets_20260921"] {
            assert_eq!(ids(&rewound, table), ids(&straight, table));
        }
    }

    #[test]
    fn a_shard_built_twice_holds_what_it_held_once_and_leaves_the_same_catch_up_to_do() {
        let pipeline = pipeline();
        let client = caught_up(PETS_CREATED_ON_20260920, NOW);

        settle(&pipeline, &client, &shard("2026-09-20", &[]), NOW).unwrap();
        settle(&pipeline, &client, &shard("2026-09-20", &[]), NOW).unwrap();

        assert_eq!(ids(&client, "people_20260920"), ["7", "8"]);
        assert_eq!(ids(&client, "pets_20260920"), ["9"]);
        assert_eq!(
            windows(settle(&pipeline, &client, &Course::CatchUp, NOW).unwrap()),
            ["window 2026-09-21T00:00:00+00:00 .. 2026-09-22T00:00:00+00:00"]
        );
    }

    #[test]
    fn a_shard_that_has_not_closed_is_not_built() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        let open = settle(&pipeline, &client, &shard("2026-09-22", &[]), NOW);

        assert!(matches!(open, Err(Unrunnable::Unclosed(_))));
    }

    #[test]
    fn a_shard_narrowed_to_some_tables_builds_only_them() {
        let pipeline = pipeline();
        let client = logged(
            BUILT_CHANGE_LOGS,
            &[
                built_before("people", CAPTURE),
                built_before("pets", CAPTURE),
            ],
        );

        settle(
            &pipeline,
            &client,
            &shard("2026-09-20", &[TableId::new("shop", "people")]),
            NOW,
        )
        .unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.pets_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
            ]
        );
    }

    #[test]
    fn a_table_created_within_a_shard_is_built_from_nothing_beside_the_ones_folded() {
        let pipeline = pipeline();
        let client = logged(PETS_CREATED_ON_20260920, &[built_before("people", CAPTURE)]);

        settle(&pipeline, &client, &shard("2026-09-20", &[]), NOW).unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
                folded("mysql_shop.pets_20260920", ""),
            ]
        );
    }

    #[test]
    fn a_table_renamed_within_a_window_folds_the_next_window_onto_what_it_was_built_into() {
        let pipeline = pipeline();
        let client = logged(
            PEOPLE_RENAMED_ON_20260920,
            &[built_before("people", CAPTURE)],
        );

        settle(&pipeline, &client, &Course::CatchUp, NOW).unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT concat_ws(' ', COALESCE(table_from, '-'), COALESCE(table_to, '-'), COALESCE(lifecycle, '-')), concat_ws(' <- ', target, COALESCE(base, '-')) FROM mysql_shop._lithify_log ORDER BY id"
            ),
            [
                folded(
                    "shop.people shop.people -",
                    "mysql_shop.people_20260919 <- -"
                ),
                folded(
                    "shop.people shop.persons renamed",
                    "mysql_shop.persons_20260920 <- mysql_shop.people_20260919"
                ),
                folded(
                    "shop.persons shop.persons -",
                    "mysql_shop.persons_20260921 <- mysql_shop.persons_20260920"
                ),
            ]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.persons_20260921 ORDER BY id"
            ),
            [
                ("7".to_owned(), "kept".to_owned()),
                ("8".to_owned(), "kept".to_owned()),
                ("9".to_owned(), "kept".to_owned())
            ]
        );
    }

    #[test]
    fn a_table_dropped_within_a_window_is_logged_as_dropped_and_built_no_more() {
        let pipeline = pipeline();
        let client = logged(
            PETS_DROPPED_ON_20260920,
            &[
                built_before("people", CAPTURE),
                built_before("pets", CAPTURE),
            ],
        );

        settle(&pipeline, &client, &Course::CatchUp, NOW).unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT concat_ws(' ', COALESCE(table_from, '-'), COALESCE(table_to, '-'), COALESCE(lifecycle, '-')), COALESCE(target, '-') FROM mysql_shop._lithify_log WHERE window_from >= '2026-09-20' ORDER BY id"
            ),
            [
                folded("shop.pets - dropped", "-"),
                folded("shop.people shop.people -", "mysql_shop.people_20260920"),
                folded("shop.people shop.people -", "mysql_shop.people_20260921"),
            ]
        );
        assert!(matches!(
            settle(
                &pipeline,
                &client,
                &shard("2026-09-21", &[TableId::new("shop", "pets")]),
                NOW
            ),
            Err(Unrunnable::UnknownTables(_))
        ));
        assert_eq!(
            Expectation::new(
                Schedule::Daily,
                pipeline.sources().next().unwrap().since(),
                CAPTURE,
                RecordedAt::from(at(NOW)),
            )
            .read(&client, "mysql_shop")
            .unwrap()
            .discrepancies(),
            []
        );
    }

    #[test]
    fn a_shard_dated_before_the_change_log_opens_is_not_built() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        let unsharded = settle(&pipeline, &client, &shard("1969-12-31", &[]), NOW);

        assert!(matches!(unsharded, Err(Unrunnable::Unsharded(_))));
    }

    #[test]
    fn a_shard_narrowed_to_a_table_it_does_not_hold_is_not_built() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        let unknown = settle(
            &pipeline,
            &client,
            &shard("2026-09-20", &[TableId::new("shop", "ghosts")]),
            NOW,
        );

        assert!(matches!(unknown, Err(Unrunnable::UnknownTables(_))));
        assert_eq!(built(&client), [folded("mysql_shop.people_20260919", "")]);
    }

    #[test]
    fn a_shard_rebuilt_behind_the_latest_makes_the_next_catch_up_rebuild_what_folded_onto_it() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(&pipeline, &client, &Course::CatchUp, NOW).unwrap();
        settle(&pipeline, &client, &shard("2026-09-20", &[]), NOW).unwrap();
        settle(&pipeline, &client, &Course::CatchUp, NOW).unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
                folded("mysql_shop.people_20260921", "mysql_shop.people_20260920"),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
                folded("mysql_shop.people_20260921", "mysql_shop.people_20260920"),
            ]
        );
    }
}
