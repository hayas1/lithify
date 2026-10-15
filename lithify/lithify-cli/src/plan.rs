use std::collections::{BTreeMap, BTreeSet};

use duckdb::types::FromSql;
use google_cloud_bigquery::error::RowError;
use google_cloud_bigquery::query::Row;

use lithify_config::infra::Sink;
use lithify_core::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use lithify_core::build_log::Latest;
use lithify_core::era::EraTracker;
use lithify_core::model::TableId;
use lithify_core::position::RecordedAt;
use lithify_core::sink::TablePath;
use lithify_core::sink::WarehouseClient;
use lithify_core::source::{ChangeLogs, RowChangeLog, SchemaChangeLog, SchemaChangeStream};
use lithify_core::window::Window;
use lithify_protocol::sink::bigquery::client::{BigQueryChangeLogsFn, BigQueryClient};
use lithify_protocol::sink::duckdb::client::{DuckDbChangeLogsFn, DuckDbClient};
use lithify_strategy::snapshot::{Snapshot, SnapshotBuilder};

use crate::artifacts::Artifacts;
use crate::pipeline::Warehouse;
use crate::settlement::Settlement;
use crate::unrunnable::Unrunnable;

type BuilderOf<'b, C, K> =
    SnapshotBuilder<'b, <C as ChangeLogs>::Row, <K as WarehouseClient>::Warehouse>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Base {
    #[default]
    Nothing,
    Recorded(BTreeMap<TableId, TablePath>),
    Shard(Window),
}

impl Base {
    pub fn recorded<K>(
        client: &K,
        namespace: &str,
        start: RecordedAt,
        since: RecordedAt,
    ) -> Result<Self, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        if start <= since {
            return Ok(Self::Nothing);
        }

        Ok(Self::Recorded(
            client
                .latest_build_logs(namespace, &Latest::SucceededUntil(start))?
                .into_iter()
                .filter_map(|log| Some((log.table_to()?.clone(), log.target()?.clone())))
                .collect(),
        ))
    }

    fn verify<L: RowChangeLog, W: lithify_core::sink::Warehouse>(
        &self,
        window: &Window,
        builders: &[SnapshotBuilder<'_, L, W>],
    ) -> Result<(), Unrunnable> {
        let tables: Vec<_> = match self {
            Self::Nothing | Self::Shard(_) => Vec::new(),
            Self::Recorded(_) => builders
                .iter()
                .filter_map(SnapshotBuilder::missing_base)
                .cloned()
                .collect(),
        };

        match tables.is_empty() {
            true => Ok(()),
            false => Err(Unrunnable::Baseless {
                window: window.clone(),
                tables,
            }),
        }
    }

    fn of(
        &self,
        snapshot: &Snapshot,
        table: &TableId,
    ) -> Result<Option<TablePath>, UnbuildableArtifact> {
        match self {
            Self::Nothing => Ok(None),
            Self::Recorded(targets) => Ok(targets.get(table).cloned()),
            Self::Shard(window) => snapshot.target(table, window).map(Some),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Tables {
    #[default]
    Every,
    Named(BTreeSet<TableId>),
}

impl Tables {
    fn narrow<B: ArtifactBuilder>(&self, mut builders: Vec<B>) -> Result<Vec<B>, Unrunnable> {
        match self {
            Self::Every => Ok(builders),
            Self::Named(tables) => {
                let unknown: Vec<_> = tables
                    .iter()
                    .filter(|table| !builders.iter().any(|builder| builder.table() == *table))
                    .cloned()
                    .collect();
                if !unknown.is_empty() {
                    return Err(Unrunnable::UnknownTables(unknown));
                }
                builders.retain(|builder| tables.contains(builder.table()));

                Ok(builders)
            }
        }
    }
}

pub struct SnapshotPlan<'a, K, S> {
    client: &'a K,
    sink: &'a Sink<Warehouse>,
    window: &'a Window,
    base: &'a Base,
    tables: &'a Tables,
    settlement: &'a S,
}

impl<'a, K, S: Settlement> SnapshotPlan<'a, K, S> {
    pub fn new(
        client: &'a K,
        sink: &'a Sink<Warehouse>,
        window: &'a Window,
        base: &'a Base,
        tables: &'a Tables,
        settlement: &'a S,
    ) -> Self {
        Self {
            client,
            sink,
            window,
            base,
            tables,
            settlement,
        }
    }

    fn settle<C>(self, logs: C) -> Result<S::Output, Unrunnable>
    where
        C: ChangeLogs,
        K: SchemaChangeStream<C::Schema> + WarehouseClient,
        <K as SchemaChangeStream<C::Schema>>::Error: Into<Unrunnable>,
        Unrunnable: From<<K as WarehouseClient>::Error>,
    {
        let snapshot = self.sink.snapshot();
        let builders = self.tables.narrow(self.builders(&snapshot, &logs)?)?;
        self.base.verify(self.window, &builders)?;

        self.settlement.settle(self.client, &builders)
    }

    fn builders<'b, C>(
        &self,
        snapshot: &'b Snapshot,
        logs: &'b C,
    ) -> Result<Vec<BuilderOf<'b, C, K>>, Unrunnable>
    where
        'a: 'b,
        C: ChangeLogs,
        K: SchemaChangeStream<C::Schema> + WarehouseClient,
        <K as SchemaChangeStream<C::Schema>>::Error: Into<Unrunnable>,
    {
        let window: &'b Window = self.window;
        let mut tracker = EraTracker::new(window.clone(), |table: &TableId| {
            snapshot.builder::<_, K::Warehouse>(
                logs.row_changes(),
                table.clone(),
                window,
                self.base.of(snapshot, table)?,
            )
        });
        let catalog = self.client.replay::<Unrunnable, _>(
            self.client.warehouse(),
            logs.schema_changes(),
            window,
            &mut tracker,
        )?;

        Ok(tracker.finish(self.client.warehouse(), &catalog)?)
    }
}

impl<S: Settlement> DuckDbChangeLogsFn for SnapshotPlan<'_, DuckDbClient, S> {
    type Output = Result<S::Output, Unrunnable>;

    fn call_duckdb<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: FromSql,
    {
        self.settle(logs)
    }
}

impl<S: Settlement> BigQueryChangeLogsFn for SnapshotPlan<'_, BigQueryClient, S> {
    type Output = Result<S::Output, Unrunnable>;

    fn call_bigquery<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: TryFrom<Row, Error = RowError>,
    {
        self.settle(logs)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::path::Path;

    use duckdb::Connection;

    use lithify_config::Pipeline;
    use lithify_core::position::RecordedAt;
    use lithify_core::sink::WarehouseClient;
    use lithify_protocol::DuckDbWarehouse;
    use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbClient};

    use chrono::{DateTime, Utc};
    use ulid::Ulid;

    use lithify_core::sql::SqlString;

    use super::*;
    use crate::pipeline::Capture;
    use crate::settlement::{Build, DryRun};
    use lithify_core::build_log::{BuildLog, Latest, Outcome, Run, Status};

    const CHANGE_LOGS: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value)";

    pub(crate) fn pipeline() -> Pipeline<Capture, Warehouse> {
        Pipeline::open(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql"))
            .unwrap()
    }

    #[test]
    fn a_plan_holds_the_snapshot_of_every_table_the_schema_changes_left_standing() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "rustfs")
            .unwrap();
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new(sink.change_log()),
        );
        client.execute(&SqlString::from(CHANGE_LOGS)).unwrap();
        let window = Window::new(
            RecordedAt::from(1789862400000),
            RecordedAt::from(1789948800000),
        );

        let statements = source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                SnapshotPlan::new(
                    &client,
                    sink,
                    &window,
                    &Base::default(),
                    &Tables::Every,
                    &DryRun::new(Path::new("plans"), "rustfs"),
                ),
            )
            .unwrap();

        assert_eq!(statements[0], "CREATE SCHEMA IF NOT EXISTS \"mysql_shop\"");
        assert!(
            statements[1]
                .as_str()
                .starts_with("CREATE OR REPLACE TABLE \"mysql_shop\".\"people_20260920\" AS")
        );
        assert!(
            statements[1]
                .as_str()
                .contains("FROM \"mysql_shop_cdc\".\"mysql.shop.people\"")
        );
        assert_eq!(statements.len(), 2);
    }

    #[test]
    fn a_plan_onto_an_earlier_window_folds_onto_the_snapshot_it_left() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "rustfs")
            .unwrap();
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new(sink.change_log()),
        );
        client.execute(&SqlString::from(CHANGE_LOGS)).unwrap();
        let window = Window::new(
            RecordedAt::from(1789862400000),
            RecordedAt::from(1789948800000),
        );
        let onto = DateTime::parse_from_rfc3339("2026-09-19T00:00:00Z")
            .unwrap()
            .to_utc();

        let statements = source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                SnapshotPlan::new(
                    &client,
                    sink,
                    &window,
                    &Base::Shard(Window::new(RecordedAt::from(onto), *window.from())),
                    &Tables::Every,
                    &DryRun::new(Path::new("plans"), "rustfs"),
                ),
            )
            .unwrap();

        assert!(
            statements[1]
                .as_str()
                .contains("  FROM \"mysql_shop\".\"people_20260919\"\n")
        );
    }

    #[test]
    fn a_plan_folds_each_table_onto_the_shard_the_build_log_recorded_for_it() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "rustfs")
            .unwrap();
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new(sink.change_log()),
        );
        client.execute(&SqlString::from(CHANGE_LOGS)).unwrap();
        let window = Window::new(
            RecordedAt::from(1789862400000),
            RecordedAt::from(1789948800000),
        );
        let recorded = Base::Recorded(BTreeMap::from([(
            TableId::new("shop", "people"),
            TablePath::new("mysql_shop", "people_recorded"),
        )]));

        let statements = source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                SnapshotPlan::new(
                    &client,
                    sink,
                    &window,
                    &recorded,
                    &Tables::Every,
                    &DryRun::new(Path::new("plans"), "rustfs"),
                ),
            )
            .unwrap();

        assert!(
            statements[1]
                .as_str()
                .contains("  FROM \"mysql_shop\".\"people_recorded\"\n")
        );
    }

    pub(crate) const CAPTURE: &str = r#"{"debezium":{"dialect":"mysql"}}"#;

    pub(crate) fn at(rfc3339: &str) -> DateTime<Utc> {
        rfc3339.parse().unwrap()
    }

    pub(crate) fn built_before(table: &str, capture: &str) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::from_parts(1, 0),
                "snapshot",
                "daily",
                "primary",
                capture,
                "0.1.0",
            ),
            Some(TableId::new("shop", table)),
            Some(TableId::new("shop", table)),
            None,
            Some(TablePath::new("mysql_shop", &format!("{table}_20260919"))),
            Window::new(
                RecordedAt::from(at("2026-09-19T00:00:00Z")),
                RecordedAt::from(at("2026-09-20T00:00:00Z")),
            ),
            Outcome::new(Status::Succeeded, at("2026-09-20T00:10:00Z")),
        )
    }

    pub(crate) fn logged(change_logs: &str, before: &[BuildLog]) -> DuckDbClient {
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
            client.append_build_log("mysql_shop", log).unwrap();
        }

        client
    }

    pub(crate) const PEOPLE: &str = "CREATE SCHEMA mysql_shop_cdc;
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

    pub(crate) const BUILT_CHANGE_LOGS: &str = "CREATE SCHEMA mysql_shop_cdc;
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
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'r', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 1::BIGINT}}),
          ({'op': 'r', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 2::BIGINT}})
        ) AS t(value)";

    pub(crate) fn rows(client: &DuckDbClient, query: &str) -> Vec<(String, String)> {
        client
            .connection()
            .prepare(query)
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn built() -> (Pipeline<Capture, Warehouse>, DuckDbClient) {
        let pipeline = pipeline();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "rustfs")
            .unwrap();
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new(sink.change_log()),
        );
        client.execute(&SqlString::from(BUILT_CHANGE_LOGS)).unwrap();

        (pipeline, client)
    }

    fn build(pipeline: &Pipeline<Capture, Warehouse>, client: &DuckDbClient) -> Vec<BuildLog> {
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "rustfs")
            .unwrap();
        let window = Window::new(
            RecordedAt::from(1789862400000),
            RecordedAt::from(1789948800000),
        );

        source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                SnapshotPlan::new(
                    client,
                    sink,
                    &window,
                    &Base::default(),
                    &Tables::Every,
                    &Build::new(sink, source).unwrap(),
                ),
            )
            .unwrap()
    }

    #[test]
    fn a_build_settles_each_table_and_logs_it_once_whether_or_not_it_was_built() {
        let (pipeline, client) = built();

        let logs = build(&pipeline, &client);

        assert_eq!(logs.len(), 2);
        assert!(logs[1].to_string().contains("mysql.shop.pets"));
        assert_eq!(
            rows(
                &client,
                "SELECT table_to, status FROM mysql_shop._lithify_log ORDER BY table_to"
            ),
            [
                ("shop.people".to_owned(), "succeeded".to_owned()),
                ("shop.pets".to_owned(), "failed".to_owned()),
            ]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260920 ORDER BY id"
            ),
            [
                ("1".to_owned(), "kept".to_owned()),
                ("2".to_owned(), "kept".to_owned())
            ]
        );
    }

    #[test]
    fn a_build_logs_the_run_the_capture_and_the_schedule_it_was_built_under() {
        let (pipeline, client) = built();

        build(&pipeline, &client);

        assert_eq!(
            rows(
                &client,
                "SELECT DISTINCT capture, schedule || ' ' || artifact || ' ' || source FROM mysql_shop._lithify_log"
            ),
            [(
                r#"{"debezium":{"dialect":"mysql"}}"#.to_owned(),
                "daily snapshot primary".to_owned()
            )]
        );
    }

    #[test]
    fn a_window_built_twice_settles_into_the_same_snapshot_and_reads_as_built_once() {
        let (pipeline, client) = built();
        let snapshot = "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260920 ORDER BY id";

        let once = build(&pipeline, &client);
        let built_once = rows(&client, snapshot);
        let twice = build(&pipeline, &client);

        assert_eq!(rows(&client, snapshot), built_once);
        assert_ne!(once[0].run_id(), twice[0].run_id());
        let latest = client
            .latest_build_logs("mysql_shop", &Latest::Attempted)
            .unwrap();
        assert_eq!(
            latest
                .iter()
                .map(|log| (log.table_to().unwrap().to_string(), log.run_id()))
                .collect::<Vec<_>>(),
            [
                ("shop.people".to_owned(), twice[0].run_id()),
                ("shop.pets".to_owned(), twice[0].run_id()),
            ]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT COUNT(*)::VARCHAR, 'rows' FROM mysql_shop._lithify_log"
            ),
            [("4".to_owned(), "rows".to_owned())]
        );
    }
}
