use chrono::{DateTime, Utc};
use duckdb::types::FromSql;
use google_cloud_bigquery::error::RowError;
use google_cloud_bigquery::query::Row;

use lithify_config::infra::Sink;
use lithify_core::artifact::{Artifact, UnbuildableArtifact};
use lithify_core::era::EraTracker;
use lithify_core::model::TableId;
use lithify_core::position::RecordedAt;
use lithify_core::sink::TablePath;
use lithify_core::sink::WarehouseClient;
use lithify_core::source::{ChangeLogs, SchemaChangeLog, SchemaChangeStream};
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

pub struct SnapshotPlan<'a, K, S> {
    client: &'a K,
    sink: &'a Sink<Warehouse>,
    window: &'a Window,
    onto: Option<DateTime<Utc>>,
    settlement: S,
}

impl<'a, K, S: Settlement> SnapshotPlan<'a, K, S> {
    pub fn new(
        client: &'a K,
        sink: &'a Sink<Warehouse>,
        window: &'a Window,
        onto: Option<DateTime<Utc>>,
        settlement: S,
    ) -> Self {
        Self {
            client,
            sink,
            window,
            onto,
            settlement,
        }
    }

    fn base(
        &self,
        snapshot: &Snapshot,
        table: &TableId,
    ) -> Result<Option<TablePath>, UnbuildableArtifact> {
        self.onto
            .map(|onto| {
                snapshot.target(
                    table,
                    &Window::new(RecordedAt::from(onto), *self.window.from()),
                )
            })
            .transpose()
    }

    fn settle<C>(self, logs: C) -> Result<S::Output, Unrunnable>
    where
        C: ChangeLogs,
        K: SchemaChangeStream<C::Schema> + WarehouseClient,
        <K as SchemaChangeStream<C::Schema>>::Error: Into<Unrunnable>,
        Unrunnable: From<<K as WarehouseClient>::Error>,
    {
        let snapshot = self.sink.snapshot();
        let builders = self.builders(&snapshot, &logs)?;

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
                self.base(snapshot, table)?,
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
mod tests {
    use std::path::Path;

    use duckdb::Connection;

    use lithify_config::Pipeline;
    use lithify_core::position::RecordedAt;
    use lithify_core::sink::WarehouseClient;
    use lithify_protocol::DuckDbWarehouse;
    use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbClient};

    use lithify_core::sql::SqlString;

    use super::*;
    use crate::pipeline::Capture;
    use crate::settlement::{Build, DryRun};
    use lithify_core::build_log::BuildLog;
    use lithify_core::sink::Warehouse as _;

    const CHANGE_LOGS: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value)";

    fn pipeline() -> Pipeline<Capture, Warehouse> {
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
                SnapshotPlan::new(&client, sink, &window, None, DryRun),
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
                SnapshotPlan::new(&client, sink, &window, Some(onto), DryRun),
            )
            .unwrap();

        assert!(
            statements[1]
                .as_str()
                .contains("  FROM \"mysql_shop\".\"people_20260919\"\n")
        );
    }

    const BUILT_CHANGE_LOGS: &str = "CREATE SCHEMA mysql_shop_cdc;
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

    fn rows(client: &DuckDbClient, query: &str) -> Vec<(String, String)> {
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
                    None,
                    Build::new(sink, source).unwrap(),
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
                "SELECT table_id, status FROM mysql_shop._lithify_log ORDER BY table_id"
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
                "SELECT DISTINCT capture, schedule || ' ' || artifact || ' ' || source || ' ' || run_id FROM mysql_shop._lithify_log"
            ),
            [(
                r#"{"debezium":{"dialect":"mysql"}}"#.to_owned(),
                "daily snapshot primary 1".to_owned()
            )]
        );
    }

    #[test]
    fn a_window_built_twice_settles_into_the_same_snapshot_and_reads_as_built_once() {
        let (pipeline, client) = built();
        let snapshot = "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260920 ORDER BY id";

        build(&pipeline, &client);
        let once = rows(&client, snapshot);
        build(&pipeline, &client);

        assert_eq!(rows(&client, snapshot), once);
        assert_eq!(
            rows(
                &client,
                &format!(
                    "SELECT table_id, run_id::VARCHAR FROM ({})",
                    client.warehouse().select_latest_build_logs("mysql_shop")
                )
            ),
            [
                ("shop.people".to_owned(), "2".to_owned()),
                ("shop.pets".to_owned(), "2".to_owned()),
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
