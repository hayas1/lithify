use duckdb::types::FromSql;
use google_cloud_bigquery::error::RowError;
use google_cloud_bigquery::query::Row;

use lithify_config::table;
use lithify_core::artifact::Artifact;
use lithify_core::build_log::BuildLog;
use lithify_core::runner::{Planner, Replay, Runner, Tables};
use lithify_core::source::{ChangeLogs, SchemaChangeLog};
use lithify_core::window::Window;
use lithify_protocol::sink::bigquery::client::{
    BigQueryCapture, BigQueryChangeLogsFn, BigQueryClient,
};
use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbChangeLogsFn, DuckDbClient};

use crate::pipeline::CaptureConfig;
use crate::unrunnable::Unrunnable;

pub struct Plan<'a, K, R, A> {
    client: &'a K,
    artifact: &'a A,
    window: &'a Window,
    previous: &'a [BuildLog],
    tables: &'a Tables,
    listing: &'a table::Tables,
    runner: &'a R,
}

impl<'a, K, R: Runner, A: Artifact> Plan<'a, K, R, A> {
    pub fn new(
        client: &'a K,
        artifact: &'a A,
        window: &'a Window,
        previous: &'a [BuildLog],
        tables: &'a Tables,
        listing: &'a table::Tables,
        runner: &'a R,
    ) -> Self {
        Self {
            client,
            artifact,
            window,
            previous,
            tables,
            listing,
            runner,
        }
    }
}

pub struct Planning<'a, K> {
    client: &'a K,
    capture: &'a CaptureConfig,
    topic_prefix: &'a str,
    listing: &'a table::Tables,
}

impl<'a, K> Planning<'a, K> {
    pub fn new(
        client: &'a K,
        capture: &'a CaptureConfig,
        topic_prefix: &'a str,
        listing: &'a table::Tables,
    ) -> Self {
        Self {
            client,
            capture,
            topic_prefix,
            listing,
        }
    }
}

impl Planner for Planning<'_, DuckDbClient> {
    type Client = DuckDbClient;
    type Error = Unrunnable;

    fn client(&self) -> &DuckDbClient {
        self.client
    }

    fn plan<A: Artifact, R: Runner>(
        &self,
        artifact: &A,
        window: &Window,
        previous: &[BuildLog],
        tables: &Tables,
        runner: &R,
    ) -> Result<R::Output, Unrunnable> {
        self.capture.with_duckdb_change_logs(
            self.topic_prefix,
            Plan::new(
                self.client,
                artifact,
                window,
                previous,
                tables,
                self.listing,
                runner,
            ),
        )
    }
}

impl Planner for Planning<'_, BigQueryClient> {
    type Client = BigQueryClient;
    type Error = Unrunnable;

    fn client(&self) -> &BigQueryClient {
        self.client
    }

    fn plan<A: Artifact, R: Runner>(
        &self,
        artifact: &A,
        window: &Window,
        previous: &[BuildLog],
        tables: &Tables,
        runner: &R,
    ) -> Result<R::Output, Unrunnable> {
        self.capture.with_bigquery_change_logs(
            self.topic_prefix,
            Plan::new(
                self.client,
                artifact,
                window,
                previous,
                tables,
                self.listing,
                runner,
            ),
        )
    }
}

impl<R: Runner, A: Artifact> DuckDbChangeLogsFn for Plan<'_, DuckDbClient, R, A> {
    type Output = Result<R::Output, Unrunnable>;

    fn call_duckdb<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: FromSql,
    {
        Replay::new(self.client, &logs).run(
            self.artifact,
            self.window,
            self.previous,
            self.tables,
            |table| self.listing.table(table).builds(A::KIND),
            self.runner,
        )
    }
}

impl<R: Runner, A: Artifact> BigQueryChangeLogsFn for Plan<'_, BigQueryClient, R, A> {
    type Output = Result<R::Output, Unrunnable>;

    fn call_bigquery<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: TryFrom<Row, Error = RowError>,
    {
        Replay::new(self.client, &logs).run(
            self.artifact,
            self.window,
            self.previous,
            self.tables,
            |table| self.listing.table(table).builds(A::KIND),
            self.runner,
        )
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::path::Path;

    use duckdb::Connection;

    use lithify_config::Pipeline;
    use lithify_core::sink::{Namespace, TablePath, WarehouseClient};
    use lithify_core::ts::Ts;
    use lithify_protocol::DuckDbWarehouse;
    use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbClient};

    use chrono::{DateTime, Utc};
    use ulid::Ulid;

    use lithify_core::sql::SqlString;

    use super::*;
    use crate::artifacts::{ArtifactConfig, DeclaredSnapshot};
    use crate::pipeline::{CaptureConfig, WarehouseConfig};
    use crate::report::DryRun;
    use lithify_artifact::snapshot::Snapshot;
    use lithify_config::infra::Sink;
    use lithify_core::build_log::{BuildLog, Latest, Outcome, Run, Status};
    use lithify_core::model::TableId;
    use lithify_core::runner::{Tables, Unsteppable};

    const CHANGE_LOGS: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value)";

    fn snapshot(sink: &Sink<WarehouseConfig, ArtifactConfig>) -> Snapshot {
        DeclaredSnapshot::of(sink).unwrap().artifact()
    }

    pub(crate) const INFRA: &str = r#"
[artifact.snapshot]
shard = "%Y%m%d"

[default]
source.capture.debezium.dialect = "mysql"

[[source]]
name = "primary"
since = "1970-01-01T00:00:00Z"
topic_prefix = "mysql"

[[sink]]
name = "bigquery"

[sink.warehouse.bigquery]
project = "local-project"
change_log = { dataset = "mysql_shop_cdc" }
place.snapshot = [{ dataset = "mysql_shop" }]

[[sink]]
name = "duckdb_s3"

[sink.warehouse.duckdb]
s3 = { endpoint = "http://rustfs:9000", region = "us-east-1", bucket = "lithify", format = "parquet" }
change_log = { prefix = "mysql_shop_cdc" }
place.snapshot = [{ prefix = "mysql_shop" }]
"#;

    pub(crate) fn opened(
        name: &str,
        infra: &str,
        listed: &str,
    ) -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        let directory =
            std::env::temp_dir().join(format!("lithify-cli-{name}-{}", Ulid::generate()));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("infra.toml"), infra).unwrap();
        std::fs::write(directory.join("table.toml"), listed).unwrap();
        let pipeline = Pipeline::open(&directory).unwrap();
        std::fs::remove_dir_all(&directory).unwrap();

        pipeline
    }

    pub(crate) fn pipeline() -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        opened("pipeline", INFRA, "")
    }

    #[test]
    fn a_plan_holds_the_snapshot_of_every_table_the_schema_changes_left_standing() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new(sink.change_log().clone()),
        );
        client.execute(&SqlString::from(CHANGE_LOGS)).unwrap();
        let window = Window::new(
            Ts::try_from(0).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );

        let statements = source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                Plan::new(
                    &client,
                    &snapshot(sink),
                    &window,
                    &[],
                    &Tables::Every,
                    pipeline.tables(),
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
    fn a_plan_nothing_was_built_before_finds_the_tables_standing_when_it_opens_baseless() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new(sink.change_log().clone()),
        );
        client.execute(&SqlString::from(CHANGE_LOGS)).unwrap();
        let window = Window::new(
            Ts::try_from(1789862400000).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );

        let baseless = source.capture().with_duckdb_change_logs(
            source.topic_prefix().unwrap(),
            Plan::new(
                &client,
                &snapshot(sink),
                &window,
                &[],
                &Tables::Every,
                pipeline.tables(),
                &DryRun::new(Path::new("plans"), "rustfs"),
            ),
        );

        assert!(matches!(
            baseless,
            Err(Unrunnable::Unsteppable(Unsteppable::Baseless { tables, .. })) if tables == [TableId::new("shop", "people")]
        ));
    }

    #[test]
    fn a_plan_folds_each_table_onto_the_shard_its_last_build_log_targeted() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new(sink.change_log().clone()),
        );
        client.execute(&SqlString::from(CHANGE_LOGS)).unwrap();
        let window = Window::new(
            Ts::try_from(1789862400000).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );
        let previous = [built_before("people", CAPTURE)];

        let statements = source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                Plan::new(
                    &client,
                    &snapshot(sink),
                    &window,
                    &previous,
                    &Tables::Every,
                    pipeline.tables(),
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

    pub(crate) const CAPTURE: &str = r#"{"debezium":{"dialect":"mysql"}}"#;

    pub(crate) fn at(rfc3339: &str) -> DateTime<Utc> {
        rfc3339.parse().unwrap()
    }

    pub(crate) fn built_before(table: &str, capture: &str) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::from_parts(1, 0),
                DateTime::UNIX_EPOCH,
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
                Ts::from(at("2026-09-19T00:00:00Z")),
                Ts::from(at("2026-09-20T00:00:00Z")),
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
        client
            .create_build_log::<Unrunnable>(&Namespace::from("mysql_shop"))
            .unwrap();
        for log in before {
            client
                .append_build_log(&Namespace::from("mysql_shop"), log)
                .unwrap();
        }

        client
    }

    pub(crate) const PEOPLE: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789948800001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789948800001::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value)";

    pub(crate) const BUILT_CHANGE_LOGS: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE pets (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"pets\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'r', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400001::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 1::BIGINT}}),
          ({'op': 'r', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400001::BIGINT},
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

    fn built() -> (
        Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
        DuckDbClient,
    ) {
        let pipeline = pipeline();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new(sink.change_log().clone()),
        );
        client.execute(&SqlString::from(BUILT_CHANGE_LOGS)).unwrap();

        (pipeline, client)
    }

    fn build(
        pipeline: &Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
        client: &DuckDbClient,
    ) -> Vec<BuildLog> {
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let window = Window::new(
            Ts::try_from(0).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );

        source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                Plan::new(
                    client,
                    &snapshot(sink),
                    &window,
                    &[],
                    &Tables::Every,
                    pipeline.tables(),
                    &DeclaredSnapshot::of(sink)
                        .unwrap()
                        .build(source, at("2026-09-22T05:00:00Z"))
                        .unwrap(),
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
            .build_logs::<Unrunnable>(
                &Namespace::from("mysql_shop"),
                &Snapshot::days(),
                &Latest::Attempted.into(),
            )
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
