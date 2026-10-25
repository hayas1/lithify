use duckdb::types::FromSql;
use google_cloud_bigquery::error::RowError;
use google_cloud_bigquery::query::Row;

use lithify_config::table;
use lithify_core::artifact::Artifact;
use lithify_core::build_log::BuildLog;
use lithify_core::runner::{BuildLogWriter, Replayer, StepTaker, Tables};
use lithify_core::source::{ChangeLogs, SchemaChangeLog};
use lithify_core::window::Window;
use lithify_protocol::sink::bigquery::client::{
    BigQueryCapture, BigQueryChangeLogsFn, BigQueryClient, BigQueryExecution,
};
use lithify_protocol::sink::duckdb::client::{
    DuckDbCapture, DuckDbChangeLogsFn, DuckDbClient, DuckDbExecution,
};

use crate::pipeline::CaptureConfig;
use crate::unrunnable::Unrunnable;

pub struct Replaying<'a, K, B, A> {
    client: &'a K,
    artifact: &'a A,
    window: &'a Window,
    previous: &'a [BuildLog],
    tables: &'a Tables,
    listing: &'a table::Tables,
    writer: &'a B,
}

impl<'a, K, B: BuildLogWriter, A: Artifact> Replaying<'a, K, B, A> {
    pub fn new(
        client: &'a K,
        artifact: &'a A,
        window: &'a Window,
        previous: &'a [BuildLog],
        tables: &'a Tables,
        listing: &'a table::Tables,
        writer: &'a B,
    ) -> Self {
        Self {
            client,
            artifact,
            window,
            previous,
            tables,
            listing,
            writer,
        }
    }
}

pub struct Taking<'a, K> {
    client: &'a K,
    capture: &'a CaptureConfig,
    topic_prefix: &'a str,
    listing: &'a table::Tables,
}

impl<'a, K> Taking<'a, K> {
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

impl<X: DuckDbExecution> StepTaker for Taking<'_, DuckDbClient<X>> {
    type Client = DuckDbClient<X>;
    type Error = Unrunnable;

    fn client(&self) -> &DuckDbClient<X> {
        self.client
    }

    fn take<A: Artifact, B: BuildLogWriter>(
        &self,
        artifact: &A,
        window: &Window,
        previous: &[BuildLog],
        tables: &Tables,
        writer: &B,
    ) -> Result<B::Output, Unrunnable> {
        self.capture.with_duckdb_change_logs(
            self.topic_prefix,
            Replaying::new(
                self.client,
                artifact,
                window,
                previous,
                tables,
                self.listing,
                writer,
            ),
        )
    }
}

impl<X: BigQueryExecution> StepTaker for Taking<'_, BigQueryClient<X>> {
    type Client = BigQueryClient<X>;
    type Error = Unrunnable;

    fn client(&self) -> &BigQueryClient<X> {
        self.client
    }

    fn take<A: Artifact, B: BuildLogWriter>(
        &self,
        artifact: &A,
        window: &Window,
        previous: &[BuildLog],
        tables: &Tables,
        writer: &B,
    ) -> Result<B::Output, Unrunnable> {
        self.capture.with_bigquery_change_logs(
            self.topic_prefix,
            Replaying::new(
                self.client,
                artifact,
                window,
                previous,
                tables,
                self.listing,
                writer,
            ),
        )
    }
}

impl<X: DuckDbExecution, B: BuildLogWriter, A: Artifact> DuckDbChangeLogsFn
    for Replaying<'_, DuckDbClient<X>, B, A>
{
    type Output = Result<B::Output, Unrunnable>;

    fn call_duckdb<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: FromSql,
    {
        Replayer::new(self.client, &logs).run(
            self.artifact,
            self.window,
            self.previous,
            self.tables,
            |table| self.listing.table(table).builds(A::KIND),
            self.writer,
        )
    }
}

impl<X: BigQueryExecution, B: BuildLogWriter, A: Artifact> BigQueryChangeLogsFn
    for Replaying<'_, BigQueryClient<X>, B, A>
{
    type Output = Result<B::Output, Unrunnable>;

    fn call_bigquery<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: TryFrom<Row, Error = RowError>,
    {
        Replayer::new(self.client, &logs).run(
            self.artifact,
            self.window,
            self.previous,
            self.tables,
            |table| self.listing.table(table).builds(A::KIND),
            self.writer,
        )
    }
}

#[cfg(test)]
pub(crate) mod tests {
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
    use lithify_artifact::snapshot::Snapshot;
    use lithify_config::infra::Sink;
    use lithify_core::artifact::UnbuildableArtifact;
    use lithify_core::build_log::{BuildLog, Latest, Outcome, Run, Status};
    use lithify_core::model::TableId;
    use lithify_core::runner::{BuildLogWrite, Tables};
    use lithify_protocol::sink::execution::DryRun;

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

    fn dry_run(sink: &Sink<WarehouseConfig, ArtifactConfig>) -> DuckDbClient<DryRun> {
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new(sink.change_log().clone()),
        );
        client.execute(&SqlString::from(CHANGE_LOGS)).unwrap();

        client.executing(DryRun::default())
    }

    fn snapshot_build() -> BuildLogWrite {
        BuildLogWrite::new(
            Namespace::from("mysql_shop"),
            Run::new(
                Ulid::nil(),
                Ts::from(DateTime::UNIX_EPOCH),
                "snapshot",
                "daily",
                "primary",
                CAPTURE,
                "0.1.0",
            ),
        )
    }

    fn statements(client: &DuckDbClient<DryRun>, starting: &str) -> Vec<String> {
        client
            .execution()
            .take()
            .iter()
            .map(SqlString::to_string)
            .filter(|statement| statement.starts_with(starting))
            .collect()
    }

    #[test]
    fn a_dry_run_records_the_snapshot_of_every_table_the_schema_changes_left_standing() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let client = dry_run(sink);
        let window = Window::new(
            Ts::try_from(0).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );

        source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                Replaying::new(
                    &client,
                    &snapshot(sink),
                    &window,
                    &[],
                    &Tables::Every,
                    pipeline.tables(),
                    &snapshot_build(),
                ),
            )
            .unwrap();
        let created = statements(&client, "CREATE OR REPLACE TABLE \"mysql_shop\".\"people_");

        assert_eq!(created.len(), 1);
        assert!(
            created[0].starts_with("CREATE OR REPLACE TABLE \"mysql_shop\".\"people_20260920\" AS")
        );
        assert!(created[0].contains("FROM \"mysql_shop_cdc\".\"mysql.shop.people\""));
        assert!(
            !client
                .exists(&TablePath::new("mysql_shop", "people_20260920"))
                .unwrap()
        );
    }

    #[test]
    fn a_dry_run_records_the_build_log_it_would_write_without_writing_it() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let client = dry_run(sink);
        let window = Window::new(
            Ts::try_from(0).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );

        source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                Replaying::new(
                    &client,
                    &snapshot(sink),
                    &window,
                    &[],
                    &Tables::Every,
                    pipeline.tables(),
                    &snapshot_build(),
                ),
            )
            .unwrap();

        assert_eq!(
            statements(&client, "INSERT INTO \"mysql_shop\".\"_lithify_log\"").len(),
            1
        );
        assert!(
            !client
                .exists(&BuildLog::path(&Namespace::from("mysql_shop")))
                .unwrap()
        );
    }

    #[test]
    fn a_dry_run_logs_a_table_with_nothing_to_fold_onto_as_failed_as_a_build_does() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let client = dry_run(sink);
        let window = Window::new(
            Ts::try_from(1789862400000).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );

        let logs = source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                Replaying::new(
                    &client,
                    &snapshot(sink),
                    &window,
                    &[],
                    &Tables::Every,
                    pipeline.tables(),
                    &snapshot_build(),
                ),
            )
            .unwrap();

        assert_eq!(
            logs.iter().map(BuildLog::status).collect::<Vec<_>>(),
            [&Status::Failed(
                UnbuildableArtifact::WithoutBase(TableId::new("shop", "people")).to_string()
            )]
        );
    }

    #[test]
    fn a_dry_run_folds_each_table_onto_the_shard_its_last_build_log_targeted() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let client = dry_run(sink);
        let window = Window::new(
            Ts::try_from(1789862400000).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );
        let previous = [built_before("people", CAPTURE)];

        source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                Replaying::new(
                    &client,
                    &snapshot(sink),
                    &window,
                    &previous,
                    &Tables::Every,
                    pipeline.tables(),
                    &snapshot_build(),
                ),
            )
            .unwrap();

        assert!(
            statements(&client, "CREATE OR REPLACE TABLE \"mysql_shop\".\"people_")[0]
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
                Ts::from(DateTime::UNIX_EPOCH),
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
            Outcome::new(Status::Succeeded, Ts::from(at("2026-09-20T00:10:00Z"))),
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
        client
            .append_build_logs(&Namespace::from("mysql_shop"), before)
            .unwrap();

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
                Replaying::new(
                    client,
                    &snapshot(sink),
                    &window,
                    &[],
                    &Tables::Every,
                    pipeline.tables(),
                    &DeclaredSnapshot::of(sink)
                        .unwrap()
                        .build(source, Ts::from(at("2026-09-22T05:00:00Z")))
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
