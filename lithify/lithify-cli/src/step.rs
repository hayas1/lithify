#[cfg(test)]
pub(crate) mod tests {
    use duckdb::Connection;

    use lithify_core::sink::{Namespace, TablePath, WarehouseClient};
    use lithify_core::ts::Ts;
    use lithify_core::window::Window;
    use lithify_pipeline::Pipeline;
    use lithify_pipeline::artifact::ArtifactControl;
    use lithify_pipeline::capture::Capture;
    use lithify_pipeline::runner::{BuildLogWriter, Replaying};
    use lithify_pipeline::warehouse::WarehouseControl;
    use lithify_protocol::DuckDbWarehouse;
    use lithify_protocol::sink::duckdb::client::DuckDbClient;

    use chrono::{DateTime, Utc};
    use ulid::Ulid;

    use lithify_core::sql::SqlString;

    use crate::artifacts::ArtifactConfig;
    use crate::pipeline::{CaptureConfig, WarehouseConfig};
    use crate::unrunnable::Unrunnable;
    use lithify_artifact::snapshot::Snapshot;
    use lithify_core::artifact::UnbuildableArtifact;
    use lithify_core::model::TableId;
    use lithify_pipeline::build_log::{BuildLog, Latest, Run, Series, Status, TableBuilt};
    use lithify_pipeline::infra::Sink;
    use lithify_pipeline::runner::BuildLogWrite;
    use lithify_pipeline::table;
    use lithify_protocol::sink::execution::DryRun;

    const CHANGE_LOGS: &str = include_str!("../testdata/step/change_logs.sql");

    fn snapshot(sink: &Sink<WarehouseConfig, ArtifactConfig>) -> Snapshot {
        ArtifactConfig::snapshot(sink, sink.scheduled().series().artifact()).unwrap()
    }

    async fn replayed<K, B>(
        pipeline: &Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
        client: &K,
        window: &Window,
        previous: &[BuildLog],
        writer: &B,
    ) -> B::Output
    where
        K: WarehouseControl,
        CaptureConfig: Capture<K>,
        Unrunnable: From<<K as WarehouseClient>::Error>,
        B: BuildLogWriter,
    {
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();

        source
            .capture()
            .with_change_logs(
                client.warehouse(),
                Replaying::<_, _, _, _, Unrunnable>::new(
                    client,
                    &snapshot(sink),
                    sink.scheduled(),
                    window,
                    previous,
                    listed(pipeline.tables(), sink),
                    writer,
                ),
            )
            .await
            .unwrap()
    }

    fn listed<'t>(
        listing: &'t table::Tables,
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
    ) -> impl Fn(&TableId) -> bool + 't {
        let artifact = snapshot(sink).series().artifact().clone();

        move |table| listing.table(table).builds(&artifact)
    }

    pub(crate) const ARTIFACTS: &str = include_str!("../testdata/step/artifacts.toml");

    pub(crate) const INFRA: &str = include_str!("../testdata/step/infra.toml");

    pub(crate) fn opening(
        name: &str,
        artifacts: &str,
        infra: &str,
        listed: &str,
    ) -> Result<
        Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
        lithify_pipeline::Unreadable,
    > {
        let directory =
            std::env::temp_dir().join(format!("lithify-cli-{name}-{}", Ulid::generate()));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("infra.toml"), infra).unwrap();
        std::fs::write(directory.join("artifact.toml"), artifacts).unwrap();
        std::fs::write(directory.join("table.toml"), listed).unwrap();
        let pipeline = Pipeline::open(&directory);
        std::fs::remove_dir_all(&directory).unwrap();

        pipeline
    }

    pub(crate) fn opened(
        name: &str,
        artifacts: &str,
        infra: &str,
        listed: &str,
    ) -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        opening(name, artifacts, infra, listed).unwrap()
    }

    pub(crate) fn pipeline() -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        opened(
            "pipeline",
            ARTIFACTS,
            INFRA,
            "[default]\nartifacts = [\"snapshot\"]\n",
        )
    }

    async fn dry_run(sink: &Sink<WarehouseConfig, ArtifactConfig>) -> DuckDbClient<DryRun> {
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new(sink.change_log().clone()),
        );
        client.execute(&SqlString::from(CHANGE_LOGS)).await.unwrap();

        client.executing(DryRun::default())
    }

    fn snapshot_build() -> BuildLogWrite {
        BuildLogWrite::new(Run::new(
            Ulid::nil(),
            Ts::from(DateTime::UNIX_EPOCH),
            "primary",
            CAPTURE,
            "0.1.0",
        ))
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

    #[tokio::test]
    async fn a_dry_run_records_the_snapshot_of_every_table_the_schema_changes_left_standing() {
        let pipeline = pipeline();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let client = dry_run(sink).await;
        let window = Window::new(
            Ts::try_from(0).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );

        replayed(&pipeline, &client, &window, &[], &snapshot_build()).await;
        let created = statements(&client, "CREATE OR REPLACE TABLE \"mysql_shop\".\"people_");

        assert_eq!(created.len(), 1);
        assert!(
            created[0].starts_with("CREATE OR REPLACE TABLE \"mysql_shop\".\"people_20260920\" AS")
        );
        assert!(created[0].contains("FROM \"mysql_shop_cdc\".\"mysql.shop.people\""));
        assert!(
            !client
                .exists(&TablePath::new("mysql_shop", "people_20260920"))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn a_dry_run_records_the_build_log_it_would_write_without_writing_it() {
        let pipeline = pipeline();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let client = dry_run(sink).await;
        let window = Window::new(
            Ts::try_from(0).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );

        replayed(&pipeline, &client, &window, &[], &snapshot_build()).await;

        assert_eq!(
            statements(&client, "INSERT INTO \"mysql_shop\".\"_lithify_log\"").len(),
            1
        );
        assert!(
            !client
                .exists(&BuildLog::path(&Namespace::from("mysql_shop")))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn a_dry_run_logs_a_table_with_nothing_to_fold_onto_as_failed_as_a_build_does() {
        let pipeline = pipeline();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let client = dry_run(sink).await;
        let window = Window::new(
            Ts::try_from(1789862400000).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );

        let logs = replayed(&pipeline, &client, &window, &[], &snapshot_build()).await;

        assert_eq!(
            logs.iter().map(BuildLog::status).collect::<Vec<_>>(),
            [&Status::Failed(
                UnbuildableArtifact::WithoutBase(TableId::new("shop", "people")).to_string()
            )]
        );
    }

    #[tokio::test]
    async fn a_dry_run_folds_each_table_onto_the_shard_its_last_build_log_targeted() {
        let pipeline = pipeline();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let client = dry_run(sink).await;
        let window = Window::new(
            Ts::try_from(1789862400000).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );
        let previous = [built_before("people", CAPTURE)];

        replayed(&pipeline, &client, &window, &previous, &snapshot_build()).await;

        assert!(
            statements(&client, "CREATE OR REPLACE TABLE \"mysql_shop\".\"people_")[0]
                .contains("  FROM \"mysql_shop\".\"people_20260919\"\n")
        );
    }

    pub(crate) const CAPTURE: &str = r#"{"debezium":{"dialect":"mysql","topic_prefix":"mysql"}}"#;

    pub(crate) fn at(rfc3339: &str) -> DateTime<Utc> {
        rfc3339.parse().unwrap()
    }

    pub(crate) fn built_before(table: &str, capture: &str) -> BuildLog {
        TableBuilt::new(
            Some(TableId::new("shop", table)),
            Some(TableId::new("shop", table)),
            None,
            Some(TablePath::new("mysql_shop", &format!("{table}_20260919"))),
            Window::new(
                Ts::from(at("2026-09-19T00:00:00Z")),
                Ts::from(at("2026-09-20T00:00:00Z")),
            ),
            Status::Succeeded,
            Ts::from(at("2026-09-20T00:10:00Z")),
        )
        .logged(
            &Run::new(
                Ulid::from_parts(1, 0),
                Ts::from(DateTime::UNIX_EPOCH),
                "primary",
                capture,
                "0.1.0",
            ),
            &Series::new("snapshot".into(), "daily"),
        )
    }

    pub(crate) async fn logged(change_logs: &str, before: &[BuildLog]) -> DuckDbClient {
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new("mysql_shop_cdc"),
        );
        client.execute(&SqlString::from(change_logs)).await.unwrap();
        client
            .execute(&SqlString::from(
                "CREATE SCHEMA mysql_shop;
                 CREATE TABLE mysql_shop.people_20260919 AS SELECT 7::BIGINT AS id;
                 CREATE TABLE mysql_shop.pets_20260919 AS SELECT 7::BIGINT AS id",
            ))
            .await
            .unwrap();
        client
            .create_build_log::<Unrunnable>(&Namespace::from("mysql_shop"))
            .await
            .unwrap();
        client
            .append_build_logs(&Namespace::from("mysql_shop"), before)
            .await
            .unwrap();

        client
    }

    pub(crate) const PEOPLE: &str = include_str!("../testdata/step/people.sql");

    pub(crate) const BUILT_CHANGE_LOGS: &str =
        include_str!("../testdata/step/built_change_logs.sql");

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

    async fn built() -> (
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
        client
            .execute(&SqlString::from(BUILT_CHANGE_LOGS))
            .await
            .unwrap();

        (pipeline, client)
    }

    async fn build(
        pipeline: &Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
        client: &DuckDbClient,
    ) -> Vec<BuildLog> {
        let source = pipeline.sources().next().unwrap();
        let window = Window::new(
            Ts::try_from(0).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );

        replayed(
            pipeline,
            client,
            &window,
            &[],
            &BuildLogWrite::of(
                source,
                Ts::from(at("2026-09-22T05:00:00Z")),
                env!("CARGO_PKG_VERSION"),
            )
            .unwrap(),
        )
        .await
    }

    #[tokio::test]
    async fn a_build_settles_each_table_and_logs_it_once_whether_or_not_it_was_built() {
        let (pipeline, client) = built().await;

        let logs = build(&pipeline, &client).await;

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

    #[tokio::test]
    async fn a_build_logs_the_run_the_capture_and_the_schedule_it_was_built_under() {
        let (pipeline, client) = built().await;

        build(&pipeline, &client).await;

        assert_eq!(
            rows(
                &client,
                "SELECT DISTINCT capture, schedule || ' ' || artifact || ' ' || source FROM mysql_shop._lithify_log"
            ),
            [(CAPTURE.to_owned(), "daily snapshot primary".to_owned())]
        );
    }

    #[tokio::test]
    async fn a_window_built_twice_settles_into_the_same_snapshot_and_reads_as_built_once() {
        let (pipeline, client) = built().await;
        let snapshot = "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260920 ORDER BY id";

        let once = build(&pipeline, &client).await;
        let built_once = rows(&client, snapshot);
        let twice = build(&pipeline, &client).await;

        assert_eq!(rows(&client, snapshot), built_once);
        assert_ne!(once[0].run_id(), twice[0].run_id());
        let latest = client
            .build_logs::<Unrunnable>(
                &Namespace::from("mysql_shop"),
                &Snapshot::days(&"snapshot".into()),
                &Latest::Attempted.into(),
            )
            .await
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
