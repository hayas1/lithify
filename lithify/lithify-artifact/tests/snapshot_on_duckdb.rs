use std::convert::Infallible;

use duckdb::Connection;

use lithify_artifact::shard::Shard;
use lithify_artifact::snapshot::Snapshot;
use lithify_core::artifact::{Artifact, UnbuildableArtifact};
use lithify_core::build::{Base, BuildContext};
use lithify_core::listener::OutOfOrder;
use lithify_core::listener::ReplayEnd;
use lithify_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use lithify_core::runner::Replayer;
use lithify_core::sink::{Namespace, TablePath, WarehouseClient, WarehouseSql};
use lithify_core::source::SchemaChangeStream;
use lithify_core::sql::SqlString;
use lithify_core::ts::Ts;
use lithify_core::window::Window;
use lithify_protocol::sink::duckdb::client::DuckDbClient;
use lithify_protocol::{Debezium, DuckDbWarehouse, MySql};

const CHANGE_LOG: &str = include_str!("../testdata/snapshot_on_duckdb/change_log.sql");

fn people() -> TableModel {
    TableModel::new(
        TableId::new("shop", "people"),
        vec![
            ColumnModel::new("id", "BIGINT", ColumnType::Int64, false, None),
            ColumnModel::new("name", "VARCHAR", ColumnType::Text, false, None),
        ],
        ["id"].into(),
    )
}

#[tokio::test]
async fn a_snapshot_holds_each_key_as_the_change_log_last_left_it() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(CHANGE_LOG)).await.unwrap();
    let log = Debezium::under("value", "mysql", MySql);
    let window = Window::new(Ts::try_from(0).unwrap(), Ts::try_from(10).unwrap());
    let artifact = Snapshot::new(Shard::new("%Y%m%d", chrono_tz::Tz::UTC));
    let target = TablePath::new(
        "shop_snapshot",
        &artifact.table_name(people().id(), &window),
    );
    client
        .execute(&client.warehouse().create_namespace(target.namespace()))
        .await
        .unwrap();
    let snapshot = artifact.builder::<_, DuckDbWarehouse>(&log, &window, Base::Missing);
    let statements = artifact
        .create(
            &snapshot,
            client.warehouse(),
            &ReplayEnd::standing(people(), None),
            &target,
        )
        .unwrap();
    for statement in &statements {
        client.execute(statement).await.unwrap();
    }

    let rows: Vec<(i64, String)> = client
        .connection()
        .prepare("SELECT id, name FROM shop_snapshot.people_19700101 ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows, [(1, "Alicia".to_owned())]);
}

const SCHEMA_CHANGES: &str = include_str!("../testdata/snapshot_on_duckdb/schema_changes.sql");

fn rows(client: &DuckDbClient, query: &str) -> Vec<String> {
    client
        .connection()
        .prepare(query)
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

async fn build_snapshots(client: &DuckDbClient, window: &Window) {
    let log = Debezium::under("value", "mysql", MySql);
    let artifact = Snapshot::new(Shard::new("%Y%m%d", chrono_tz::Tz::UTC));
    let namespace = Namespace::from("shop_snapshot");
    let history = client.history::<Unbuilt>(&log, window).await.unwrap();
    let builds = Replayer::new(BuildContext::new(client.warehouse(), &namespace), &log)
        .planned::<_, Unbuilt>(&history, &artifact, window, |_| None, |_| true)
        .unwrap();
    for build in builds {
        for statement in build.statements().unwrap() {
            client.execute(statement).await.unwrap();
        }
    }
}

#[tokio::test]
async fn a_replay_of_the_schema_changes_builds_a_snapshot_of_every_table_left_standing() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(CHANGE_LOG)).await.unwrap();
    client
        .execute(&SqlString::from(SCHEMA_CHANGES))
        .await
        .unwrap();
    let window = Window::new(Ts::try_from(0).unwrap(), Ts::try_from(10).unwrap());

    build_snapshots(&client, &window).await;

    assert_eq!(
        rows(
            &client,
            "SELECT name FROM shop_snapshot.people_19700101 ORDER BY id"
        ),
        ["Alicia"]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT id::VARCHAR FROM shop_snapshot.pets_19700101 ORDER BY id"
        ),
        ["7"]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'shop_snapshot' ORDER BY table_name"
        ),
        ["people_19700101", "pets_19700101"]
    );
}

#[tokio::test]
async fn a_table_created_before_the_window_is_snapshotted_too() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(CHANGE_LOG)).await.unwrap();
    client
        .execute(&SqlString::from(SCHEMA_CHANGES))
        .await
        .unwrap();
    let window = Window::new(Ts::try_from(1).unwrap(), Ts::try_from(10).unwrap());

    build_snapshots(&client, &window).await;

    assert_eq!(
        rows(
            &client,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'shop_snapshot' ORDER BY table_name"
        ),
        ["people_19700101", "pets_19700101"]
    );
}

const WIDENED: &str = include_str!("../testdata/snapshot_on_duckdb/widened.sql");

#[tokio::test]
async fn a_column_added_within_the_window_is_filled_in_for_the_rows_last_changed_before_it() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(WIDENED)).await.unwrap();

    build_snapshots(
        &client,
        &Window::new(Ts::try_from(0).unwrap(), Ts::try_from(10).unwrap()),
    )
    .await;

    assert_eq!(
        rows(
            &client,
            "SELECT id::VARCHAR || ' ' || name || ' ' || email FROM shop_snapshot.people_19700101 ORDER BY id"
        ),
        ["1 Alicia alicia@example.com", "2 Bob none"]
    );
}

const RESHAPED: &str = include_str!("../testdata/snapshot_on_duckdb/reshaped.sql");

#[tokio::test]
async fn columns_one_statement_dropped_and_added_reshape_the_rows_last_changed_before_it() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(RESHAPED)).await.unwrap();

    build_snapshots(
        &client,
        &Window::new(Ts::try_from(0).unwrap(), Ts::try_from(10).unwrap()),
    )
    .await;

    assert_eq!(
        rows(
            &client,
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'shop_snapshot' AND table_name = 'people_19700101' ORDER BY ordinal_position"
        ),
        ["id", "phone", "name", "nickname"]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT concat_ws(' ', id, phone, name, coalesce(nickname, '-')) FROM shop_snapshot.people_19700101 ORDER BY id"
        ),
        ["1 0123 Alicia Ali", "2 unknown Bob -"]
    );
}

const RENAMED: &str = include_str!("../testdata/snapshot_on_duckdb/renamed.sql");

#[tokio::test]
async fn a_column_renamed_within_the_window_keeps_the_values_of_the_rows_last_changed_before_it() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(RENAMED)).await.unwrap();

    build_snapshots(
        &client,
        &Window::new(Ts::try_from(0).unwrap(), Ts::try_from(10).unwrap()),
    )
    .await;

    assert_eq!(
        rows(
            &client,
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'shop_snapshot' AND table_name = 'people_19700101' ORDER BY ordinal_position"
        ),
        ["id", "full_name"]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT concat_ws(' ', id, full_name) FROM shop_snapshot.people_19700101 ORDER BY id"
        ),
        ["1 Alicia Liddell", "2 Bob"]
    );
}

const MOVED: &str = include_str!("../testdata/snapshot_on_duckdb/moved.sql");

#[tokio::test]
async fn a_table_renamed_within_the_window_is_snapshotted_under_its_new_name_with_every_row() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(MOVED)).await.unwrap();

    build_snapshots(
        &client,
        &Window::new(Ts::try_from(0).unwrap(), Ts::try_from(10).unwrap()),
    )
    .await;

    assert_eq!(
        rows(
            &client,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'shop_snapshot' ORDER BY table_name"
        ),
        ["persons_19700101"]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT concat_ws(' ', id, name) FROM shop_snapshot.persons_19700101 ORDER BY id"
        ),
        ["1 Alicia", "2 Bob", "3 Carol"]
    );
}

const STAMPED: &str = include_str!("../testdata/snapshot_on_duckdb/stamped.sql");

#[tokio::test]
async fn a_column_added_with_a_default_no_literal_spells_is_filled_with_null_rather_than_failing() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(STAMPED)).await.unwrap();

    build_snapshots(
        &client,
        &Window::new(Ts::try_from(0).unwrap(), Ts::try_from(10).unwrap()),
    )
    .await;

    assert_eq!(
        rows(
            &client,
            "SELECT concat_ws(' ', id, coalesce(seen_at::VARCHAR, 'unknown')) FROM shop_snapshot.people_19700101 ORDER BY id"
        ),
        ["1 unknown"]
    );
}

const SIMULTANEOUS: &str = include_str!("../testdata/snapshot_on_duckdb/simultaneous.sql");

#[tokio::test]
async fn changes_processed_in_the_instant_of_a_schema_change_fall_on_its_side_their_position_puts_them()
 {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client
        .execute(&SqlString::from(SIMULTANEOUS))
        .await
        .unwrap();

    build_snapshots(
        &client,
        &Window::new(Ts::try_from(0).unwrap(), Ts::try_from(10).unwrap()),
    )
    .await;

    assert_eq!(
        rows(
            &client,
            "SELECT concat_ws(' ', id, name, coalesce(email, 'null')) FROM shop_snapshot.people_19700101 ORDER BY id"
        ),
        ["1 Alicia alicia@example.com", "2 Bobby none"]
    );
}

#[derive(Debug)]
struct Unbuilt;

impl From<duckdb::Error> for Unbuilt {
    fn from(_: duckdb::Error) -> Self {
        Self
    }
}

impl From<UnbuildableArtifact> for Unbuilt {
    fn from(_: UnbuildableArtifact) -> Self {
        Self
    }
}

impl From<Infallible> for Unbuilt {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

impl From<OutOfOrder> for Unbuilt {
    fn from(_: OutOfOrder) -> Self {
        Self
    }
}
