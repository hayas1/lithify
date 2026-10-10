use duckdb::Connection;

use event_replay_core::artifact::UnbuildableArtifact;
use event_replay_core::listener::OutOfOrder;
use event_replay_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use event_replay_core::position::RecordedAt;
use event_replay_core::sink::{TablePath, WarehouseClient};
use event_replay_core::source::SchemaChangeStream;
use event_replay_core::window::Window;
use event_replay_protocol::sink::duckdb::client::DuckDbClient;
use event_replay_protocol::{Debezium, DuckDbWarehouse, MySql};
use event_replay_strategy::snapshot::{Base, Snapshot, SnapshotReplay};

const CHANGE_LOG: &str = "CREATE SCHEMA cdc;
    CREATE TABLE cdc.people AS SELECT * FROM (VALUES
      ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 1::BIGINT},
        'before': NULL, 'after': {'id': 1::BIGINT, 'name': 'Alice'}}),
      ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 2::BIGINT},
        'before': NULL, 'after': {'id': 2::BIGINT, 'name': 'Bob'}}),
      ({'op': 'u', 'ts_ms': 2::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 3::BIGINT},
        'before': {'id': 1::BIGINT, 'name': 'Alice'}, 'after': {'id': 1::BIGINT, 'name': 'Alicia'}}),
      ({'op': 'd', 'ts_ms': 3::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 4::BIGINT},
        'before': {'id': 2::BIGINT, 'name': 'Bob'}, 'after': NULL})
    ) AS t(value)";

fn people() -> TableModel {
    TableModel::new(
        TableId::new("shop", "people"),
        vec![
            ColumnModel::new("id", "BIGINT", ColumnType::Int64, 1, false, None),
            ColumnModel::new("name", "VARCHAR", ColumnType::Text, 2, false, None),
        ],
        vec!["id".to_owned()],
    )
}

#[test]
fn a_snapshot_holds_each_key_as_the_change_log_last_left_it() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::default(),
    );
    client.execute(CHANGE_LOG).unwrap();
    let log = Debezium::under("value", MySql);
    let snapshot = Snapshot::new(
        &log,
        people(),
        TablePath::new("cdc", "people"),
        TablePath::new("shop_snapshot", "people"),
        Window::new(RecordedAt::from(0), RecordedAt::from(10)),
        Base::Captured,
    );

    client.build::<Unbuilt>(&snapshot).unwrap();

    let rows: Vec<(i64, String)> = client
        .connection()
        .prepare("SELECT id, name FROM shop_snapshot.people ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows, [(1, "Alicia".to_owned())]);
}

const SCHEMA_CHANGES: &str = "CREATE SCHEMA ddl;
    CREATE TABLE ddl.shop AS SELECT * FROM (VALUES
      ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
        'ddl': 'CREATE TABLE people (id BIGINT, name VARCHAR(64))',
        'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
            {'name': 'name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL}]}}]}),
      ({'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
        'ddl': 'CREATE TABLE pets (id BIGINT)',
        'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"pets\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
      ({'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
        'ddl': 'CREATE TABLE gone (id BIGINT)',
        'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"gone\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
      ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
        'ddl': 'DROP TABLE gone',
        'tableChanges': [{'type': 'DROP', 'id': '\"shop\".\"gone\"', 'table': NULL}]})
    ) AS t(value);
    CREATE TABLE cdc.pets AS SELECT * FROM (VALUES
      ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 5::BIGINT},
        'before': NULL::STRUCT(id BIGINT), 'after': {'id': 7::BIGINT}})
    ) AS t(value)";

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

fn replay(client: &DuckDbClient, window: &Window) {
    let log = Debezium::under("value", MySql);
    let mut replay = SnapshotReplay::new(|table: &TableModel| {
        Ok::<_, Unbuilt>(Snapshot::new(
            &log,
            table.clone(),
            TablePath::new("cdc", table.id().table()),
            TablePath::new("shop_snapshot", table.id().table()),
            window.clone(),
            Base::Captured,
        ))
    });
    let catalog = client
        .replay::<Unbuilt>(&log, &TablePath::new("ddl", "shop"), window, &mut replay)
        .unwrap();
    let snapshots = replay.snapshots(&catalog).unwrap();

    client.build::<Unbuilt>(&snapshots).unwrap();
}

#[test]
fn a_replay_of_the_schema_changes_builds_a_snapshot_of_every_table_left_standing() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::default(),
    );
    client.execute(CHANGE_LOG).unwrap();
    client.execute(SCHEMA_CHANGES).unwrap();
    let window = Window::new(RecordedAt::from(0), RecordedAt::from(10));

    replay(&client, &window);

    assert_eq!(
        rows(&client, "SELECT name FROM shop_snapshot.people ORDER BY id"),
        ["Alicia"]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT id::VARCHAR FROM shop_snapshot.pets ORDER BY id"
        ),
        ["7"]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'shop_snapshot' ORDER BY table_name"
        ),
        ["people", "pets"]
    );
}

#[test]
fn a_table_created_before_the_window_is_snapshotted_too() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::default(),
    );
    client.execute(CHANGE_LOG).unwrap();
    client.execute(SCHEMA_CHANGES).unwrap();
    let window = Window::new(RecordedAt::from(1), RecordedAt::from(10));

    replay(&client, &window);

    assert_eq!(
        rows(
            &client,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'shop_snapshot' ORDER BY table_name"
        ),
        ["people", "pets"]
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

impl From<OutOfOrder> for Unbuilt {
    fn from(_: OutOfOrder) -> Self {
        Self
    }
}
