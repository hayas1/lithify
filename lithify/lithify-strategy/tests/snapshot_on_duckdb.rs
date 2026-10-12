use duckdb::Connection;

use lithify_core::artifact::{Artifact, UnbuildableArtifact};
use lithify_core::era::{Bound, Era, EraTracker};
use lithify_core::listener::OutOfOrder;
use lithify_core::listener::SchemaChangeListener;
use lithify_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use lithify_core::position::RecordedAt;
use lithify_core::sink::WarehouseClient;
use lithify_core::source::SchemaChangeStream;
use lithify_core::sql::SqlString;
use lithify_core::window::Window;
use lithify_protocol::sink::duckdb::client::DuckDbClient;
use lithify_protocol::{Debezium, DuckDbWarehouse, MySql, MySqlPosition};
use lithify_strategy::snapshot::Snapshot;

const CHANGE_LOG: &str = "CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
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
            ColumnModel::new("id", "BIGINT", ColumnType::Int64, false, None),
            ColumnModel::new("name", "VARCHAR", ColumnType::Text, false, None),
        ],
        vec!["id".to_owned()],
    )
}

#[test]
fn a_snapshot_holds_each_key_as_the_change_log_last_left_it() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(CHANGE_LOG)).unwrap();
    let log = Debezium::under("value", "mysql", MySql);
    let window = Window::new(RecordedAt::from(0), RecordedAt::from(10));
    let artifact = Snapshot::new("shop_snapshot", "%Y%m%d");
    let mut snapshot =
        artifact.builder::<_, DuckDbWarehouse>(&log, people().id().clone(), &window, None);
    SchemaChangeListener::<MySqlPosition>::era_boundary(
        &mut snapshot,
        client.warehouse(),
        &Era::new(
            people(),
            Bound::Time(*window.from()),
            Bound::Time(*window.to()),
        ),
    )
    .unwrap();

    client.build::<Unbuilt>(&snapshot).unwrap();

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

const SCHEMA_CHANGES: &str = "CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.mysql AS SELECT * FROM (VALUES
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
    CREATE TABLE cdc.\"mysql.shop.pets\" AS SELECT * FROM (VALUES
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
    let log = Debezium::under("value", "mysql", MySql);
    let artifact = Snapshot::new("shop_snapshot", "%Y%m%d");
    let mut tracker = EraTracker::new(window.clone(), |table: &TableId| {
        Ok::<_, Unbuilt>(artifact.builder::<_, DuckDbWarehouse>(&log, table.clone(), window, None))
    });
    let catalog = client
        .replay::<Unbuilt, _>(client.warehouse(), &log, window, &mut tracker)
        .unwrap();
    for snapshot in tracker.finish(client.warehouse(), &catalog).unwrap() {
        client.build::<Unbuilt>(&snapshot).unwrap();
    }
}

#[test]
fn a_replay_of_the_schema_changes_builds_a_snapshot_of_every_table_left_standing() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(CHANGE_LOG)).unwrap();
    client.execute(&SqlString::from(SCHEMA_CHANGES)).unwrap();
    let window = Window::new(RecordedAt::from(0), RecordedAt::from(10));

    replay(&client, &window);

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

#[test]
fn a_table_created_before_the_window_is_snapshotted_too() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(CHANGE_LOG)).unwrap();
    client.execute(&SqlString::from(SCHEMA_CHANGES)).unwrap();
    let window = Window::new(RecordedAt::from(1), RecordedAt::from(10));

    replay(&client, &window);

    assert_eq!(
        rows(
            &client,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'shop_snapshot' ORDER BY table_name"
        ),
        ["people_19700101", "pets_19700101"]
    );
}

const WIDENED: &str = "CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.mysql AS SELECT * FROM (VALUES
      ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
        'ddl': 'CREATE TABLE people (id BIGINT, name VARCHAR(64))',
        'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
            {'name': 'name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL}]}}]}),
      ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL}, 'ts_ms': 5::BIGINT,
        'ddl': 'ALTER TABLE people ADD COLUMN email VARCHAR(64)',
        'tableChanges': [{'type': 'ALTER', 'id': '\"shop\".\"people\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
            {'name': 'name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL},
            {'name': 'email', 'typeName': 'VARCHAR', 'position': 3, 'optional': true,
             'defaultValueExpression': 'none', 'length': 64, 'scale': NULL}]}}]})
    ) AS t(value);
    CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
      ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 2::BIGINT},
        'before': NULL::STRUCT(id BIGINT, name VARCHAR, email VARCHAR),
        'after': {'id': 1::BIGINT, 'name': 'Alice', 'email': NULL::VARCHAR}}),
      ({'op': 'c', 'ts_ms': 2::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 3::BIGINT},
        'before': NULL::STRUCT(id BIGINT, name VARCHAR, email VARCHAR),
        'after': {'id': 2::BIGINT, 'name': 'Bob', 'email': NULL::VARCHAR}}),
      ({'op': 'u', 'ts_ms': 6::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 5::BIGINT},
        'before': {'id': 1::BIGINT, 'name': 'Alice', 'email': 'none'},
        'after': {'id': 1::BIGINT, 'name': 'Alicia', 'email': 'alicia@example.com'}})
    ) AS t(value)";

#[test]
fn a_column_added_within_the_window_is_filled_in_for_the_rows_last_changed_before_it() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(WIDENED)).unwrap();

    replay(
        &client,
        &Window::new(RecordedAt::from(0), RecordedAt::from(10)),
    );

    assert_eq!(
        rows(
            &client,
            "SELECT id::VARCHAR || ' ' || name || ' ' || email FROM shop_snapshot.people_19700101 ORDER BY id"
        ),
        ["1 Alicia alicia@example.com", "2 Bob none"]
    );
}

const RESHAPED: &str = "CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.mysql AS SELECT * FROM (VALUES
      ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
        'ddl': 'CREATE TABLE people (id BIGINT, name VARCHAR(64), email VARCHAR(64))',
        'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
            {'name': 'name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL},
            {'name': 'email', 'typeName': 'VARCHAR', 'position': 3, 'optional': true,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL}]}}]}),
      ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL}, 'ts_ms': 5::BIGINT,
        'ddl': 'ALTER TABLE people DROP COLUMN email, ADD COLUMN phone VARCHAR(16) DEFAULT ''unknown'' AFTER id, ADD COLUMN nickname VARCHAR(64)',
        'tableChanges': [{'type': 'ALTER', 'id': '\"shop\".\"people\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
            {'name': 'phone', 'typeName': 'VARCHAR', 'position': 2, 'optional': true,
             'defaultValueExpression': 'unknown', 'length': 16, 'scale': NULL},
            {'name': 'name', 'typeName': 'VARCHAR', 'position': 3, 'optional': false,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL},
            {'name': 'nickname', 'typeName': 'VARCHAR', 'position': 4, 'optional': true,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL}]}}]})
    ) AS t(value);
    CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
      ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 2::BIGINT},
        'before': NULL::STRUCT(id BIGINT, name VARCHAR, email VARCHAR, phone VARCHAR, nickname VARCHAR),
        'after': {'id': 1::BIGINT, 'name': 'Alice', 'email': 'alice@example.com', 'phone': NULL::VARCHAR, 'nickname': NULL::VARCHAR}}),
      ({'op': 'c', 'ts_ms': 2::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 3::BIGINT},
        'before': NULL::STRUCT(id BIGINT, name VARCHAR, email VARCHAR, phone VARCHAR, nickname VARCHAR),
        'after': {'id': 2::BIGINT, 'name': 'Bob', 'email': 'bob@example.com', 'phone': NULL::VARCHAR, 'nickname': NULL::VARCHAR}}),
      ({'op': 'u', 'ts_ms': 6::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 5::BIGINT},
        'before': {'id': 1::BIGINT, 'name': 'Alice', 'email': NULL::VARCHAR, 'phone': 'unknown', 'nickname': NULL::VARCHAR},
        'after': {'id': 1::BIGINT, 'name': 'Alicia', 'email': NULL::VARCHAR, 'phone': '0123', 'nickname': 'Ali'}})
    ) AS t(value)";

#[test]
fn columns_one_statement_dropped_and_added_reshape_the_rows_last_changed_before_it() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(RESHAPED)).unwrap();

    replay(
        &client,
        &Window::new(RecordedAt::from(0), RecordedAt::from(10)),
    );

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

const RENAMED: &str = "CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.mysql AS SELECT * FROM (VALUES
      ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
        'ddl': 'CREATE TABLE people (id BIGINT, name VARCHAR(64))',
        'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
            {'name': 'name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL}]}}]}),
      ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL}, 'ts_ms': 5::BIGINT,
        'ddl': 'ALTER TABLE people RENAME COLUMN name TO full_name',
        'tableChanges': [{'type': 'ALTER', 'id': '\"shop\".\"people\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
            {'name': 'full_name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL}]}}]})
    ) AS t(value);
    CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
      ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 2::BIGINT},
        'before': NULL::STRUCT(id BIGINT, name VARCHAR, full_name VARCHAR),
        'after': {'id': 1::BIGINT, 'name': 'Alice', 'full_name': NULL::VARCHAR}}),
      ({'op': 'c', 'ts_ms': 2::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 3::BIGINT},
        'before': NULL::STRUCT(id BIGINT, name VARCHAR, full_name VARCHAR),
        'after': {'id': 2::BIGINT, 'name': 'Bob', 'full_name': NULL::VARCHAR}}),
      ({'op': 'u', 'ts_ms': 6::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 5::BIGINT},
        'before': {'id': 1::BIGINT, 'name': NULL::VARCHAR, 'full_name': 'Alice'},
        'after': {'id': 1::BIGINT, 'name': NULL::VARCHAR, 'full_name': 'Alicia Liddell'}})
    ) AS t(value)";

#[test]
fn a_column_renamed_within_the_window_keeps_the_values_of_the_rows_last_changed_before_it() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(RENAMED)).unwrap();

    replay(
        &client,
        &Window::new(RecordedAt::from(0), RecordedAt::from(10)),
    );

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

const MOVED: &str = "CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.mysql AS SELECT * FROM (VALUES
      ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
        'ddl': 'CREATE TABLE people (id BIGINT, name VARCHAR(64))',
        'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
            {'name': 'name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL}]}}]}),
      ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL}, 'ts_ms': 5::BIGINT,
        'ddl': 'RENAME TABLE people TO persons',
        'tableChanges': [{'type': 'ALTER', 'id': '\"shop\".\"persons\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
            {'name': 'name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL}]}}]})
    ) AS t(value);
    CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
      ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 2::BIGINT},
        'before': NULL::STRUCT(id BIGINT, name VARCHAR), 'after': {'id': 1::BIGINT, 'name': 'Alice'}}),
      ({'op': 'c', 'ts_ms': 2::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 3::BIGINT},
        'before': NULL::STRUCT(id BIGINT, name VARCHAR), 'after': {'id': 2::BIGINT, 'name': 'Bob'}})
    ) AS t(value);
    CREATE TABLE cdc.\"mysql.shop.persons\" AS SELECT * FROM (VALUES
      ({'op': 'u', 'ts_ms': 6::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 5::BIGINT},
        'before': {'id': 1::BIGINT, 'name': 'Alice'}, 'after': {'id': 1::BIGINT, 'name': 'Alicia'}}),
      ({'op': 'c', 'ts_ms': 7::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 6::BIGINT},
        'before': NULL::STRUCT(id BIGINT, name VARCHAR), 'after': {'id': 3::BIGINT, 'name': 'Carol'}})
    ) AS t(value)";

#[test]
fn a_table_renamed_within_the_window_is_snapshotted_under_its_new_name_with_every_row() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(MOVED)).unwrap();

    replay(
        &client,
        &Window::new(RecordedAt::from(0), RecordedAt::from(10)),
    );

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

const STAMPED: &str = "CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.mysql AS SELECT * FROM (VALUES
      ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
        'ddl': 'CREATE TABLE people (id BIGINT)',
        'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
      ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL}, 'ts_ms': 5::BIGINT,
        'ddl': 'ALTER TABLE people ADD COLUMN seen_at DATETIME DEFAULT CURRENT_TIMESTAMP',
        'tableChanges': [{'type': 'ALTER', 'id': '\"shop\".\"people\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
            {'name': 'seen_at', 'typeName': 'DATETIME', 'position': 2, 'optional': true,
             'defaultValueExpression': 'CURRENT_TIMESTAMP', 'length': NULL, 'scale': NULL}]}}]})
    ) AS t(value);
    CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
      ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 2::BIGINT},
        'before': NULL::STRUCT(id BIGINT, seen_at BIGINT), 'after': {'id': 1::BIGINT, 'seen_at': NULL::BIGINT}})
    ) AS t(value)";

#[test]
fn a_column_added_with_a_default_no_literal_spells_is_filled_with_null_rather_than_failing() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(STAMPED)).unwrap();

    replay(
        &client,
        &Window::new(RecordedAt::from(0), RecordedAt::from(10)),
    );

    assert_eq!(
        rows(
            &client,
            "SELECT concat_ws(' ', id, coalesce(seen_at::VARCHAR, 'unknown')) FROM shop_snapshot.people_19700101 ORDER BY id"
        ),
        ["1 unknown"]
    );
}

const SIMULTANEOUS: &str = "CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.mysql AS SELECT * FROM (VALUES
      ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
        'ddl': 'CREATE TABLE people (id BIGINT, name VARCHAR(64))',
        'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
            {'name': 'name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL}]}}]}),
      ({'source': {'file': 'binlog.000001', 'pos': 10::BIGINT, 'gtid': NULL}, 'ts_ms': 5::BIGINT,
        'ddl': 'ALTER TABLE people ADD COLUMN email VARCHAR(64) DEFAULT ''none''',
        'tableChanges': [{'type': 'ALTER', 'id': '\"shop\".\"people\"',
          'table': {'primaryKeyColumnNames': ['id'], 'columns': [
            {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
             'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
            {'name': 'name', 'typeName': 'VARCHAR', 'position': 2, 'optional': false,
             'defaultValueExpression': NULL, 'length': 64, 'scale': NULL},
            {'name': 'email', 'typeName': 'VARCHAR', 'position': 3, 'optional': true,
             'defaultValueExpression': 'none', 'length': 64, 'scale': NULL}]}}]})
    ) AS t(value);
    CREATE SCHEMA IF NOT EXISTS cdc;
    CREATE TABLE cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
      ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 2::BIGINT},
        'before': NULL::STRUCT(id BIGINT, name VARCHAR, email VARCHAR),
        'after': {'id': 1::BIGINT, 'name': 'Alice', 'email': NULL::VARCHAR}}),
      ({'op': 'r', 'ts_ms': 1::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 3::BIGINT},
        'before': NULL::STRUCT(id BIGINT, name VARCHAR, email VARCHAR),
        'after': {'id': 2::BIGINT, 'name': 'Bob', 'email': NULL::VARCHAR}}),
      ({'op': 'u', 'ts_ms': 5::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 8::BIGINT},
        'before': {'id': 2::BIGINT, 'name': 'Bob', 'email': NULL::VARCHAR},
        'after': {'id': 2::BIGINT, 'name': 'Bobby', 'email': NULL::VARCHAR}}),
      ({'op': 'u', 'ts_ms': 5::BIGINT, 'source': {'file': 'binlog.000001', 'pos': 12::BIGINT},
        'before': {'id': 1::BIGINT, 'name': 'Alice', 'email': 'none'},
        'after': {'id': 1::BIGINT, 'name': 'Alicia', 'email': 'alicia@example.com'}})
    ) AS t(value)";

#[test]
fn changes_recorded_in_the_instant_of_a_schema_change_fall_on_its_side_their_position_puts_them() {
    let client = DuckDbClient::new(
        Connection::open_in_memory().unwrap(),
        DuckDbWarehouse::new("cdc"),
    );
    client.execute(&SqlString::from(SIMULTANEOUS)).unwrap();

    replay(
        &client,
        &Window::new(RecordedAt::from(0), RecordedAt::from(10)),
    );

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

impl From<OutOfOrder> for Unbuilt {
    fn from(_: OutOfOrder) -> Self {
        Self
    }
}
