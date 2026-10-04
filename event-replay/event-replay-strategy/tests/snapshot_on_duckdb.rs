use duckdb::Connection;

use event_replay_core::artifact::UnbuildableArtifact;
use event_replay_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use event_replay_core::position::RecordedAt;
use event_replay_core::sink::{TablePath, WarehouseClient};
use event_replay_core::window::Window;
use event_replay_protocol::sink::duckdb::client::DuckDbClient;
use event_replay_protocol::{Debezium, DuckDbWarehouse, MySql};
use event_replay_strategy::snapshot::{Base, Snapshot};

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
