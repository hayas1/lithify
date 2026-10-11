use event_replay_core::artifact::Artifact;
use event_replay_core::era::{Era, Lineage};
use event_replay_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use event_replay_core::position::RecordedAt;
use event_replay_core::sink::TablePath;
use event_replay_core::window::Window;
use event_replay_protocol::{BigQueryWarehouse, Debezium, DuckDbWarehouse, MySql};
use event_replay_strategy::snapshot::{Base, Snapshot};

fn column(name: &str, type_name: &str, column_type: ColumnType, position: u32) -> ColumnModel {
    ColumnModel::new(name, type_name, column_type, position, false, None)
}

fn customers() -> TableModel {
    TableModel::new(
        TableId::new("shop", "customers"),
        vec![
            column("id", "BIGINT", ColumnType::Int64, 1),
            column("email", "VARCHAR", ColumnType::Text, 2),
            column("name", "VARCHAR", ColumnType::Text, 3),
            column("is_active", "TINYINT", ColumnType::Int16, 4),
            column("tags", "JSON", ColumnType::Json, 5),
            column("created_at", "DATETIME", ColumnType::Timestamp, 6),
            column("updated_at", "DATETIME", ColumnType::Timestamp, 7),
        ],
        vec!["id".to_owned()],
    )
}

fn main() {
    let log = Debezium::new(MySql);
    let window = Window::new(RecordedAt::from(0), RecordedAt::from(4102444800000));
    let snapshot = Snapshot::new(
        &log,
        Lineage::new(customers(), vec![Era::new(customers(), window)]),
        TablePath::new("mysql_shop_cdc", "mysql_shop_customers"),
        TablePath::new("mysql_shop", "customers_20260920"),
        match std::env::args().nth(2).as_deref() {
            Some(shard) => Base::Shard(TablePath::new("mysql_shop", shard)),
            None => Base::Captured,
        },
    );

    match std::env::args().nth(1).as_deref() {
        Some("bigquery") => println!(
            "{}",
            snapshot
                .statements(&BigQueryWarehouse::new("local-project"))
                .unwrap()
                .join(";\n")
        ),
        _ => println!(
            "{}",
            snapshot
                .statements(&DuckDbWarehouse::default())
                .unwrap()
                .join(";\n")
        ),
    }
}
