use event_replay_core::artifact::Artifact;
use event_replay_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use event_replay_core::position::RecordedAt;
use event_replay_core::sink::TablePath;
use event_replay_core::window::Window;
use event_replay_protocol::{BigQuery, Debezium, DuckDb, MySql};
use event_replay_strategy::snapshot::Snapshot;

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
    let snapshot = Snapshot::new(
        &log,
        customers(),
        TablePath::new("mysql_shop_cdc", "mysql_shop_customers"),
        TablePath::new("mysql_shop", "customers_20260920"),
        Window::new(RecordedAt::from(0), RecordedAt::from(4102444800000)),
    );

    match std::env::args().nth(1).as_deref() {
        Some("bigquery") => println!(
            "{}",
            snapshot
                .statements(&BigQuery::new("local-project"))
                .unwrap()[0]
        ),
        _ => println!("{}", snapshot.statements(&DuckDb).unwrap()[0]),
    }
}
