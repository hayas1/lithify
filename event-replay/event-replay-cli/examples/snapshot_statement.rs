use event_replay_core::artifact::Artifact;
use event_replay_core::era::Era;
use event_replay_core::listener::SchemaChangeListener;
use event_replay_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use event_replay_core::position::RecordedAt;
use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::window::Window;
use event_replay_protocol::{BigQueryWarehouse, Debezium, DuckDbWarehouse, MySql, MySqlPosition};
use event_replay_strategy::snapshot::{Base, Snapshot};

fn column(name: &str, type_name: &str, column_type: ColumnType) -> ColumnModel {
    ColumnModel::new(name, type_name, column_type, false, None)
}

fn customers() -> TableModel {
    TableModel::new(
        TableId::new("shop", "customers"),
        vec![
            column("id", "BIGINT", ColumnType::Int64),
            column("email", "VARCHAR", ColumnType::Text),
            column("name", "VARCHAR", ColumnType::Text),
            column("is_active", "TINYINT", ColumnType::Int16),
            column("tags", "JSON", ColumnType::Json),
            column("created_at", "DATETIME", ColumnType::Timestamp),
            column("updated_at", "DATETIME", ColumnType::Timestamp),
        ],
        vec!["id".to_owned()],
    )
}

fn statements<W: Warehouse>(warehouse: &W) -> String {
    let log = Debezium::new(MySql);
    let window = Window::new(RecordedAt::from(0), RecordedAt::from(4102444800000));
    let mut snapshot: Snapshot<'_, _, W> = Snapshot::new(
        &log,
        customers().id().clone(),
        TablePath::new("mysql_shop_cdc", "mysql_shop_customers"),
        TablePath::new("mysql_shop", "customers_20260920"),
        match std::env::args().nth(2).as_deref() {
            Some(shard) => Base::Shard(TablePath::new("mysql_shop", shard)),
            None => Base::Captured,
        },
    );

    SchemaChangeListener::<MySqlPosition>::era_boundary(
        &mut snapshot,
        warehouse,
        &Era::new(customers(), window),
    )
    .unwrap();

    snapshot.statements().unwrap().join(";\n")
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("bigquery") => println!("{}", statements(&BigQueryWarehouse::new("local-project"))),
        _ => println!("{}", statements(&DuckDbWarehouse::default())),
    }
}
