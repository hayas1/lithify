use event_replay_core::artifact::{Artifact, ArtifactBuilder};
use event_replay_core::era::{Bound, Era};
use event_replay_core::listener::SchemaChangeListener;
use event_replay_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use event_replay_core::position::RecordedAt;
use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::window::Window;
use event_replay_protocol::{BigQueryWarehouse, Debezium, DuckDbWarehouse, MySql, MySqlPosition};
use event_replay_strategy::snapshot::Snapshot;

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
    let log = Debezium::new("mysql", MySql);
    let window = Window::new(
        RecordedAt::from(1789862400000),
        RecordedAt::from(1789948800000),
    );
    let artifact = Snapshot::new("mysql_shop", "%Y%m%d");
    let mut snapshot = artifact.builder::<_, W>(
        &log,
        customers().id().clone(),
        &window,
        std::env::args()
            .nth(2)
            .map(|shard| TablePath::new("mysql_shop", &shard)),
    );

    SchemaChangeListener::<MySqlPosition>::era_boundary(
        &mut snapshot,
        warehouse,
        &Era::new(
            customers(),
            Bound::Time(*window.from()),
            Bound::Time(*window.to()),
        ),
    )
    .unwrap();

    snapshot.create().unwrap().join(";\n")
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("bigquery") => println!(
            "{}",
            statements(&BigQueryWarehouse::new("local-project", "mysql_shop_cdc"))
        ),
        _ => println!("{}", statements(&DuckDbWarehouse::new("mysql_shop_cdc"))),
    }
}
