use duckdb::Connection;

use event_replay_core::listener::{CatalogModel, TableEvent, TableListener};
use event_replay_core::position::RecordedAt;
use event_replay_core::sink::TablePath;
use event_replay_core::source::{SchemaChangeStream, SchemaChanges};
use event_replay_core::window::Window;
use event_replay_protocol::sink::duckdb::client::DuckDbClient;
use event_replay_protocol::source::debezium::Debezium;
use event_replay_protocol::{MySql, MySqlPosition};

#[derive(Default)]
struct Journal;

impl TableListener<MySqlPosition> for Journal {
    fn accept(&mut self, event: &TableEvent<'_, MySqlPosition>) {
        let entry = match event {
            TableEvent::Created(created) => format!(
                "created {} ({} columns, key {:?})",
                created.table().id(),
                created.table().columns().count(),
                created.table().primary_key().collect::<Vec<_>>()
            ),
            TableEvent::Dropped(dropped) => format!("dropped {}", dropped.table().id()),
            TableEvent::ColumnAdded(added) => format!(
                "added {}.{} at {}",
                added.after().id(),
                added.column().name(),
                added.column().position()
            ),
            TableEvent::ColumnDropped(dropped) => format!(
                "dropped {}.{}",
                dropped.before().id(),
                dropped.column().name()
            ),
            TableEvent::ColumnRetyped(retyped) => format!(
                "retyped {}.{} {} -> {}",
                retyped.after().id(),
                retyped.column().name(),
                retyped.previous().type_name(),
                retyped.column().type_name()
            ),
            TableEvent::PrimaryKeyChanged(changed) => {
                format!("primary key {}", changed.after().id())
            }
        };
        println!("{entry}");
    }
}

fn main() {
    let parquet = std::env::args().nth(1).expect("a parquet path");
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(&format!(
            "CREATE SCHEMA mysql_shop_cdc;
             CREATE VIEW mysql_shop_cdc.mysql AS SELECT * FROM read_parquet('{parquet}')"
        ))
        .unwrap();

    let client = DuckDbClient::new(connection);
    let log = Debezium::new(MySql);
    let mut catalog = CatalogModel::default();
    let mut journal = Journal;

    let mut changes = client
        .schema_changes(
            &log,
            &TablePath::new("mysql_shop_cdc", "mysql"),
            &Window::new(RecordedAt::from(0), RecordedAt::from(4102444800000)),
        )
        .unwrap();

    for change in changes.stream().unwrap() {
        catalog
            .accept(&change.unwrap(), &mut journal)
            .expect("in order");
    }

    println!("--- catalog");
    for table in catalog.tables() {
        println!("{} ({} columns)", table.id(), table.columns().count());
    }
}
