use duckdb::Connection;

use event_replay_core::listener::{CatalogModel, TableEvent, TableListener};
use event_replay_protocol::source::debezium::{Debezium, DebeziumDdl};
use event_replay_protocol::{MySql, MySqlPosition, MySqlSource};

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

const QUERY: &str = "SELECT value
    FROM read_parquet(?)
    WHERE len(value.tableChanges) > 0
    ORDER BY value.source.file, value.source.pos";

fn main() {
    let parquet = std::env::args().nth(1).expect("a parquet path");
    let connection = Connection::open_in_memory().unwrap();
    let log = Debezium::new(MySql);
    let mut catalog = CatalogModel::default();
    let mut journal = Journal;

    let mut prepared = connection.prepare(QUERY).unwrap();
    let mut rows = prepared.query([parquet]).unwrap();

    while let Some(row) = rows.next().unwrap() {
        let statement: DebeziumDdl<MySqlSource> = row.get(0).unwrap();
        let change = log.schema_change(statement).unwrap();
        catalog.accept(&change, &mut journal).unwrap();
    }

    println!("--- catalog");
    for table in catalog.tables() {
        println!("{} ({} columns)", table.id(), table.columns().count());
    }
}
