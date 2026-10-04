use duckdb::Connection;
use duckdb::types::FromSql;

use event_replay_core::artifact::Artifact;
use event_replay_core::listener::{CatalogModel, SchemaChangeListener};
use event_replay_core::model::{TableId, TableModel};
use event_replay_core::sink::TablePath;
use event_replay_core::source::{ChangeLogs, SchemaChangeLog, SchemaChangeStream};
use event_replay_core::window::Window;
use event_replay_protocol::DuckDbWarehouse;
use event_replay_protocol::sink::duckdb::store::ObjectStore;
use event_replay_strategy::snapshot::{Base, Snapshot};

use crate::unreplayable::Unreplayable;

pub struct DuckDbReplay<'a, C> {
    store: &'a ObjectStore,
    logs: C,
}

impl<'a, C> DuckDbReplay<'a, C>
where
    C: ChangeLogs,
    <C::Schema as SchemaChangeLog>::Record: FromSql,
{
    pub fn new(store: &'a ObjectStore, logs: C) -> Self {
        Self { store, logs }
    }

    pub fn catalog(
        &self,
        schema_changes: &TablePath,
        window: &Window,
        listener: &mut impl SchemaChangeListener<C::Position>,
    ) -> Result<CatalogModel<C::Position>, Unreplayable> {
        let mut catalog = CatalogModel::default();

        self.store.client(schema_changes)?.catalog::<Unreplayable>(
            self.logs.schema_changes(),
            schema_changes,
            window,
            &mut catalog,
            listener,
        )?;

        Ok(catalog)
    }

    pub fn table(
        &self,
        schema_changes: &TablePath,
        table: &TableId,
        window: &Window,
    ) -> Result<Option<TableModel>, Unreplayable> {
        let catalog = self.catalog(schema_changes, window, &mut Bystander)?;

        Ok(catalog.table(table).cloned())
    }

    pub fn snapshot(
        &self,
        connection: &Connection,
        model: &TableModel,
        changes: &TablePath,
        into: &TablePath,
        window: &Window,
        base: Base,
    ) -> Result<(), Unreplayable> {
        self.store.attach(connection, changes)?;
        self.fold(connection, model, changes, into, window, base)
    }

    fn fold(
        &self,
        connection: &Connection,
        model: &TableModel,
        changes: &TablePath,
        into: &TablePath,
        window: &Window,
        base: Base,
    ) -> Result<(), Unreplayable> {
        let statements = Snapshot::new(
            self.logs.row_changes(),
            model.clone(),
            changes.clone(),
            into.clone(),
            window.clone(),
            base,
        )
        .statements(&DuckDbWarehouse)?;

        for statement in &statements {
            connection.execute_batch(statement)?;
        }

        Ok(())
    }
}

struct Bystander;

impl<P> SchemaChangeListener<P> for Bystander {}

#[cfg(test)]
mod tests {
    use event_replay_core::model::{ColumnModel, ColumnType};
    use event_replay_core::position::RecordedAt;
    use event_replay_protocol::MySql;
    use event_replay_protocol::sink::duckdb::config::Store;
    use event_replay_protocol::source::debezium::Debezium;

    use super::*;

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

    fn store() -> ObjectStore {
        let store: Store = toml::from_str(
            "[s3]\nendpoint = \"http://minio:9000\"\nregion = \"us-east-1\"\nbucket = \"event-replay\"\nformat = \"parquet\"",
        )
        .unwrap();

        ObjectStore::open(&store, "minioadmin", "minioadmin")
    }

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
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(CHANGE_LOG).unwrap();
        let store = store();
        let into = TablePath::new("shop_snapshot", "people");

        DuckDbReplay::new(&store, Debezium::under("value", MySql))
            .fold(
                &connection,
                &people(),
                &TablePath::new("cdc", "people"),
                &into,
                &Window::new(RecordedAt::from(0), RecordedAt::from(10)),
                Base::Captured,
            )
            .unwrap();

        let rows: Vec<(i64, String)> = connection
            .prepare("SELECT id, name FROM shop_snapshot.people ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(rows, [(1, "Alicia".to_owned())]);
    }
}
