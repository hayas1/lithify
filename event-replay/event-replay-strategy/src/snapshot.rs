use event_replay_core::artifact::{Artifact, UnbuildableArtifact};
use event_replay_core::model::{ColumnModel, TableModel};
use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::source::ChangeLog;
use event_replay_core::window::Window;

pub struct Snapshot<'a, L> {
    log: &'a L,
    table: TableModel,
    cdc: TablePath,
    target: TablePath,
    window: Window,
}

impl<'a, L: ChangeLog> Snapshot<'a, L> {
    pub fn new(
        log: &'a L,
        table: TableModel,
        cdc: TablePath,
        target: TablePath,
        window: Window,
    ) -> Self {
        Self {
            log,
            table,
            cdc,
            target,
            window,
        }
    }

    fn create_table(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        Ok(format!(
            "CREATE OR REPLACE TABLE {} AS\n{}\n{}\nFROM {}\nWHERE {} <> 'd'",
            warehouse.table_ref(&self.target),
            self.latest_of_each_key(warehouse)?,
            self.columns(warehouse)?,
            warehouse.identifier("changes"),
            warehouse.path(&self.log.operation()),
        ))
    }

    fn latest_of_each_key(
        &self,
        warehouse: &impl Warehouse,
    ) -> Result<String, UnbuildableArtifact> {
        Ok(format!(
            concat!(
                "WITH {} AS (\n",
                "  SELECT *\n",
                "  FROM {}\n",
                "  WHERE {} >= {} AND {} < {}\n",
                "  QUALIFY ROW_NUMBER() OVER (\n",
                "    PARTITION BY {}\n",
                "    ORDER BY {}\n",
                "  ) = 1\n",
                ")"
            ),
            warehouse.identifier("changes"),
            warehouse.table_ref(&self.cdc),
            warehouse.path(&self.log.recorded_at()),
            self.window.from().milliseconds_since_epoch(),
            warehouse.path(&self.log.recorded_at()),
            self.window.to().milliseconds_since_epoch(),
            self.key(warehouse)?,
            self.ordering(warehouse),
        ))
    }

    fn key(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        let key = warehouse.path(&self.log.key());
        let columns: Vec<String> = self
            .table
            .primary_key()
            .map(|column| format!("{key}.{}", warehouse.identifier(column)))
            .collect();

        if columns.is_empty() {
            return Err(UnbuildableArtifact::WithoutPrimaryKey(
                self.table.id().clone(),
            ));
        }

        Ok(columns.join(", "))
    }

    fn ordering(&self, warehouse: &impl Warehouse) -> String {
        self.log
            .ordering()
            .iter()
            .map(|path| format!("{} DESC", warehouse.path(path)))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn columns(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        let after = warehouse.path(&self.log.after());
        let columns: Vec<String> = self
            .table
            .columns()
            .map(|column| self.column(warehouse, &after, column))
            .collect::<Result<_, _>>()?;

        Ok(format!("SELECT\n{}", columns.join(",\n")))
    }

    fn column(
        &self,
        warehouse: &impl Warehouse,
        after: &str,
        column: &ColumnModel,
    ) -> Result<String, UnbuildableArtifact> {
        let name = warehouse.identifier(column.name());
        let read = warehouse.read(&format!("{after}.{name}"), column.column_type())?;

        Ok(format!("  {read} AS {name}"))
    }
}

impl<L: ChangeLog> Artifact for Snapshot<'_, L> {
    fn statements(&self, warehouse: &impl Warehouse) -> Result<Vec<String>, UnbuildableArtifact> {
        Ok(vec![self.create_table(warehouse)?])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_replay_core::artifact::{Artifact, UnbuildableArtifact};
    use event_replay_core::model::{ColumnModel, ColumnType, TableId, TableModel};
    use event_replay_core::position::{Position, RecordedAt};
    use event_replay_core::sink::{TablePath, UnsupportedColumn, Warehouse};
    use event_replay_core::source::{ChangeLog, ColumnPath};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Offset(u64);

    impl Position for Offset {}

    struct Log;

    impl ChangeLog for Log {
        type Position = Offset;

        fn key(&self) -> ColumnPath {
            ColumnPath::from("_kafka_key")
        }

        fn operation(&self) -> ColumnPath {
            ColumnPath::from("op")
        }

        fn before(&self) -> ColumnPath {
            ColumnPath::from("before")
        }

        fn after(&self) -> ColumnPath {
            ColumnPath::from("after")
        }

        fn recorded_at(&self) -> ColumnPath {
            ColumnPath::from("ts_ms")
        }

        fn ordering(&self) -> Vec<ColumnPath> {
            vec![ColumnPath::from("source.offset")]
        }
    }

    struct Engine;

    impl Warehouse for Engine {
        fn identifier(&self, name: &str) -> String {
            format!("\"{name}\"")
        }

        fn table_ref(&self, table: &TablePath) -> String {
            format!(
                "{}.{}",
                self.identifier(table.namespace()),
                self.identifier(table.name())
            )
        }

        fn column_type(&self, column_type: &ColumnType) -> Result<String, UnsupportedColumn> {
            match column_type {
                ColumnType::Int64 => Ok("BIGINT".to_owned()),
                ColumnType::Text => Ok("TEXT".to_owned()),
                unsupported => Err(UnsupportedColumn::new(unsupported)),
            }
        }

        fn read(
            &self,
            expression: &str,
            column_type: &ColumnType,
        ) -> Result<String, UnsupportedColumn> {
            Ok(format!(
                "CAST({expression} AS {})",
                self.column_type(column_type)?
            ))
        }
    }

    fn column(name: &str, column_type: ColumnType, position: u32) -> ColumnModel {
        ColumnModel::new(name, "", column_type, position, false, None)
    }

    fn customers() -> TableModel {
        TableModel::new(
            TableId::new("shop", "customers"),
            vec![
                column("id", ColumnType::Int64, 1),
                column("name", ColumnType::Text, 2),
            ],
            vec!["id".to_owned()],
        )
    }

    fn window() -> Window {
        Window::new(
            RecordedAt::from(1789800000000),
            RecordedAt::from(1789886400000),
        )
    }

    fn snapshot() -> Snapshot<'static, Log> {
        Snapshot::new(
            &Log,
            customers(),
            TablePath::new("mysql_shop_cdc", "mysql_shop_customers"),
            TablePath::new("mysql_shop", "customers_20260920"),
            window(),
        )
    }

    #[test]
    fn a_first_snapshot_takes_the_last_change_of_each_key_in_the_window() {
        let statements = snapshot().statements(&Engine).unwrap();

        assert_eq!(
            statements[0],
            concat!(
                "CREATE OR REPLACE TABLE \"mysql_shop\".\"customers_20260920\" AS\n",
                "WITH \"changes\" AS (\n",
                "  SELECT *\n",
                "  FROM \"mysql_shop_cdc\".\"mysql_shop_customers\"\n",
                "  WHERE \"ts_ms\" >= 1789800000000 AND \"ts_ms\" < 1789886400000\n",
                "  QUALIFY ROW_NUMBER() OVER (\n",
                "    PARTITION BY \"_kafka_key\".\"id\"\n",
                "    ORDER BY \"source\".\"offset\" DESC\n",
                "  ) = 1\n",
                ")\n",
                "SELECT\n",
                "  CAST(\"after\".\"id\" AS BIGINT) AS \"id\",\n",
                "  CAST(\"after\".\"name\" AS TEXT) AS \"name\"\n",
                "FROM \"changes\"\n",
                "WHERE \"op\" <> 'd'"
            )
        );
    }

    #[test]
    fn a_composite_key_partitions_by_every_column_of_it() {
        let table = TableModel::new(
            TableId::new("shop", "order_items"),
            vec![
                column("order_id", ColumnType::Int64, 1),
                column("line_no", ColumnType::Int64, 2),
            ],
            vec!["order_id".to_owned(), "line_no".to_owned()],
        );
        let snapshot = Snapshot::new(
            &Log,
            table,
            TablePath::new("mysql_shop_cdc", "mysql_shop_order_items"),
            TablePath::new("mysql_shop", "order_items_20260920"),
            window(),
        );

        assert!(snapshot.statements(&Engine).unwrap()[0].contains(
            "    PARTITION BY \"_kafka_key\".\"order_id\", \"_kafka_key\".\"line_no\"\n"
        ));
    }

    #[test]
    fn a_column_the_warehouse_cannot_hold_is_reported() {
        let table = TableModel::new(
            TableId::new("shop", "customers"),
            vec![column("tags", ColumnType::Json, 1)],
            vec!["tags".to_owned()],
        );
        let snapshot = Snapshot::new(
            &Log,
            table,
            TablePath::new("mysql_shop_cdc", "mysql_shop_customers"),
            TablePath::new("mysql_shop", "customers_20260920"),
            window(),
        );

        assert_eq!(
            snapshot.statements(&Engine).unwrap_err(),
            UnbuildableArtifact::from(UnsupportedColumn::new(&ColumnType::Json))
        );
    }

    #[test]
    fn a_table_without_a_primary_key_cannot_be_replayed() {
        let table = TableModel::new(
            TableId::new("shop", "customers"),
            vec![column("id", ColumnType::Int64, 1)],
            Vec::new(),
        );
        let snapshot = Snapshot::new(
            &Log,
            table,
            TablePath::new("mysql_shop_cdc", "mysql_shop_customers"),
            TablePath::new("mysql_shop", "customers_20260920"),
            window(),
        );

        assert_eq!(
            snapshot.statements(&Engine).unwrap_err(),
            UnbuildableArtifact::WithoutPrimaryKey(TableId::new("shop", "customers"))
        );
    }
}
