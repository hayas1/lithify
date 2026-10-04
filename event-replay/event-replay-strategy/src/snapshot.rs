use event_replay_core::artifact::{Artifact, UnbuildableArtifact};
use event_replay_core::model::{ColumnModel, TableModel};
use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::source::RowChangeLog;
use event_replay_core::window::Window;

pub enum Base {
    Captured,
    Shard(TablePath),
}

pub struct Snapshot<'a, L> {
    log: &'a L,
    table: TableModel,
    cdc: TablePath,
    target: TablePath,
    window: Window,
    base: Base,
}

impl<'a, L: RowChangeLog> Snapshot<'a, L> {
    pub fn new(
        log: &'a L,
        table: TableModel,
        cdc: TablePath,
        target: TablePath,
        window: Window,
        base: Base,
    ) -> Self {
        Self {
            log,
            table,
            cdc,
            target,
            window,
            base,
        }
    }

    fn create_table(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        Ok(format!(
            "CREATE OR REPLACE TABLE {} AS\n{}\n{}\nUNION ALL\n{}",
            warehouse.table_ref(&self.target),
            self.reads(warehouse)?,
            self.kept(warehouse)?,
            self.changed(warehouse)?,
        ))
    }

    fn reads(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        Ok(format!(
            "WITH {} AS (\n{}\n),\n{} AS (\n{}\n)",
            warehouse.identifier("base"),
            self.base(warehouse)?,
            warehouse.identifier("changes"),
            self.changes(warehouse)?,
        ))
    }

    fn base(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        match &self.base {
            Base::Shard(shard) => Ok(format!("  SELECT *\n  FROM {}", warehouse.table_ref(shard))),
            Base::Captured => Ok(format!(
                "  SELECT\n{}\n  FROM {}\n  WHERE {} = 'r'\n{}",
                self.casts(warehouse, "    ")?,
                warehouse.change_log_ref(&self.cdc),
                warehouse.path(&self.log.operation()),
                self.latest_of_each_key(warehouse)?,
            )),
        }
    }

    fn changes(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        Ok(format!(
            "  SELECT *\n  FROM {}\n  WHERE {} >= {} AND {} < {}\n{}",
            warehouse.change_log_ref(&self.cdc),
            warehouse.path(&self.log.recorded_at()),
            self.window.from().milliseconds_since_epoch(),
            warehouse.path(&self.log.recorded_at()),
            self.window.to().milliseconds_since_epoch(),
            self.latest_of_each_key(warehouse)?,
        ))
    }

    fn latest_of_each_key(
        &self,
        warehouse: &impl Warehouse,
    ) -> Result<String, UnbuildableArtifact> {
        Ok(format!(
            concat!(
                "  QUALIFY ROW_NUMBER() OVER (\n",
                "    PARTITION BY {}\n",
                "    ORDER BY {}\n",
                "  ) = 1"
            ),
            self.key(warehouse)?,
            self.ordering(warehouse),
        ))
    }

    fn kept(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        Ok(format!(
            concat!(
                "SELECT *\n",
                "FROM {}\n",
                "WHERE NOT EXISTS (\n",
                "  SELECT 1\n",
                "  FROM {}\n",
                "{}\n",
                ")"
            ),
            warehouse.identifier("base"),
            warehouse.identifier("changes"),
            self.changed_key(warehouse)?,
        ))
    }

    fn changed(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        Ok(format!(
            "SELECT\n{}\nFROM {}\nWHERE {} <> 'd'",
            self.casts(warehouse, "  ")?,
            warehouse.identifier("changes"),
            warehouse.path(&self.log.operation()),
        ))
    }

    fn changed_key(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        let changes = warehouse.identifier("changes");
        let base = warehouse.identifier("base");
        let matched: Vec<String> = self
            .primary_key(warehouse)?
            .iter()
            .map(|column| {
                format!(
                    "{} = {base}.{column}",
                    self.keyed(warehouse, &format!("{changes}."), column)
                )
            })
            .collect();

        Ok(matched
            .iter()
            .enumerate()
            .map(|(at, matched)| match at {
                0 => format!("  WHERE {matched}"),
                _ => format!("    AND {matched}"),
            })
            .collect::<Vec<_>>()
            .join("\n"))
    }

    fn key(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        Ok(self
            .primary_key(warehouse)?
            .iter()
            .map(|column| self.keyed(warehouse, "", column))
            .collect::<Vec<_>>()
            .join(", "))
    }

    fn keyed(&self, warehouse: &impl Warehouse, within: &str, column: &str) -> String {
        format!(
            "COALESCE({within}{}.{column}, {within}{}.{column})",
            warehouse.path(&self.log.after()),
            warehouse.path(&self.log.before()),
        )
    }

    fn primary_key(&self, warehouse: &impl Warehouse) -> Result<Vec<String>, UnbuildableArtifact> {
        let columns: Vec<String> = self
            .table
            .primary_key()
            .map(|column| warehouse.identifier(column))
            .collect();

        match columns.is_empty() {
            true => Err(UnbuildableArtifact::WithoutPrimaryKey(
                self.table.id().clone(),
            )),
            false => Ok(columns),
        }
    }

    fn ordering(&self, warehouse: &impl Warehouse) -> String {
        self.log
            .ordering()
            .iter()
            .map(|path| format!("{} DESC", warehouse.path(path)))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn casts(
        &self,
        warehouse: &impl Warehouse,
        indent: &str,
    ) -> Result<String, UnbuildableArtifact> {
        let after = warehouse.path(&self.log.after());
        let columns: Vec<String> = self
            .table
            .columns()
            .map(|column| self.cast(warehouse, &after, column, indent))
            .collect::<Result<_, _>>()?;

        Ok(columns.join(",\n"))
    }

    fn cast(
        &self,
        warehouse: &impl Warehouse,
        after: &str,
        column: &ColumnModel,
        indent: &str,
    ) -> Result<String, UnbuildableArtifact> {
        let name = warehouse.identifier(column.name());
        let read = warehouse.read(&format!("{after}.{name}"), column.column_type())?;

        Ok(format!("{indent}{read} AS {name}"))
    }
}

impl<L: RowChangeLog> Artifact for Snapshot<'_, L> {
    fn statements(&self, warehouse: &impl Warehouse) -> Result<Vec<String>, UnbuildableArtifact> {
        Ok(vec![
            warehouse.create_namespace(self.target.namespace()),
            self.create_table(warehouse)?,
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_replay_core::artifact::{Artifact, UnbuildableArtifact};
    use event_replay_core::model::{ColumnModel, ColumnType, TableId, TableModel};
    use event_replay_core::position::{Position, RecordedAt};
    use event_replay_core::sink::{TablePath, UnsupportedColumn, Warehouse};
    use event_replay_core::source::{ChangeLog, ColumnPath, RowChangeLog};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Offset(u64);

    impl Position for Offset {}

    struct Log;

    impl ChangeLog for Log {
        type Position = Offset;

        fn envelope(&self) -> Option<ColumnPath> {
            None
        }

        fn recorded_at(&self) -> ColumnPath {
            ColumnPath::from("ts_ms")
        }

        fn ordering(&self) -> Vec<ColumnPath> {
            vec![ColumnPath::from("source.offset")]
        }
    }

    impl RowChangeLog for Log {
        fn operation(&self) -> ColumnPath {
            ColumnPath::from("op")
        }

        fn before(&self) -> ColumnPath {
            ColumnPath::from("before")
        }

        fn after(&self) -> ColumnPath {
            ColumnPath::from("after")
        }
    }

    struct Engine;

    impl Warehouse for Engine {
        type Config = ();

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

    struct Lake;

    impl Warehouse for Lake {
        type Config = ();

        fn identifier(&self, name: &str) -> String {
            Engine.identifier(name)
        }

        fn table_ref(&self, table: &TablePath) -> String {
            Engine.table_ref(table)
        }

        fn change_log_ref(&self, table: &TablePath) -> String {
            format!(
                "read_parquet('{}/{}/*.parquet')",
                table.namespace(),
                table.name()
            )
        }

        fn column_type(&self, column_type: &ColumnType) -> Result<String, UnsupportedColumn> {
            Engine.column_type(column_type)
        }

        fn read(
            &self,
            expression: &str,
            column_type: &ColumnType,
        ) -> Result<String, UnsupportedColumn> {
            Engine.read(expression, column_type)
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

    fn snapshot(base: Base) -> Snapshot<'static, Log> {
        Snapshot::new(
            &Log,
            customers(),
            TablePath::new("mysql_shop_cdc", "mysql_shop_customers"),
            TablePath::new("mysql_shop", "customers_20260920"),
            window(),
            base,
        )
    }

    #[test]
    fn a_snapshot_reads_the_change_log_where_the_warehouse_reads_change_logs() {
        let statement = &snapshot(Base::Captured).statements(&Lake).unwrap()[1];

        assert!(
            statement
                .contains("FROM read_parquet('mysql_shop_cdc/mysql_shop_customers/*.parquet')")
        );
        assert!(!statement.contains("\"mysql_shop_cdc\".\"mysql_shop_customers\""));
    }

    #[test]
    fn a_snapshot_is_built_into_a_namespace_made_for_it() {
        let statements = snapshot(Base::Captured).statements(&Engine).unwrap();

        assert_eq!(statements[0], "CREATE SCHEMA IF NOT EXISTS \"mysql_shop\"");
    }

    #[test]
    fn a_first_snapshot_folds_the_window_onto_what_the_capture_read() {
        let statements = snapshot(Base::Captured).statements(&Engine).unwrap();

        assert_eq!(
            statements[1],
            concat!(
                "CREATE OR REPLACE TABLE \"mysql_shop\".\"customers_20260920\" AS\n",
                "WITH \"base\" AS (\n",
                "  SELECT\n",
                "    CAST(\"after\".\"id\" AS BIGINT) AS \"id\",\n",
                "    CAST(\"after\".\"name\" AS TEXT) AS \"name\"\n",
                "  FROM \"mysql_shop_cdc\".\"mysql_shop_customers\"\n",
                "  WHERE \"op\" = 'r'\n",
                "  QUALIFY ROW_NUMBER() OVER (\n",
                "    PARTITION BY COALESCE(\"after\".\"id\", \"before\".\"id\")\n",
                "    ORDER BY \"source\".\"offset\" DESC\n",
                "  ) = 1\n",
                "),\n",
                "\"changes\" AS (\n",
                "  SELECT *\n",
                "  FROM \"mysql_shop_cdc\".\"mysql_shop_customers\"\n",
                "  WHERE \"ts_ms\" >= 1789800000000 AND \"ts_ms\" < 1789886400000\n",
                "  QUALIFY ROW_NUMBER() OVER (\n",
                "    PARTITION BY COALESCE(\"after\".\"id\", \"before\".\"id\")\n",
                "    ORDER BY \"source\".\"offset\" DESC\n",
                "  ) = 1\n",
                ")\n",
                "SELECT *\n",
                "FROM \"base\"\n",
                "WHERE NOT EXISTS (\n",
                "  SELECT 1\n",
                "  FROM \"changes\"\n",
                "  WHERE COALESCE(\"changes\".\"after\".\"id\", \"changes\".\"before\".\"id\") = \"base\".\"id\"\n",
                ")\n",
                "UNION ALL\n",
                "SELECT\n",
                "  CAST(\"after\".\"id\" AS BIGINT) AS \"id\",\n",
                "  CAST(\"after\".\"name\" AS TEXT) AS \"name\"\n",
                "FROM \"changes\"\n",
                "WHERE \"op\" <> 'd'"
            )
        );
    }

    #[test]
    fn a_later_snapshot_folds_the_window_onto_the_snapshot_before_it() {
        let base = Base::Shard(TablePath::new("mysql_shop", "customers_20260919"));

        let statements = snapshot(base).statements(&Engine).unwrap();

        assert!(statements[1].starts_with(concat!(
            "CREATE OR REPLACE TABLE \"mysql_shop\".\"customers_20260920\" AS\n",
            "WITH \"base\" AS (\n",
            "  SELECT *\n",
            "  FROM \"mysql_shop\".\"customers_20260919\"\n",
            "),\n",
            "\"changes\" AS (\n"
        )));
    }

    #[test]
    fn a_key_the_window_changed_is_taken_from_the_window_instead_of_the_base() {
        let statements = snapshot(Base::Captured).statements(&Engine).unwrap();

        assert!(statements[1].contains(concat!(
            "WHERE NOT EXISTS (\n",
            "  SELECT 1\n",
            "  FROM \"changes\"\n",
            "  WHERE COALESCE(\"changes\".\"after\".\"id\", \"changes\".\"before\".\"id\") = \"base\".\"id\"\n",
            ")\n",
            "UNION ALL\n"
        )));
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
            Base::Captured,
        );

        let statement = &snapshot.statements(&Engine).unwrap()[1];

        assert!(statement.contains(concat!(
            "    PARTITION BY COALESCE(\"after\".\"order_id\", \"before\".\"order_id\"), ",
            "COALESCE(\"after\".\"line_no\", \"before\".\"line_no\")\n"
        )));
        assert!(statement.contains(concat!(
            concat!(
                "  WHERE COALESCE(\"changes\".\"after\".\"order_id\", \"changes\".\"before\".\"order_id\")",
                " = \"base\".\"order_id\"\n"
            ),
            concat!(
                "    AND COALESCE(\"changes\".\"after\".\"line_no\", \"changes\".\"before\".\"line_no\")",
                " = \"base\".\"line_no\"\n"
            )
        )));
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
            Base::Captured,
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
            Base::Captured,
        );

        assert_eq!(
            snapshot.statements(&Engine).unwrap_err(),
            UnbuildableArtifact::WithoutPrimaryKey(TableId::new("shop", "customers"))
        );
    }
}
