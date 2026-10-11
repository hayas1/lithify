use event_replay_core::artifact::{Artifact, UnbuildableArtifact};
use event_replay_core::era::{Era, Lineage};
use event_replay_core::model::{ColumnModel, TableModel};
use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::source::RowChangeLog;

pub enum Base {
    Captured,
    Shard(TablePath),
}

impl Base {
    fn select<L: RowChangeLog>(
        &self,
        snapshot: &Snapshot<'_, L>,
        warehouse: &impl Warehouse,
        changes: &Origin,
        keys: &Keys,
        ordering: &Ordering,
    ) -> Result<String, UnbuildableArtifact> {
        let first = snapshot.first()?;

        match self {
            Self::Shard(shard) => Ok(format!(
                "  SELECT\n{}\n  FROM {}",
                snapshot.projected(warehouse, first, &Origin::Table, "    ")?,
                warehouse.table_ref(shard),
            )),
            Self::Captured => Ok(format!(
                "  SELECT\n{}\n  FROM {}\n  WHERE {} = 'r'\n{}",
                snapshot.projected(warehouse, first, changes, "    ")?,
                warehouse.change_log_ref(&snapshot.cdc),
                warehouse.path(&snapshot.log.operation()),
                Snapshot::<L>::latest(&keys.read, &ordering.read),
            )),
        }
    }
}

enum Origin {
    ChangeLog(String),
    Table,
}

impl Origin {
    fn read(
        &self,
        warehouse: &impl Warehouse,
        name: &str,
        column: &ColumnModel,
    ) -> Result<String, UnbuildableArtifact> {
        match self {
            Self::ChangeLog(after) => {
                Ok(warehouse.read(&format!("{after}.{name}"), column.column_type())?)
            }
            Self::Table => Ok(name.to_owned()),
        }
    }
}

struct Keys {
    columns: Vec<String>,
    read: Vec<String>,
    aliases: Vec<String>,
}

struct Ordering {
    read: Vec<String>,
    aliases: Vec<String>,
}

pub struct Snapshot<'a, L> {
    log: &'a L,
    lineage: Lineage,
    cdc: TablePath,
    target: TablePath,
    base: Base,
}

impl<'a, L: RowChangeLog> Snapshot<'a, L> {
    pub fn new(
        log: &'a L,
        lineage: Lineage,
        cdc: TablePath,
        target: TablePath,
        base: Base,
    ) -> Self {
        Self {
            log,
            lineage,
            cdc,
            target,
            base,
        }
    }

    fn table(&self) -> &TableModel {
        self.lineage.table()
    }

    fn first(&self) -> Result<&TableModel, UnbuildableArtifact> {
        self.lineage
            .first()
            .map(Era::table)
            .ok_or_else(|| UnbuildableArtifact::WithoutEra(self.table().id().clone()))
    }

    fn after(&self, warehouse: &impl Warehouse) -> String {
        warehouse.path(&self.log.after())
    }

    fn create_table(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        let keys = self.keys(warehouse)?;
        let ordering = self.ordering(warehouse);
        let changes = Origin::ChangeLog(self.after(warehouse));

        Ok(format!(
            "CREATE OR REPLACE TABLE {} AS\nWITH {} AS (\n{}\n),\n{} AS (\n{}\n)\n{}\nUNION ALL\n{}",
            warehouse.table_ref(&self.target),
            Self::base_name(warehouse),
            self.base
                .select(self, warehouse, &changes, &keys, &ordering)?,
            Self::changes_name(warehouse),
            self.changes(warehouse, &changes, &keys, &ordering)?,
            self.kept(warehouse, &keys),
            self.changed(warehouse),
        ))
    }

    fn latest(partition: &[String], ordering: &[String]) -> String {
        format!(
            concat!(
                "  QUALIFY ROW_NUMBER() OVER (\n",
                "    PARTITION BY {}\n",
                "    ORDER BY {}\n",
                "  ) = 1"
            ),
            partition.join(", "),
            ordering
                .iter()
                .map(|column| format!("{column} DESC"))
                .collect::<Vec<_>>()
                .join(", "),
        )
    }

    fn changes(
        &self,
        warehouse: &impl Warehouse,
        origin: &Origin,
        keys: &Keys,
        ordering: &Ordering,
    ) -> Result<String, UnbuildableArtifact> {
        let eras: Vec<String> = self
            .lineage
            .eras()
            .map(|era| self.era(warehouse, era, origin, keys, ordering))
            .collect::<Result<_, _>>()?;

        Ok(format!(
            "  SELECT *\n  FROM (\n{}\n  )\n{}",
            eras.join("\n    UNION ALL\n"),
            Self::latest(&keys.aliases, &ordering.aliases),
        ))
    }

    fn era(
        &self,
        warehouse: &impl Warehouse,
        era: &Era,
        origin: &Origin,
        keys: &Keys,
        ordering: &Ordering,
    ) -> Result<String, UnbuildableArtifact> {
        self.guard_key(era.table())?;

        let recorded_at = warehouse.path(&self.log.recorded_at());

        Ok(format!(
            "    SELECT\n{},\n{}\n    FROM {}\n    WHERE {recorded_at} >= {} AND {recorded_at} < {}",
            self.bookkeeping(warehouse, keys, ordering).join(",\n"),
            self.projected(warehouse, era.table(), origin, "      ")?,
            warehouse.change_log_ref(&self.cdc),
            era.window().from().milliseconds_since_epoch(),
            era.window().to().milliseconds_since_epoch(),
        ))
    }

    fn bookkeeping(
        &self,
        warehouse: &impl Warehouse,
        keys: &Keys,
        ordering: &Ordering,
    ) -> Vec<String> {
        std::iter::once((
            warehouse.path(&self.log.operation()),
            self.operation(warehouse),
        ))
        .chain(
            ordering
                .read
                .iter()
                .cloned()
                .zip(ordering.aliases.iter().cloned()),
        )
        .chain(keys.read.iter().cloned().zip(keys.aliases.iter().cloned()))
        .map(|(value, alias)| format!("      {value} AS {alias}"))
        .collect()
    }

    fn kept(&self, warehouse: &impl Warehouse, keys: &Keys) -> String {
        let changes = Self::changes_name(warehouse);
        let base = Self::base_name(warehouse);
        let matched: Vec<String> = keys
            .aliases
            .iter()
            .zip(&keys.columns)
            .enumerate()
            .map(|(at, (alias, column))| {
                let matched = format!("{changes}.{alias} = {base}.{column}");

                match at {
                    0 => format!("  WHERE {matched}"),
                    _ => format!("    AND {matched}"),
                }
            })
            .collect();

        format!(
            "SELECT *\nFROM {base}\nWHERE NOT EXISTS (\n  SELECT 1\n  FROM {changes}\n{}\n)",
            matched.join("\n"),
        )
    }

    fn changed(&self, warehouse: &impl Warehouse) -> String {
        let columns: Vec<String> = self
            .table()
            .columns()
            .map(|column| format!("  {}", warehouse.identifier(column.name())))
            .collect();

        format!(
            "SELECT\n{}\nFROM {}\nWHERE {} <> 'd'",
            columns.join(",\n"),
            Self::changes_name(warehouse),
            self.operation(warehouse),
        )
    }

    fn base_name(warehouse: &impl Warehouse) -> String {
        warehouse.identifier("base")
    }

    fn changes_name(warehouse: &impl Warehouse) -> String {
        warehouse.identifier("changes")
    }

    fn operation(&self, warehouse: &impl Warehouse) -> String {
        warehouse.identifier("__replay_operation")
    }

    fn aliases(warehouse: &impl Warehouse, role: &str, count: usize) -> Vec<String> {
        (0..count)
            .map(|at| warehouse.identifier(&format!("__replay_{role}_{at}")))
            .collect()
    }

    fn keys(&self, warehouse: &impl Warehouse) -> Result<Keys, UnbuildableArtifact> {
        let columns: Vec<String> = self
            .table()
            .primary_key()
            .map(|column| warehouse.identifier(column))
            .collect();

        if columns.is_empty() {
            return Err(UnbuildableArtifact::WithoutPrimaryKey(
                self.table().id().clone(),
            ));
        }

        let after = self.after(warehouse);
        let before = warehouse.path(&self.log.before());

        Ok(Keys {
            read: columns
                .iter()
                .map(|column| format!("COALESCE({after}.{column}, {before}.{column})"))
                .collect(),
            aliases: Self::aliases(warehouse, "key", columns.len()),
            columns,
        })
    }

    fn ordering(&self, warehouse: &impl Warehouse) -> Ordering {
        let read: Vec<String> = self
            .log
            .ordering()
            .iter()
            .map(|path| warehouse.path(path))
            .collect();

        Ordering {
            aliases: Self::aliases(warehouse, "order", read.len()),
            read,
        }
    }

    fn guard_key(&self, era: &TableModel) -> Result<(), UnbuildableArtifact> {
        match era.primary_key().eq(self.table().primary_key()) {
            true => Ok(()),
            false => Err(UnbuildableArtifact::PrimaryKeyChanged(
                self.table().id().clone(),
            )),
        }
    }

    fn projected(
        &self,
        warehouse: &impl Warehouse,
        era: &TableModel,
        origin: &Origin,
        indent: &str,
    ) -> Result<String, UnbuildableArtifact> {
        let columns: Vec<String> = self
            .table()
            .columns()
            .map(|column| {
                let name = warehouse.identifier(column.name());
                let value = Self::project(warehouse, column, era, origin, &name)?;

                Ok(format!("{indent}{value} AS {name}"))
            })
            .collect::<Result<_, UnbuildableArtifact>>()?;

        Ok(columns.join(",\n"))
    }

    fn project(
        warehouse: &impl Warehouse,
        column: &ColumnModel,
        era: &TableModel,
        origin: &Origin,
        name: &str,
    ) -> Result<String, UnbuildableArtifact> {
        match era.column(column.name()) {
            Some(was) if was.column_type() == column.column_type() => {
                origin.read(warehouse, name, was)
            }
            Some(was) => Ok(format!(
                "CAST({} AS {})",
                origin.read(warehouse, name, was)?,
                warehouse.column_type(column.column_type())?,
            )),
            None => Ok(format!(
                "CAST({} AS {})",
                column
                    .default_value()
                    .map(|default| warehouse.literal(default))
                    .unwrap_or_else(|| "NULL".to_owned()),
                warehouse.column_type(column.column_type())?,
            )),
        }
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
    use event_replay_core::era::{Era, Lineage};
    use event_replay_core::model::{ColumnModel, ColumnType, TableId, TableModel};
    use event_replay_core::position::{Position, RecordedAt};
    use event_replay_core::sink::{TablePath, UnsupportedColumn, Warehouse};
    use event_replay_core::source::{ChangeLog, ColumnPath, RowChangeLog};
    use event_replay_core::window::Window;

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

    fn window(from: i64, to: i64) -> Window {
        Window::new(RecordedAt::from(from), RecordedAt::from(to))
    }

    fn whole(table: TableModel) -> Lineage {
        Lineage::new(table.clone(), vec![Era::new(table, window(100, 200))])
    }

    fn snapshot_of(lineage: Lineage, base: Base) -> Snapshot<'static, Log> {
        Snapshot::new(
            &Log,
            lineage,
            TablePath::new("mysql_shop_cdc", "mysql_shop_customers"),
            TablePath::new("mysql_shop", "customers_20260920"),
            base,
        )
    }

    fn snapshot(base: Base) -> Snapshot<'static, Log> {
        snapshot_of(whole(customers()), base)
    }

    fn widened() -> Lineage {
        let email = ColumnModel::new("email", "", ColumnType::Text, 3, true, Some("none"));
        let after = TableModel::new(
            TableId::new("shop", "customers"),
            vec![
                column("id", ColumnType::Int64, 1),
                column("name", ColumnType::Text, 2),
                email,
            ],
            vec!["id".to_owned()],
        );

        Lineage::new(
            after.clone(),
            vec![
                Era::new(customers(), window(100, 150)),
                Era::new(after, window(150, 200)),
            ],
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
                "  FROM (\n",
                "    SELECT\n",
                "      \"op\" AS \"__replay_operation\",\n",
                "      \"source\".\"offset\" AS \"__replay_order_0\",\n",
                "      COALESCE(\"after\".\"id\", \"before\".\"id\") AS \"__replay_key_0\",\n",
                "      CAST(\"after\".\"id\" AS BIGINT) AS \"id\",\n",
                "      CAST(\"after\".\"name\" AS TEXT) AS \"name\"\n",
                "    FROM \"mysql_shop_cdc\".\"mysql_shop_customers\"\n",
                "    WHERE \"ts_ms\" >= 100 AND \"ts_ms\" < 200\n",
                "  )\n",
                "  QUALIFY ROW_NUMBER() OVER (\n",
                "    PARTITION BY \"__replay_key_0\"\n",
                "    ORDER BY \"__replay_order_0\" DESC\n",
                "  ) = 1\n",
                ")\n",
                "SELECT *\n",
                "FROM \"base\"\n",
                "WHERE NOT EXISTS (\n",
                "  SELECT 1\n",
                "  FROM \"changes\"\n",
                "  WHERE \"changes\".\"__replay_key_0\" = \"base\".\"id\"\n",
                ")\n",
                "UNION ALL\n",
                "SELECT\n",
                "  \"id\",\n",
                "  \"name\"\n",
                "FROM \"changes\"\n",
                "WHERE \"__replay_operation\" <> 'd'"
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
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    \"name\" AS \"name\"\n",
            "  FROM \"mysql_shop\".\"customers_20260919\"\n",
            "),\n",
            "\"changes\" AS (\n"
        )));
    }

    #[test]
    fn each_era_is_read_in_its_own_schema_and_laid_out_in_the_last_one() {
        let statement = &snapshot_of(widened(), Base::Captured)
            .statements(&Engine)
            .unwrap()[1];

        assert!(statement.contains(concat!(
            "      CAST(\"after\".\"name\" AS TEXT) AS \"name\",\n",
            "      CAST('none' AS TEXT) AS \"email\"\n",
            "    FROM \"mysql_shop_cdc\".\"mysql_shop_customers\"\n",
            "    WHERE \"ts_ms\" >= 100 AND \"ts_ms\" < 150\n",
            "    UNION ALL\n",
        )));
        assert!(statement.contains(concat!(
            "      CAST(\"after\".\"email\" AS TEXT) AS \"email\"\n",
            "    FROM \"mysql_shop_cdc\".\"mysql_shop_customers\"\n",
            "    WHERE \"ts_ms\" >= 150 AND \"ts_ms\" < 200\n",
        )));
    }

    #[test]
    fn a_base_from_before_a_column_was_added_fills_the_column_in() {
        let base = Base::Shard(TablePath::new("mysql_shop", "customers_20260919"));
        let statement = &snapshot_of(widened(), base).statements(&Engine).unwrap()[1];

        assert!(statement.contains(concat!(
            "    \"name\" AS \"name\",\n",
            "    CAST('none' AS TEXT) AS \"email\"\n",
            "  FROM \"mysql_shop\".\"customers_20260919\"\n",
        )));
    }

    #[test]
    fn a_column_retyped_within_the_window_is_cast_to_its_last_type() {
        let after = TableModel::new(
            TableId::new("shop", "customers"),
            vec![
                column("id", ColumnType::Int64, 1),
                column("name", ColumnType::Int64, 2),
            ],
            vec!["id".to_owned()],
        );
        let lineage = Lineage::new(
            after.clone(),
            vec![
                Era::new(customers(), window(100, 150)),
                Era::new(after, window(150, 200)),
            ],
        );

        let statement = &snapshot_of(lineage, Base::Captured)
            .statements(&Engine)
            .unwrap()[1];

        assert!(
            statement
                .contains("      CAST(CAST(\"after\".\"name\" AS TEXT) AS BIGINT) AS \"name\"\n")
        );
    }

    #[test]
    fn a_table_whose_primary_key_changed_within_the_window_cannot_be_replayed() {
        let after = TableModel::new(
            TableId::new("shop", "customers"),
            vec![
                column("id", ColumnType::Int64, 1),
                column("name", ColumnType::Text, 2),
            ],
            vec!["name".to_owned()],
        );
        let lineage = Lineage::new(
            after.clone(),
            vec![
                Era::new(customers(), window(100, 150)),
                Era::new(after, window(150, 200)),
            ],
        );

        assert_eq!(
            snapshot_of(lineage, Base::Captured)
                .statements(&Engine)
                .unwrap_err(),
            UnbuildableArtifact::PrimaryKeyChanged(TableId::new("shop", "customers"))
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

        let statement = &snapshot_of(whole(table), Base::Captured)
            .statements(&Engine)
            .unwrap()[1];

        assert!(statement.contains(concat!(
            "      COALESCE(\"after\".\"order_id\", \"before\".\"order_id\") AS \"__replay_key_0\",\n",
            "      COALESCE(\"after\".\"line_no\", \"before\".\"line_no\") AS \"__replay_key_1\",\n",
        )));
        assert!(statement.contains("    PARTITION BY \"__replay_key_0\", \"__replay_key_1\"\n"));
        assert!(statement.contains(concat!(
            "  WHERE \"changes\".\"__replay_key_0\" = \"base\".\"order_id\"\n",
            "    AND \"changes\".\"__replay_key_1\" = \"base\".\"line_no\"\n",
        )));
    }

    #[test]
    fn a_column_the_warehouse_cannot_hold_is_reported() {
        let table = TableModel::new(
            TableId::new("shop", "customers"),
            vec![column("tags", ColumnType::Json, 1)],
            vec!["tags".to_owned()],
        );

        assert_eq!(
            snapshot_of(whole(table), Base::Captured)
                .statements(&Engine)
                .unwrap_err(),
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

        assert_eq!(
            snapshot_of(whole(table), Base::Captured)
                .statements(&Engine)
                .unwrap_err(),
            UnbuildableArtifact::WithoutPrimaryKey(TableId::new("shop", "customers"))
        );
    }
}
