use std::marker::PhantomData;

use event_replay_core::artifact::{Artifact, UnbuildableArtifact};
use event_replay_core::era::Era;
use event_replay_core::listener::{
    ColumnAdded, ColumnDropped, ColumnRenamed, ColumnRetyped, PrimaryKeyChanged,
    SchemaChangeListener, TableRenamed,
};
use event_replay_core::model::{ColumnModel, TableId, TableModel};
use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::source::RowChangeLog;
use event_replay_core::sql::SqlString;

pub enum Base {
    Captured,
    Shard(TablePath),
}

impl Base {
    fn select<L: RowChangeLog, W: Warehouse>(
        &self,
        warehouse: &W,
        snapshot: &Snapshot<'_, L, W>,
        table: &TableModel,
    ) -> Result<SqlString, UnbuildableArtifact> {
        match self {
            Self::Shard(shard) => Ok(SqlString::from(format!(
                "  SELECT\n{}\n  FROM {}",
                Snapshot::<L, W>::columns(warehouse, table, "    "),
                warehouse.table_ref(shard),
            ))),
            Self::Captured => Ok(SqlString::from(format!(
                "  SELECT\n{}\n  FROM {}\n  WHERE {} = 'r'\n{}",
                snapshot.reads(warehouse, table, "    ")?,
                warehouse.change_log_ref(&snapshot.cdc),
                warehouse.path(&snapshot.log.operation()),
                snapshot.latest(warehouse, table),
            ))),
        }
    }
}

pub struct Snapshot<'a, L, W> {
    log: &'a L,
    table: TableId,
    cdc: TablePath,
    target: TablePath,
    base: Base,
    folding: Option<Folding>,
    relations: Vec<(SqlString, SqlString)>,
    steps: usize,
    warehouse: PhantomData<fn(&W)>,
}

struct Folding {
    namespace: SqlString,
    target: SqlString,
}

impl<'a, L: RowChangeLog, W: Warehouse> Snapshot<'a, L, W> {
    pub fn new(log: &'a L, table: TableId, cdc: TablePath, target: TablePath, base: Base) -> Self {
        Self {
            log,
            table,
            cdc,
            target,
            base,
            folding: None,
            relations: Vec::new(),
            steps: 0,
            warehouse: PhantomData,
        }
    }

    pub fn table(&self) -> &TableId {
        &self.table
    }

    fn begin(&mut self, warehouse: &W, table: &TableModel) -> Result<(), UnbuildableArtifact> {
        if self.folding.is_some() {
            return Ok(());
        }
        if table.primary_key().next().is_none() {
            return Err(UnbuildableArtifact::WithoutPrimaryKey(table.id().clone()));
        }

        let base = self.base.select(warehouse, self, table)?;

        self.relations.push((warehouse.identifier("base"), base));
        self.folding = Some(Folding {
            namespace: warehouse.create_namespace(self.target.namespace()),
            target: warehouse.table_ref(&self.target),
        });

        Ok(())
    }

    fn previous(&self) -> Result<SqlString, UnbuildableArtifact> {
        self.relations
            .last()
            .map(|(name, _)| name.clone())
            .ok_or_else(|| self.without_era())
    }

    fn without_era(&self) -> UnbuildableArtifact {
        UnbuildableArtifact::WithoutEra(self.table.clone())
    }

    fn fold_era(&mut self, warehouse: &W, era: &Era) -> Result<(), UnbuildableArtifact> {
        self.begin(warehouse, era.table())?;
        self.steps += 1;

        let previous = self.previous()?;
        let changes = warehouse.identifier(&format!("changes_{}", self.steps));
        let folded = warehouse.identifier(&format!("era_{}", self.steps));
        let fold = self.fold(warehouse, era.table(), &previous, &changes)?;

        self.relations.push((changes, self.changes(warehouse, era)));
        self.relations.push((folded, fold));

        Ok(())
    }

    fn carry(
        &mut self,
        warehouse: &W,
        next: &TableModel,
        carried: impl Fn(&ColumnModel, &SqlString) -> Result<SqlString, UnbuildableArtifact>,
    ) -> Result<(), UnbuildableArtifact> {
        self.steps += 1;

        let previous = self.previous()?;
        let columns: Vec<String> = next
            .columns()
            .map(|column| {
                let name = warehouse.identifier(column.name());

                Ok(format!("    {} AS {name}", carried(column, &name)?))
            })
            .collect::<Result<_, UnbuildableArtifact>>()?;

        self.relations.push((
            warehouse.identifier(&format!("era_{}", self.steps)),
            SqlString::from(format!(
                "  SELECT\n{}\n  FROM {previous}",
                columns.join(",\n")
            )),
        ));

        Ok(())
    }

    fn fill(warehouse: &W, column: &ColumnModel) -> Result<SqlString, UnbuildableArtifact> {
        Ok(SqlString::from(format!(
            "CAST({} AS {})",
            column
                .default_value()
                .map(|default| warehouse.literal(default))
                .unwrap_or_else(|| SqlString::from("NULL")),
            warehouse.column_type(column.column_type())?,
        )))
    }

    fn with(folding: &Folding, relations: &[(SqlString, SqlString)]) -> SqlString {
        let ctes: Vec<String> = relations
            .iter()
            .map(|(name, select)| format!("{name} AS (\n{select}\n)"))
            .collect();
        let last = relations
            .last()
            .map(|(name, _)| name.as_str())
            .unwrap_or_default();

        SqlString::from(format!(
            "CREATE OR REPLACE TABLE {} AS\nWITH {}\nSELECT * FROM {last}",
            folding.target,
            ctes.join(",\n"),
        ))
    }

    fn changes(&self, warehouse: &W, era: &Era) -> SqlString {
        let recorded_at = warehouse.path(&self.log.recorded_at());

        SqlString::from(format!(
            "  SELECT *\n  FROM {}\n  WHERE {recorded_at} >= {} AND {recorded_at} < {}\n{}",
            warehouse.change_log_ref(&self.cdc),
            era.window().from().milliseconds_since_epoch(),
            era.window().to().milliseconds_since_epoch(),
            self.latest(warehouse, era.table()),
        ))
    }

    fn fold(
        &self,
        warehouse: &W,
        table: &TableModel,
        previous: &SqlString,
        changes: &SqlString,
    ) -> Result<SqlString, UnbuildableArtifact> {
        Ok(SqlString::from(format!(
            concat!(
                "  SELECT\n{}\n  FROM {previous}\n",
                "  WHERE NOT EXISTS (\n    SELECT 1\n    FROM {changes}\n{}\n  )\n",
                "  UNION ALL\n",
                "  SELECT\n{}\n  FROM {changes}\n  WHERE {} <> 'd'"
            ),
            Self::columns(warehouse, table, "    "),
            self.matched(warehouse, table, previous, changes),
            self.reads(warehouse, table, "    ")?,
            warehouse.path(&self.log.operation()),
            previous = previous,
            changes = changes,
        )))
    }

    fn matched(
        &self,
        warehouse: &W,
        table: &TableModel,
        previous: &SqlString,
        changes: &SqlString,
    ) -> SqlString {
        let matched = table
            .primary_key()
            .map(|column| warehouse.identifier(column))
            .enumerate()
            .map(|(at, column)| {
                let matched = format!(
                    "{} = {previous}.{column}",
                    self.keyed(warehouse, Some(changes), &column)
                );

                match at {
                    0 => format!("    WHERE {matched}"),
                    _ => format!("      AND {matched}"),
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        SqlString::from(matched)
    }

    fn latest(&self, warehouse: &W, table: &TableModel) -> SqlString {
        let partition: Vec<String> = table
            .primary_key()
            .map(|column| {
                self.keyed(warehouse, None, &warehouse.identifier(column))
                    .to_string()
            })
            .collect();
        let ordering: Vec<String> = self
            .log
            .ordering()
            .iter()
            .map(|path| format!("{} DESC", warehouse.path(path)))
            .collect();

        SqlString::from(format!(
            concat!(
                "  QUALIFY ROW_NUMBER() OVER (\n",
                "    PARTITION BY {}\n",
                "    ORDER BY {}\n",
                "  ) = 1"
            ),
            partition.join(", "),
            ordering.join(", "),
        ))
    }

    fn keyed(&self, warehouse: &W, within: Option<&SqlString>, column: &SqlString) -> SqlString {
        let within = within
            .map(|relation| format!("{relation}."))
            .unwrap_or_default();

        SqlString::from(format!(
            "COALESCE({within}{}.{column}, {within}{}.{column})",
            warehouse.path(&self.log.after()),
            warehouse.path(&self.log.before()),
        ))
    }

    fn columns(warehouse: &W, table: &TableModel, indent: &str) -> SqlString {
        SqlString::from(
            table
                .columns()
                .map(|column| format!("{indent}{}", warehouse.identifier(column.name())))
                .collect::<Vec<_>>()
                .join(",\n"),
        )
    }

    fn reads(
        &self,
        warehouse: &W,
        table: &TableModel,
        indent: &str,
    ) -> Result<SqlString, UnbuildableArtifact> {
        let after = warehouse.path(&self.log.after());
        let columns: Vec<String> = table
            .columns()
            .map(|column| {
                let name = warehouse.identifier(column.name());
                let read = warehouse.read(
                    &SqlString::from(format!("{after}.{name}")),
                    column.column_type(),
                )?;

                Ok(format!("{indent}{read} AS {name}"))
            })
            .collect::<Result<_, UnbuildableArtifact>>()?;

        Ok(SqlString::from(columns.join(",\n")))
    }
}

impl<P, L: RowChangeLog, W: Warehouse> SchemaChangeListener<P> for Snapshot<'_, L, W> {
    type Context = W;
    type Error = UnbuildableArtifact;

    fn visit_era(&mut self, warehouse: &W, era: &Era) -> Result<(), UnbuildableArtifact> {
        self.fold_era(warehouse, era)
    }

    // TODO: follow a renamed table by reading each era from the sink table of the name the table
    // had in it. The sink names its tables after the table id (Sink::row_changes), yet a snapshot
    // is handed one sink table when it opens; it needs a way to name the sink table of the id the
    // rename leads to, and the target has to be named after one of the names. Until then a rename
    // fails the snapshot rather than losing the rows changed before it.
    fn visit_renamed(
        &mut self,
        _: &W,
        event: &TableRenamed<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        Err(UnbuildableArtifact::Renamed(event.before().id().clone()))
    }

    fn visit_column_renamed(
        &mut self,
        warehouse: &W,
        event: &ColumnRenamed<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        self.begin(warehouse, event.before())?;

        let renamed = event.column();
        let previous = warehouse.identifier(event.previous().name());

        self.carry(warehouse, event.after(), |column, name| {
            match column.name() == renamed.name() {
                true => Ok(previous.clone()),
                false => Ok(name.clone()),
            }
        })
    }

    fn visit_column_added(
        &mut self,
        warehouse: &W,
        event: &ColumnAdded<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        self.begin(warehouse, event.before())?;

        let added = event.column();

        self.carry(warehouse, event.after(), |column, name| {
            match column.name() == added.name() {
                true => Self::fill(warehouse, column),
                false => Ok(name.clone()),
            }
        })
    }

    fn visit_column_dropped(
        &mut self,
        warehouse: &W,
        event: &ColumnDropped<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        self.begin(warehouse, event.before())?;

        self.carry(warehouse, event.after(), |_, name| Ok(name.clone()))
    }

    fn visit_column_retyped(
        &mut self,
        warehouse: &W,
        event: &ColumnRetyped<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        self.begin(warehouse, event.before())?;

        let retyped = event.column();

        self.carry(warehouse, event.after(), |column, name| {
            match column.name() == retyped.name() {
                true => Ok(SqlString::from(format!(
                    "CAST({name} AS {})",
                    warehouse.column_type(column.column_type())?
                ))),
                false => Ok(name.clone()),
            }
        })
    }

    fn visit_primary_key_changed(
        &mut self,
        _: &W,
        event: &PrimaryKeyChanged<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        Err(UnbuildableArtifact::PrimaryKeyChanged(
            event.after().id().clone(),
        ))
    }
}

impl<L: RowChangeLog, W: Warehouse> Artifact for Snapshot<'_, L, W> {
    fn statements(&self) -> Result<Vec<SqlString>, UnbuildableArtifact> {
        match &self.folding {
            Some(folding) => Ok(vec![
                folding.namespace.clone(),
                Self::with(folding, &self.relations),
            ]),
            None => Err(self.without_era()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_replay_core::artifact::{Artifact, UnbuildableArtifact};
    use event_replay_core::era::EraTracker;
    use event_replay_core::event::{SchemaChange, TableChange};
    use event_replay_core::listener::{CatalogModel, OutOfOrder};
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

        fn identifier(&self, name: &str) -> SqlString {
            SqlString::from(format!("\"{name}\""))
        }

        fn table_ref(&self, table: &TablePath) -> SqlString {
            SqlString::from(format!(
                "{}.{}",
                self.identifier(table.namespace()),
                self.identifier(table.name())
            ))
        }

        fn column_type(&self, column_type: &ColumnType) -> Result<SqlString, UnsupportedColumn> {
            match column_type {
                ColumnType::Int64 => Ok(SqlString::from("BIGINT")),
                ColumnType::Text => Ok(SqlString::from("TEXT")),
                unsupported => Err(UnsupportedColumn::new(unsupported)),
            }
        }

        fn read(
            &self,
            expression: &SqlString,
            column_type: &ColumnType,
        ) -> Result<SqlString, UnsupportedColumn> {
            Ok(SqlString::from(format!(
                "CAST({expression} AS {})",
                self.column_type(column_type)?
            )))
        }
    }

    struct Lake;

    impl Warehouse for Lake {
        type Config = ();

        fn identifier(&self, name: &str) -> SqlString {
            Engine.identifier(name)
        }

        fn table_ref(&self, table: &TablePath) -> SqlString {
            Engine.table_ref(table)
        }

        fn change_log_ref(&self, table: &TablePath) -> SqlString {
            SqlString::from(format!(
                "read_parquet('{}/{}/*.parquet')",
                table.namespace(),
                table.name()
            ))
        }

        fn column_type(&self, column_type: &ColumnType) -> Result<SqlString, UnsupportedColumn> {
            Engine.column_type(column_type)
        }

        fn read(
            &self,
            expression: &SqlString,
            column_type: &ColumnType,
        ) -> Result<SqlString, UnsupportedColumn> {
            Engine.read(expression, column_type)
        }
    }
    fn column(name: &str, column_type: ColumnType) -> ColumnModel {
        let type_name = format!("{column_type:?}");

        ColumnModel::new(name, &type_name, column_type, false, None)
    }

    fn customers_with(columns: Vec<ColumnModel>, key: &[&str]) -> TableModel {
        TableModel::new(
            TableId::new("shop", "customers"),
            columns,
            key.iter().map(|column| (*column).to_owned()).collect(),
        )
    }

    fn customers() -> TableModel {
        customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("name", ColumnType::Text),
            ],
            &["id"],
        )
    }

    fn change(at: i64, table: TableChange) -> SchemaChange<Offset> {
        changes(at, vec![table])
    }

    fn changes(at: i64, tables: Vec<TableChange>) -> SchemaChange<Offset> {
        SchemaChange::new(Offset(at as u64), RecordedAt::from(at), "", tables)
    }

    fn renamed(from: &str, to: &str) -> TableChange {
        TableChange::ColumnRenamed {
            table: TableId::new("shop", "customers"),
            from: from.to_owned(),
            to: to.to_owned(),
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Untold {
        OutOfOrder,
        Unbuildable(UnbuildableArtifact),
    }

    impl From<OutOfOrder> for Untold {
        fn from(_: OutOfOrder) -> Self {
            Self::OutOfOrder
        }
    }

    impl From<UnbuildableArtifact> for Untold {
        fn from(unbuildable: UnbuildableArtifact) -> Self {
            Self::Unbuildable(unbuildable)
        }
    }

    fn snapshots_in<W: Warehouse>(
        warehouse: &W,
        created: TableModel,
        within: Vec<SchemaChange<Offset>>,
        base: impl Fn() -> Base,
    ) -> Result<Vec<Snapshot<'static, Log, W>>, Untold> {
        let mut catalog = CatalogModel::default();
        catalog.accept::<OutOfOrder, _>(&(), &change(1, TableChange::Created(created)), &mut ())?;
        let mut tracker = EraTracker::new(
            Window::new(RecordedAt::from(100), RecordedAt::from(200)),
            |table: &TableId| {
                Ok::<_, Untold>(Snapshot::new(
                    &Log,
                    table.clone(),
                    TablePath::new("mysql_shop_cdc", "mysql_shop_customers"),
                    TablePath::new("mysql_shop", "customers_20260920"),
                    base(),
                ))
            },
        );
        for change in &within {
            catalog.accept::<Untold, _>(warehouse, change, &mut tracker)?;
        }

        tracker.finish(warehouse, &catalog)
    }

    fn snapshots(
        created: TableModel,
        within: Vec<SchemaChange<Offset>>,
        base: impl Fn() -> Base,
    ) -> Result<Vec<Snapshot<'static, Log, Engine>>, Untold> {
        snapshots_in(&Engine, created, within, base)
    }

    fn statement<W: Warehouse>(
        snapshots: Result<Vec<Snapshot<'static, Log, W>>, Untold>,
    ) -> String {
        snapshots.unwrap()[0]
            .statements()
            .unwrap()
            .remove(1)
            .to_string()
    }

    fn widened() -> TableModel {
        customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("name", ColumnType::Text),
                ColumnModel::new("email", "", ColumnType::Text, true, Some("none")),
            ],
            &["id"],
        )
    }

    #[test]
    fn a_snapshot_is_built_into_a_namespace_made_for_it() {
        let statements = snapshots(customers(), vec![], || Base::Captured).unwrap()[0]
            .statements()
            .unwrap();

        assert_eq!(statements[0], "CREATE SCHEMA IF NOT EXISTS \"mysql_shop\"");
    }

    #[test]
    fn a_snapshot_reads_the_change_log_where_the_warehouse_reads_change_logs() {
        let statement = statement(snapshots_in(&Lake, customers(), vec![], || Base::Captured));

        assert!(
            statement
                .contains("FROM read_parquet('mysql_shop_cdc/mysql_shop_customers/*.parquet')")
        );
        assert!(!statement.contains("\"mysql_shop_cdc\".\"mysql_shop_customers\""));
    }

    #[test]
    fn a_first_snapshot_folds_the_window_onto_what_the_capture_read() {
        assert_eq!(
            statement(snapshots(customers(), vec![], || Base::Captured)),
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
                "\"changes_1\" AS (\n",
                "  SELECT *\n",
                "  FROM \"mysql_shop_cdc\".\"mysql_shop_customers\"\n",
                "  WHERE \"ts_ms\" >= 100 AND \"ts_ms\" < 200\n",
                "  QUALIFY ROW_NUMBER() OVER (\n",
                "    PARTITION BY COALESCE(\"after\".\"id\", \"before\".\"id\")\n",
                "    ORDER BY \"source\".\"offset\" DESC\n",
                "  ) = 1\n",
                "),\n",
                "\"era_1\" AS (\n",
                "  SELECT\n",
                "    \"id\",\n",
                "    \"name\"\n",
                "  FROM \"base\"\n",
                "  WHERE NOT EXISTS (\n",
                "    SELECT 1\n",
                "    FROM \"changes_1\"\n",
                "    WHERE COALESCE(\"changes_1\".\"after\".\"id\", \"changes_1\".\"before\".\"id\") = \"base\".\"id\"\n",
                "  )\n",
                "  UNION ALL\n",
                "  SELECT\n",
                "    CAST(\"after\".\"id\" AS BIGINT) AS \"id\",\n",
                "    CAST(\"after\".\"name\" AS TEXT) AS \"name\"\n",
                "  FROM \"changes_1\"\n",
                "  WHERE \"op\" <> 'd'\n",
                ")\n",
                "SELECT * FROM \"era_1\""
            )
        );
    }

    #[test]
    fn a_later_snapshot_folds_the_window_onto_the_snapshot_before_it() {
        let statement = statement(snapshots(customers(), vec![], || {
            Base::Shard(TablePath::new("mysql_shop", "customers_20260919"))
        }));

        assert!(statement.contains(concat!(
            "WITH \"base\" AS (\n",
            "  SELECT\n",
            "    \"id\",\n",
            "    \"name\"\n",
            "  FROM \"mysql_shop\".\"customers_20260919\"\n",
            "),\n",
        )));
    }

    #[test]
    fn a_column_added_within_the_window_fills_in_the_rows_folded_before_it() {
        let statement = statement(snapshots(
            customers(),
            vec![change(150, TableChange::Altered(widened()))],
            || Base::Captured,
        ));

        assert!(statement.contains("  WHERE \"ts_ms\" >= 100 AND \"ts_ms\" < 150\n"));
        assert!(statement.contains(concat!(
            "\"era_2\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    \"name\" AS \"name\",\n",
            "    CAST('none' AS TEXT) AS \"email\"\n",
            "  FROM \"era_1\"\n",
            "),\n",
        )));
        assert!(statement.contains("  WHERE \"ts_ms\" >= 150 AND \"ts_ms\" < 200\n"));
        assert!(statement.contains("    CAST(\"after\".\"email\" AS TEXT) AS \"email\"\n"));
        assert!(statement.ends_with("SELECT * FROM \"era_3\""));
    }

    #[test]
    fn each_column_a_statement_adds_is_carried_in_a_relation_of_its_own() {
        let twice = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("name", ColumnType::Text),
                column("email", ColumnType::Text),
                column("phone", ColumnType::Text),
            ],
            &["id"],
        );

        let statement = statement(snapshots(
            customers(),
            vec![change(150, TableChange::Altered(twice))],
            || Base::Captured,
        ));

        assert!(statement.contains("    CAST(NULL AS TEXT) AS \"email\"\n  FROM \"era_1\"\n"));
        assert!(statement.contains("    CAST(NULL AS TEXT) AS \"phone\"\n  FROM \"era_2\"\n"));
        assert!(statement.ends_with("SELECT * FROM \"era_4\""));
    }

    #[test]
    fn a_column_dropped_and_one_added_by_one_statement_are_carried_one_after_the_other() {
        let replaced = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("email", ColumnType::Text),
            ],
            &["id"],
        );

        let statement = statement(snapshots(
            customers(),
            vec![change(150, TableChange::Altered(replaced))],
            || Base::Captured,
        ));

        assert!(statement.contains(concat!(
            "\"era_2\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\"\n",
            "  FROM \"era_1\"\n",
            "),\n",
            "\"era_3\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    CAST(NULL AS TEXT) AS \"email\"\n",
            "  FROM \"era_2\"\n",
            "),\n",
        )));
        assert!(statement.ends_with("SELECT * FROM \"era_4\""));
    }

    #[test]
    fn a_column_retyped_and_one_added_by_one_statement_are_carried_one_after_the_other() {
        let reshaped = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("name", ColumnType::Int64),
                column("email", ColumnType::Text),
            ],
            &["id"],
        );

        let statement = statement(snapshots(
            customers(),
            vec![change(150, TableChange::Altered(reshaped))],
            || Base::Captured,
        ));

        assert!(statement.contains(concat!(
            "\"era_2\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    CAST(\"name\" AS BIGINT) AS \"name\"\n",
            "  FROM \"era_1\"\n",
            "),\n",
            "\"era_3\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    \"name\" AS \"name\",\n",
            "    CAST(NULL AS TEXT) AS \"email\"\n",
            "  FROM \"era_2\"\n",
            "),\n",
        )));
    }

    #[test]
    fn a_column_added_between_others_lands_where_the_statement_placed_it() {
        let inserted = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("email", ColumnType::Text),
                column("name", ColumnType::Text),
            ],
            &["id"],
        );

        let statement = statement(snapshots(
            customers(),
            vec![change(150, TableChange::Altered(inserted))],
            || Base::Captured,
        ));

        assert!(statement.contains(concat!(
            "\"era_2\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    CAST(NULL AS TEXT) AS \"email\",\n",
            "    \"name\" AS \"name\"\n",
            "  FROM \"era_1\"\n",
        )));
    }

    #[test]
    fn statements_at_different_times_fold_the_era_between_them_in_the_schema_it_had() {
        let widened = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("name", ColumnType::Text),
                column("email", ColumnType::Text),
            ],
            &["id"],
        );
        let narrowed = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("email", ColumnType::Text),
            ],
            &["id"],
        );

        let statement = statement(snapshots(
            customers(),
            vec![
                change(130, TableChange::Altered(widened)),
                change(160, TableChange::Altered(narrowed)),
            ],
            || Base::Captured,
        ));

        assert!(statement.contains(concat!(
            "  WHERE \"ts_ms\" >= 130 AND \"ts_ms\" < 160\n",
            "  QUALIFY ROW_NUMBER() OVER (\n",
            "    PARTITION BY COALESCE(\"after\".\"id\", \"before\".\"id\")\n",
            "    ORDER BY \"source\".\"offset\" DESC\n",
            "  ) = 1\n",
            "),\n",
            "\"era_3\" AS (\n",
            "  SELECT\n",
            "    \"id\",\n",
            "    \"name\",\n",
            "    \"email\"\n",
            "  FROM \"era_2\"\n",
        )));
        assert!(statement.contains(concat!(
            "\"era_4\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    \"email\" AS \"email\"\n",
            "  FROM \"era_3\"\n",
        )));
        assert!(statement.contains("  WHERE \"ts_ms\" >= 160 AND \"ts_ms\" < 200\n"));
        assert!(statement.ends_with("SELECT * FROM \"era_5\""));
    }

    #[test]
    fn a_column_retyped_within_the_window_casts_the_rows_folded_before_it() {
        let retyped = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("name", ColumnType::Int64),
            ],
            &["id"],
        );

        let statement = statement(snapshots(
            customers(),
            vec![change(150, TableChange::Altered(retyped))],
            || Base::Captured,
        ));

        assert!(statement.contains("    CAST(\"name\" AS BIGINT) AS \"name\"\n  FROM \"era_1\"\n"));
    }

    #[test]
    fn a_column_dropped_within_the_window_leaves_the_rows_folded_before_it() {
        let narrowed = customers_with(vec![column("id", ColumnType::Int64)], &["id"]);

        let statement = statement(snapshots(
            customers(),
            vec![change(150, TableChange::Altered(narrowed))],
            || Base::Captured,
        ));

        assert!(statement.contains(concat!(
            "\"era_2\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\"\n",
            "  FROM \"era_1\"\n",
        )));
    }

    #[test]
    fn a_table_whose_primary_key_changed_within_the_window_cannot_be_replayed() {
        let rekeyed = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("name", ColumnType::Text),
            ],
            &["name"],
        );

        assert_eq!(
            snapshots(
                customers(),
                vec![change(150, TableChange::Altered(rekeyed))],
                || Base::Captured
            )
            .map(drop)
            .unwrap_err(),
            Untold::Unbuildable(UnbuildableArtifact::PrimaryKeyChanged(TableId::new(
                "shop",
                "customers"
            )))
        );
    }

    #[test]
    fn a_column_retyped_and_moved_by_one_statement_lands_where_the_statement_placed_it() {
        let customers = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("name", ColumnType::Text),
                column("email", ColumnType::Text),
            ],
            &["id"],
        );
        let moved = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("email", ColumnType::Text),
                column("name", ColumnType::Int64),
            ],
            &["id"],
        );

        let statement = statement(snapshots(
            customers,
            vec![change(150, TableChange::Altered(moved))],
            || Base::Captured,
        ));

        assert!(statement.contains(concat!(
            "\"era_2\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    \"email\" AS \"email\",\n",
            "    CAST(\"name\" AS BIGINT) AS \"name\"\n",
            "  FROM \"era_1\"\n",
        )));
    }

    #[test]
    fn a_key_changed_after_columns_by_one_statement_still_cannot_be_replayed() {
        let rekeyed = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("name", ColumnType::Text),
                column("email", ColumnType::Text),
            ],
            &["id", "email"],
        );

        assert_eq!(
            snapshots(
                customers(),
                vec![change(150, TableChange::Altered(rekeyed))],
                || Base::Captured
            )
            .map(drop)
            .unwrap_err(),
            Untold::Unbuildable(UnbuildableArtifact::PrimaryKeyChanged(TableId::new(
                "shop",
                "customers"
            )))
        );
    }

    #[test]
    fn a_column_renamed_within_the_window_carries_the_rows_folded_before_it_under_its_new_name() {
        let renamed_to = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("full_name", ColumnType::Text),
            ],
            &["id"],
        );

        let statement = statement(snapshots(
            customers(),
            vec![changes(
                150,
                vec![
                    renamed("name", "full_name"),
                    TableChange::Altered(renamed_to),
                ],
            )],
            || Base::Captured,
        ));

        assert!(statement.contains(concat!(
            "\"era_2\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    \"name\" AS \"full_name\"\n",
            "  FROM \"era_1\"\n",
            "),\n",
        )));
        assert!(statement.contains("    CAST(\"after\".\"full_name\" AS TEXT) AS \"full_name\"\n"));
        assert!(statement.ends_with("SELECT * FROM \"era_3\""));
    }

    #[test]
    fn a_column_renamed_and_another_added_by_one_statement_are_carried_one_after_the_other() {
        let reshaped = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("full_name", ColumnType::Text),
                column("email", ColumnType::Text),
            ],
            &["id"],
        );

        let statement = statement(snapshots(
            customers(),
            vec![changes(
                150,
                vec![renamed("name", "full_name"), TableChange::Altered(reshaped)],
            )],
            || Base::Captured,
        ));

        assert!(statement.contains(concat!(
            "    \"name\" AS \"full_name\"\n",
            "  FROM \"era_1\"\n",
            "),\n",
            "\"era_3\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    \"full_name\" AS \"full_name\",\n",
            "    CAST(NULL AS TEXT) AS \"email\"\n",
            "  FROM \"era_2\"\n",
        )));
    }

    #[test]
    fn a_renamed_key_column_keys_each_era_by_the_name_it_had_in_it() {
        let rekeyed = customers_with(
            vec![
                column("customer_id", ColumnType::Int64),
                column("name", ColumnType::Text),
            ],
            &["customer_id"],
        );

        let statement = statement(snapshots(
            customers(),
            vec![changes(
                150,
                vec![renamed("id", "customer_id"), TableChange::Altered(rekeyed)],
            )],
            || Base::Captured,
        ));

        assert!(statement.contains(concat!(
            "  WHERE \"ts_ms\" >= 100 AND \"ts_ms\" < 150\n",
            "  QUALIFY ROW_NUMBER() OVER (\n",
            "    PARTITION BY COALESCE(\"after\".\"id\", \"before\".\"id\")\n",
        )));
        assert!(statement.contains("    \"id\" AS \"customer_id\",\n"));
        assert!(statement.contains(concat!(
            "  WHERE \"ts_ms\" >= 150 AND \"ts_ms\" < 200\n",
            "  QUALIFY ROW_NUMBER() OVER (\n",
            "    PARTITION BY COALESCE(\"after\".\"customer_id\", \"before\".\"customer_id\")\n",
        )));
        assert!(statement.contains(concat!(
            "    WHERE COALESCE(\"changes_3\".\"after\".\"customer_id\", \"changes_3\".\"before\".\"customer_id\")",
            " = \"era_2\".\"customer_id\"\n"
        )));
    }

    #[test]
    fn a_table_renamed_within_the_window_cannot_be_snapshotted_yet() {
        let clients = TableId::new("shop", "clients");
        let moved = customers().with_id(&clients);

        assert_eq!(
            snapshots(
                customers(),
                vec![changes(
                    150,
                    vec![
                        TableChange::Renamed {
                            from: TableId::new("shop", "customers"),
                            to: clients,
                        },
                        TableChange::Altered(moved),
                    ],
                )],
                || Base::Captured
            )
            .map(drop)
            .unwrap_err(),
            Untold::Unbuildable(UnbuildableArtifact::Renamed(TableId::new(
                "shop",
                "customers"
            )))
        );
    }

    #[test]
    fn a_composite_key_partitions_by_every_column_of_it() {
        let items = TableModel::new(
            TableId::new("shop", "order_items"),
            vec![
                column("order_id", ColumnType::Int64),
                column("line_no", ColumnType::Int64),
            ],
            vec!["order_id".to_owned(), "line_no".to_owned()],
        );

        let statement = statement(snapshots(items, vec![], || Base::Captured));

        assert!(statement.contains(concat!(
            "    PARTITION BY COALESCE(\"after\".\"order_id\", \"before\".\"order_id\"), ",
            "COALESCE(\"after\".\"line_no\", \"before\".\"line_no\")\n"
        )));
        assert!(statement.contains(concat!(
            "    WHERE COALESCE(\"changes_1\".\"after\".\"order_id\", \"changes_1\".\"before\".\"order_id\")",
            " = \"base\".\"order_id\"\n",
            "      AND COALESCE(\"changes_1\".\"after\".\"line_no\", \"changes_1\".\"before\".\"line_no\")",
            " = \"base\".\"line_no\"\n"
        )));
    }

    #[test]
    fn a_column_the_warehouse_cannot_hold_is_reported() {
        let tagged = customers_with(vec![column("tags", ColumnType::Json)], &["tags"]);

        assert_eq!(
            snapshots(tagged, vec![], || Base::Captured)
                .map(drop)
                .unwrap_err(),
            Untold::Unbuildable(UnbuildableArtifact::from(UnsupportedColumn::new(
                &ColumnType::Json
            )))
        );
    }

    #[test]
    fn a_table_without_a_primary_key_cannot_be_replayed() {
        let keyless = customers_with(vec![column("id", ColumnType::Int64)], &[]);

        assert_eq!(
            snapshots(keyless, vec![], || Base::Captured)
                .map(drop)
                .unwrap_err(),
            Untold::Unbuildable(UnbuildableArtifact::WithoutPrimaryKey(TableId::new(
                "shop",
                "customers"
            )))
        );
    }

    #[test]
    fn a_snapshot_told_of_no_era_cannot_be_built() {
        let snapshot: Snapshot<'_, Log, Engine> = Snapshot::new(
            &Log,
            TableId::new("shop", "customers"),
            TablePath::new("mysql_shop_cdc", "mysql_shop_customers"),
            TablePath::new("mysql_shop", "customers_20260920"),
            Base::Captured,
        );

        assert_eq!(
            snapshot.statements().unwrap_err(),
            UnbuildableArtifact::WithoutEra(TableId::new("shop", "customers"))
        );
    }
}
