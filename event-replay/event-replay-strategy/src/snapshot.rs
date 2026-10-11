use event_replay_core::artifact::{Artifact, UnbuildableArtifact};
use event_replay_core::era::Era;
use event_replay_core::listener::{
    ColumnAdded, ColumnDropped, ColumnRetyped, PrimaryKeyChanged, SchemaChangeListener,
};
use event_replay_core::model::{ColumnModel, TableId, TableModel};
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
        table: &TableModel,
        keys: &Keys,
    ) -> Result<String, UnbuildableArtifact> {
        match self {
            Self::Shard(shard) => Ok(format!(
                "  SELECT\n{}\n  FROM {}",
                Snapshot::<L>::columns(warehouse, table, "    "),
                warehouse.table_ref(shard),
            )),
            Self::Captured => Ok(format!(
                "  SELECT\n{}\n  FROM {}\n  WHERE {} = 'r'\n{}",
                snapshot.reads(warehouse, table, "    ")?,
                warehouse.change_log_ref(&snapshot.cdc),
                warehouse.path(&snapshot.log.operation()),
                snapshot.latest(warehouse, keys),
            )),
        }
    }
}

enum Step {
    Fold(Era),
    Transition {
        before: TableModel,
        after: TableModel,
    },
}

impl Step {
    fn entering(&self) -> &TableModel {
        match self {
            Self::Fold(era) => era.table(),
            Self::Transition { before, .. } => before,
        }
    }

    fn leaving(&self) -> &TableModel {
        match self {
            Self::Fold(era) => era.table(),
            Self::Transition { after, .. } => after,
        }
    }

    fn guard_key(&self, last: &TableModel) -> Result<(), UnbuildableArtifact> {
        match self.entering().primary_key().eq(last.primary_key()) {
            true => Ok(()),
            false => Err(UnbuildableArtifact::PrimaryKeyChanged(last.id().clone())),
        }
    }
}

struct Keys {
    columns: Vec<String>,
}

pub struct Snapshot<'a, L> {
    log: &'a L,
    table: TableId,
    cdc: TablePath,
    target: TablePath,
    base: Base,
    steps: Vec<Step>,
}

impl<'a, L: RowChangeLog> Snapshot<'a, L> {
    pub fn new(log: &'a L, table: TableId, cdc: TablePath, target: TablePath, base: Base) -> Self {
        Self {
            log,
            table,
            cdc,
            target,
            base,
            steps: Vec::new(),
        }
    }

    pub fn table(&self) -> &TableId {
        &self.table
    }

    fn transit(&mut self, before: &TableModel, after: &TableModel) {
        if let Some(Step::Transition { after: last, .. }) = self.steps.last()
            && last == after
        {
            return;
        }

        self.steps.push(Step::Transition {
            before: before.clone(),
            after: after.clone(),
        });
    }

    fn create_table(&self, warehouse: &impl Warehouse) -> Result<String, UnbuildableArtifact> {
        let (first, last) = match (self.steps.first(), self.steps.last()) {
            (Some(first), Some(last)) => (first.entering(), last.leaving()),
            _ => return Err(UnbuildableArtifact::WithoutEra(self.table.clone())),
        };
        let keys = self.keys(warehouse, last)?;
        let mut relations = vec![(
            warehouse.identifier("base"),
            self.base.select(self, warehouse, first, &keys)?,
        )];

        for (at, step) in self.steps.iter().enumerate() {
            step.guard_key(last)?;

            let previous = Self::latest_relation(&relations).to_owned();
            relations.extend(self.relations(warehouse, at + 1, step, &previous, &keys)?);
        }

        Ok(self.with(warehouse, &relations))
    }

    fn latest_relation(relations: &[(String, String)]) -> &str {
        relations.last().map_or("", |(name, _)| name.as_str())
    }

    fn with(&self, warehouse: &impl Warehouse, relations: &[(String, String)]) -> String {
        let ctes: Vec<String> = relations
            .iter()
            .map(|(name, select)| format!("{name} AS (\n{select}\n)"))
            .collect();

        format!(
            "CREATE OR REPLACE TABLE {} AS\nWITH {}\nSELECT * FROM {}",
            warehouse.table_ref(&self.target),
            ctes.join(",\n"),
            Self::latest_relation(relations),
        )
    }

    fn relations(
        &self,
        warehouse: &impl Warehouse,
        at: usize,
        step: &Step,
        previous: &str,
        keys: &Keys,
    ) -> Result<Vec<(String, String)>, UnbuildableArtifact> {
        let era = warehouse.identifier(&format!("era_{at}"));

        match step {
            Step::Fold(fold) => {
                let changes = warehouse.identifier(&format!("changes_{at}"));

                Ok(vec![
                    (changes.clone(), self.changes(warehouse, fold, keys)),
                    (
                        era,
                        self.fold(warehouse, fold.table(), previous, &changes, keys)?,
                    ),
                ])
            }
            Step::Transition { before, after } => Ok(vec![(
                era,
                Self::transition(warehouse, before, after, previous)?,
            )]),
        }
    }

    fn changes(&self, warehouse: &impl Warehouse, era: &Era, keys: &Keys) -> String {
        let recorded_at = warehouse.path(&self.log.recorded_at());

        format!(
            "  SELECT *\n  FROM {}\n  WHERE {recorded_at} >= {} AND {recorded_at} < {}\n{}",
            warehouse.change_log_ref(&self.cdc),
            era.window().from().milliseconds_since_epoch(),
            era.window().to().milliseconds_since_epoch(),
            self.latest(warehouse, keys),
        )
    }

    fn fold(
        &self,
        warehouse: &impl Warehouse,
        table: &TableModel,
        previous: &str,
        changes: &str,
        keys: &Keys,
    ) -> Result<String, UnbuildableArtifact> {
        Ok(format!(
            concat!(
                "  SELECT\n{}\n  FROM {previous}\n",
                "  WHERE NOT EXISTS (\n    SELECT 1\n    FROM {changes}\n{}\n  )\n",
                "  UNION ALL\n",
                "  SELECT\n{}\n  FROM {changes}\n  WHERE {} <> 'd'"
            ),
            Self::columns(warehouse, table, "    "),
            self.matched(warehouse, previous, changes, keys),
            self.reads(warehouse, table, "    ")?,
            warehouse.path(&self.log.operation()),
            previous = previous,
            changes = changes,
        ))
    }

    fn matched(
        &self,
        warehouse: &impl Warehouse,
        previous: &str,
        changes: &str,
        keys: &Keys,
    ) -> String {
        keys.columns
            .iter()
            .enumerate()
            .map(|(at, column)| {
                let matched = format!(
                    "{} = {previous}.{column}",
                    self.keyed(warehouse, &format!("{changes}."), column)
                );

                match at {
                    0 => format!("    WHERE {matched}"),
                    _ => format!("      AND {matched}"),
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn transition(
        warehouse: &impl Warehouse,
        before: &TableModel,
        after: &TableModel,
        previous: &str,
    ) -> Result<String, UnbuildableArtifact> {
        let columns: Vec<String> = after
            .columns()
            .map(|column| {
                let name = warehouse.identifier(column.name());
                let value = Self::carried(warehouse, column, before, &name)?;

                Ok(format!("    {value} AS {name}"))
            })
            .collect::<Result<_, UnbuildableArtifact>>()?;

        Ok(format!(
            "  SELECT\n{}\n  FROM {previous}",
            columns.join(",\n")
        ))
    }

    fn carried(
        warehouse: &impl Warehouse,
        column: &ColumnModel,
        before: &TableModel,
        name: &str,
    ) -> Result<String, UnbuildableArtifact> {
        match before.column(column.name()) {
            Some(was) if was.column_type() == column.column_type() => Ok(name.to_owned()),
            Some(_) => Ok(format!(
                "CAST({name} AS {})",
                warehouse.column_type(column.column_type())?
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

    fn latest(&self, warehouse: &impl Warehouse, keys: &Keys) -> String {
        let partition: Vec<String> = keys
            .columns
            .iter()
            .map(|column| self.keyed(warehouse, "", column))
            .collect();
        let ordering: Vec<String> = self
            .log
            .ordering()
            .iter()
            .map(|path| format!("{} DESC", warehouse.path(path)))
            .collect();

        format!(
            concat!(
                "  QUALIFY ROW_NUMBER() OVER (\n",
                "    PARTITION BY {}\n",
                "    ORDER BY {}\n",
                "  ) = 1"
            ),
            partition.join(", "),
            ordering.join(", "),
        )
    }

    fn keyed(&self, warehouse: &impl Warehouse, within: &str, column: &str) -> String {
        format!(
            "COALESCE({within}{}.{column}, {within}{}.{column})",
            warehouse.path(&self.log.after()),
            warehouse.path(&self.log.before()),
        )
    }

    fn keys(
        &self,
        warehouse: &impl Warehouse,
        table: &TableModel,
    ) -> Result<Keys, UnbuildableArtifact> {
        let columns: Vec<String> = table
            .primary_key()
            .map(|column| warehouse.identifier(column))
            .collect();

        match columns.is_empty() {
            true => Err(UnbuildableArtifact::WithoutPrimaryKey(table.id().clone())),
            false => Ok(Keys { columns }),
        }
    }

    fn columns(warehouse: &impl Warehouse, table: &TableModel, indent: &str) -> String {
        table
            .columns()
            .map(|column| format!("{indent}{}", warehouse.identifier(column.name())))
            .collect::<Vec<_>>()
            .join(",\n")
    }

    fn reads(
        &self,
        warehouse: &impl Warehouse,
        table: &TableModel,
        indent: &str,
    ) -> Result<String, UnbuildableArtifact> {
        let after = warehouse.path(&self.log.after());
        let columns: Vec<String> = table
            .columns()
            .map(|column| {
                let name = warehouse.identifier(column.name());
                let read = warehouse.read(&format!("{after}.{name}"), column.column_type())?;

                Ok(format!("{indent}{read} AS {name}"))
            })
            .collect::<Result<_, UnbuildableArtifact>>()?;

        Ok(columns.join(",\n"))
    }
}

impl<P, L: RowChangeLog> SchemaChangeListener<P> for Snapshot<'_, L> {
    fn visit_era(&mut self, era: &Era) {
        self.steps.push(Step::Fold(era.clone()));
    }

    fn visit_column_added(&mut self, event: &ColumnAdded<'_>) {
        self.transit(event.before(), event.after());
    }

    fn visit_column_dropped(&mut self, event: &ColumnDropped<'_>) {
        self.transit(event.before(), event.after());
    }

    fn visit_column_retyped(&mut self, event: &ColumnRetyped<'_>) {
        self.transit(event.before(), event.after());
    }

    fn visit_primary_key_changed(&mut self, event: &PrimaryKeyChanged<'_>) {
        self.transit(event.before(), event.after());
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
    use event_replay_core::era::EraTracker;
    use event_replay_core::event::{SchemaChange, TableChange};
    use event_replay_core::listener::CatalogModel;
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
        let type_name = format!("{column_type:?}");

        ColumnModel::new(name, &type_name, column_type, position, false, None)
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
                column("id", ColumnType::Int64, 1),
                column("name", ColumnType::Text, 2),
            ],
            &["id"],
        )
    }

    fn change(at: i64, table: TableChange) -> SchemaChange<Offset> {
        SchemaChange::new(Offset(at as u64), RecordedAt::from(at), "", vec![table])
    }

    fn snapshots(
        created: TableModel,
        within: Vec<SchemaChange<Offset>>,
        base: impl Fn() -> Base,
    ) -> Vec<Snapshot<'static, Log>> {
        let mut catalog = CatalogModel::default();
        catalog
            .accept(&change(1, TableChange::Created(created)), &mut ())
            .unwrap();
        let mut tracker = EraTracker::new(
            Window::new(RecordedAt::from(100), RecordedAt::from(200)),
            |table: &TableId| {
                Ok::<_, ()>(Snapshot::new(
                    &Log,
                    table.clone(),
                    TablePath::new("mysql_shop_cdc", "mysql_shop_customers"),
                    TablePath::new("mysql_shop", "customers_20260920"),
                    base(),
                ))
            },
        );
        for change in &within {
            catalog.accept(change, &mut tracker).unwrap();
        }

        tracker.finish(&catalog).unwrap()
    }

    fn statement(snapshots: Vec<Snapshot<'static, Log>>) -> String {
        snapshots[0].statements(&Engine).unwrap().remove(1)
    }

    fn widened() -> TableModel {
        customers_with(
            vec![
                column("id", ColumnType::Int64, 1),
                column("name", ColumnType::Text, 2),
                ColumnModel::new("email", "", ColumnType::Text, 3, true, Some("none")),
            ],
            &["id"],
        )
    }

    #[test]
    fn a_snapshot_is_built_into_a_namespace_made_for_it() {
        let statements = snapshots(customers(), vec![], || Base::Captured)[0]
            .statements(&Engine)
            .unwrap();

        assert_eq!(statements[0], "CREATE SCHEMA IF NOT EXISTS \"mysql_shop\"");
    }

    #[test]
    fn a_snapshot_reads_the_change_log_where_the_warehouse_reads_change_logs() {
        let statement = snapshots(customers(), vec![], || Base::Captured)[0]
            .statements(&Lake)
            .unwrap()
            .remove(1);

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
    fn a_statement_that_adds_two_columns_transits_once() {
        let twice = customers_with(
            vec![
                column("id", ColumnType::Int64, 1),
                column("name", ColumnType::Text, 2),
                column("email", ColumnType::Text, 3),
                column("phone", ColumnType::Text, 4),
            ],
            &["id"],
        );

        let statement = statement(snapshots(
            customers(),
            vec![change(150, TableChange::Altered(twice))],
            || Base::Captured,
        ));

        assert!(statement.ends_with("SELECT * FROM \"era_3\""));
    }

    #[test]
    fn a_column_retyped_within_the_window_casts_the_rows_folded_before_it() {
        let retyped = customers_with(
            vec![
                column("id", ColumnType::Int64, 1),
                column("name", ColumnType::Int64, 2),
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
        let narrowed = customers_with(vec![column("id", ColumnType::Int64, 1)], &["id"]);

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
                column("id", ColumnType::Int64, 1),
                column("name", ColumnType::Text, 2),
            ],
            &["name"],
        );

        assert_eq!(
            snapshots(
                customers(),
                vec![change(150, TableChange::Altered(rekeyed))],
                || Base::Captured
            )[0]
            .statements(&Engine)
            .unwrap_err(),
            UnbuildableArtifact::PrimaryKeyChanged(TableId::new("shop", "customers"))
        );
    }

    #[test]
    fn a_composite_key_partitions_by_every_column_of_it() {
        let items = TableModel::new(
            TableId::new("shop", "order_items"),
            vec![
                column("order_id", ColumnType::Int64, 1),
                column("line_no", ColumnType::Int64, 2),
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
        let tagged = customers_with(vec![column("tags", ColumnType::Json, 1)], &["tags"]);

        assert_eq!(
            snapshots(tagged, vec![], || Base::Captured)[0]
                .statements(&Engine)
                .unwrap_err(),
            UnbuildableArtifact::from(UnsupportedColumn::new(&ColumnType::Json))
        );
    }

    #[test]
    fn a_table_without_a_primary_key_cannot_be_replayed() {
        let keyless = customers_with(vec![column("id", ColumnType::Int64, 1)], &[]);

        assert_eq!(
            snapshots(keyless, vec![], || Base::Captured)[0]
                .statements(&Engine)
                .unwrap_err(),
            UnbuildableArtifact::WithoutPrimaryKey(TableId::new("shop", "customers"))
        );
    }

    #[test]
    fn a_snapshot_told_of_no_era_cannot_be_built() {
        let snapshot: Snapshot<'_, Log> = Snapshot::new(
            &Log,
            TableId::new("shop", "customers"),
            TablePath::new("mysql_shop_cdc", "mysql_shop_customers"),
            TablePath::new("mysql_shop", "customers_20260920"),
            Base::Captured,
        );

        assert_eq!(
            snapshot.statements(&Engine).unwrap_err(),
            UnbuildableArtifact::WithoutEra(TableId::new("shop", "customers"))
        );
    }
}
