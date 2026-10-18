use std::marker::PhantomData;

use chrono::{DateTime, Utc};

use lithify_core::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use lithify_core::build_log::{BuildLog, Outcome, Run};
use lithify_core::era::Era;
use lithify_core::listener::{
    Alteration, Appearance, SchemaChangeListener, TableAltered, TableCreated, TableDropped,
    TableRenamed,
};
use lithify_core::model::{ColumnModel, TableId, TableModel};
use lithify_core::sink::{TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
use lithify_core::sql::SqlString;
use lithify_core::window::Window;

use fold::Fold;
use query::ChangeLogQuery;

mod fold;
mod query;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    namespace: String,
    shard: String,
}

impl Snapshot {
    pub fn new(namespace: &str, shard: &str) -> Self {
        Self {
            namespace: namespace.to_owned(),
            shard: shard.to_owned(),
        }
    }
}

impl Artifact for Snapshot {
    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = SnapshotBuilder<'a, L, W>;

    fn target(&self, table: &TableId, window: &Window) -> Result<TablePath, UnbuildableArtifact> {
        let at = DateTime::<Utc>::from(window.last());

        Ok(TablePath::new(
            &self.namespace,
            &format!("{}_{}", table.table(), at.format(&self.shard)),
        ))
    }

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        log: &'a L,
        table: TableId,
        window: &'a Window,
        previous: Option<&BuildLog>,
    ) -> Result<Self::Builder<'a, L, W>, UnbuildableArtifact> {
        SnapshotBuilder::new(
            self,
            log,
            table,
            window.clone(),
            previous.and_then(BuildLog::target).cloned(),
        )
    }
}

pub struct SnapshotBuilder<'a, L, W> {
    snapshot: &'a Snapshot,
    query: ChangeLogQuery<'a, L>,
    table_from: Option<TableId>,
    table: TableId,
    appearance: Option<Appearance>,
    window: Window,
    base: Option<TablePath>,
    target: Option<TablePath>,
    begun: bool,
    fold: Fold,
    warehouse: PhantomData<fn(&W)>,
}

impl<'a, L: RowChangeLog, W: Warehouse> SnapshotBuilder<'a, L, W> {
    fn new(
        snapshot: &'a Snapshot,
        log: &'a L,
        table: TableId,
        window: Window,
        base: Option<TablePath>,
    ) -> Result<Self, UnbuildableArtifact> {
        Ok(Self {
            snapshot,
            query: ChangeLogQuery::new(log),
            target: Some(snapshot.target(&table, &window)?),
            table_from: Some(table.clone()),
            table,
            appearance: None,
            window,
            base,
            begun: false,
            fold: Fold::default(),
            warehouse: PhantomData,
        })
    }

    pub fn missing_base(&self) -> Option<&TableId> {
        let logged_from_start = self
            .appearance
            .is_some_and(|appearance| appearance.is_logged_from_start());

        match logged_from_start || self.base.is_some() {
            true => None,
            false => self.table_from.as_ref(),
        }
    }

    fn begin(&mut self, warehouse: &W, table: &TableModel) -> Result<(), UnbuildableArtifact> {
        if self.begun {
            return Ok(());
        }
        if table.primary_key().next().is_none() {
            return Err(UnbuildableArtifact::WithoutPrimaryKey(table.id().clone()));
        }

        let base = self.select_base(warehouse, table)?;

        self.fold.push(warehouse.identifier("base"), base);
        self.begun = true;

        Ok(())
    }

    fn select_base(
        &self,
        warehouse: &W,
        table: &TableModel,
    ) -> Result<SqlString, UnbuildableArtifact> {
        match &self.base {
            Some(base) => Ok(self.query.shard(warehouse, base, table)),
            None => self.query.captured(
                warehouse,
                &self.query.change_log(warehouse, table.id()),
                table,
            ),
        }
    }

    fn previous(&self) -> Result<SqlString, UnbuildableArtifact> {
        self.fold
            .previous()
            .cloned()
            .ok_or_else(|| self.without_era())
    }

    fn without_era(&self) -> UnbuildableArtifact {
        UnbuildableArtifact::WithoutEra(self.table.clone())
    }

    fn fold_era(
        &mut self,
        warehouse: &W,
        era: &Era<L::Position>,
    ) -> Result<(), UnbuildableArtifact> {
        self.begin(warehouse, era.table())?;

        let step = self.fold.step();
        let previous = self.previous()?;
        let changes = warehouse.identifier(&format!("changes_{step}"));
        let fold = self
            .query
            .fold_changes(warehouse, era.table(), &previous, &changes)?;
        let read = self.query.changes(
            warehouse,
            &self.query.change_log(warehouse, era.table().id()),
            era,
        );

        self.fold.push(changes, read);
        self.fold
            .push(warehouse.identifier(&format!("era_{step}")), fold);

        Ok(())
    }

    fn carry(
        &mut self,
        warehouse: &W,
        next: &TableModel,
        carried: impl Fn(&ColumnModel, &SqlString) -> Result<SqlString, UnbuildableArtifact>,
    ) -> Result<(), UnbuildableArtifact> {
        let step = self.fold.step();
        let carry = self
            .query
            .carry(warehouse, next, &self.previous()?, carried)?;

        self.fold
            .push(warehouse.identifier(&format!("era_{step}")), carry);

        Ok(())
    }

    fn source(
        warehouse: &W,
        alterations: &[Alteration<'_>],
        column: &ColumnModel,
        name: &SqlString,
    ) -> SqlString {
        alterations
            .iter()
            .filter_map(Alteration::as_column_renamed)
            .find(|(_, renamed)| renamed.name() == column.name())
            .map_or_else(
                || name.clone(),
                |(previous, _)| warehouse.identifier(previous.name()),
            )
    }

    fn carried(
        warehouse: &W,
        alterations: &[Alteration<'_>],
        column: &ColumnModel,
        name: &SqlString,
    ) -> Result<SqlString, UnbuildableArtifact> {
        let source = Self::source(warehouse, alterations, column, name);
        let added = alterations
            .iter()
            .filter_map(Alteration::as_column_added)
            .any(|added| added.name() == column.name());
        let retyped = alterations
            .iter()
            .filter_map(Alteration::as_column_retyped)
            .any(|(_, retyped)| retyped.name() == column.name());

        match (added, retyped) {
            (true, _) => Ok(warehouse.fill(column)?),
            (false, true) => Ok(SqlString::from(format!(
                "CAST({source} AS {})",
                warehouse.column_type(column.column_type())?
            ))),
            (false, false) => Ok(source),
        }
    }
}

impl<L: RowChangeLog, W: Warehouse> SchemaChangeListener<L::Position>
    for SnapshotBuilder<'_, L, W>
{
    type Context = W;
    type Error = UnbuildableArtifact;

    fn table_created(
        &mut self,
        _: &W,
        event: &TableCreated<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        self.appearance = Some(event.appearance());
        if event.appearance().is_creation() {
            self.table_from = None;
        }
        if event.appearance().is_logged_from_start() {
            self.base = None;
        }

        Ok(())
    }

    fn table_dropped(&mut self, _: &W, _: &TableDropped<'_>) -> Result<(), UnbuildableArtifact> {
        self.target = None;

        Ok(())
    }

    fn era_boundary(
        &mut self,
        warehouse: &W,
        closed_era: &Era<L::Position>,
    ) -> Result<(), UnbuildableArtifact> {
        self.fold_era(warehouse, closed_era)
    }

    fn table_renamed(
        &mut self,
        warehouse: &W,
        event: &TableRenamed<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        self.begin(warehouse, event.before())?;
        self.table = event.after().id().clone();
        self.target = Some(self.snapshot.target(&self.table, &self.window)?);

        Ok(())
    }

    fn table_altered(
        &mut self,
        warehouse: &W,
        event: &TableAltered<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        let rekeyed = event
            .alterations()
            .iter()
            .any(Alteration::is_primary_key_changed);
        if rekeyed {
            return Err(UnbuildableArtifact::PrimaryKeyChanged(
                event.after().id().clone(),
            ));
        }
        if event.alterations().iter().all(Alteration::is_metadata_only) {
            return Ok(());
        }
        self.begin(warehouse, event.before())?;

        self.carry(warehouse, event.after(), |column, name| {
            Self::carried(warehouse, event.alterations(), column, name)
        })
    }
}

impl<L: RowChangeLog, W: Warehouse> ArtifactBuilder for SnapshotBuilder<'_, L, W> {
    fn table(&self) -> &TableId {
        &self.table
    }

    fn select(&self) -> Result<SqlString, UnbuildableArtifact> {
        match self.begun {
            true => Ok(self.fold.select()),
            false => Err(self.without_era()),
        }
    }

    fn create<V: Warehouse>(&self, warehouse: &V) -> Result<Vec<SqlString>, UnbuildableArtifact> {
        let Some(target) = &self.target else {
            return Ok(Vec::new());
        };

        Ok(vec![
            warehouse.create_namespace(target.namespace()),
            warehouse.create_table_as(target, &self.select()?),
        ])
    }

    fn build_log(&self, run: &Run, outcome: Outcome) -> BuildLog {
        BuildLog::new(
            run,
            self.table_from.clone(),
            self.target.as_ref().map(|_| self.table.clone()),
            self.base.clone(),
            self.target.clone(),
            self.window.clone(),
            outcome,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lithify_core::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
    use lithify_core::build_log::{Lifecycle, Status};
    use lithify_core::era::{Bound, EraTracker};
    use lithify_core::event::{Hint, SchemaChange, TableChange};
    use lithify_core::listener::{CatalogModel, OutOfOrder};
    use lithify_core::model::{ColumnModel, ColumnType, TableId, TableModel};
    use lithify_core::position::{Coordinate, Position};
    use lithify_core::sink::{TablePath, UnsupportedColumn};
    use lithify_core::source::{ChangeLog, ColumnPath, RowChangeLog};
    use lithify_core::testing::Engine;
    use lithify_core::ts::Ts;
    use lithify_core::window::Window;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Offset(u64);

    impl Position for Offset {}

    struct Log;

    impl ChangeLog for Log {
        type Position = Offset;

        fn envelope(&self) -> Option<ColumnPath> {
            None
        }

        fn event_time(&self) -> ColumnPath {
            ColumnPath::from("source.ts_ms")
        }

        fn processing_time(&self) -> ColumnPath {
            ColumnPath::from("ts_ms")
        }

        fn ordering(&self) -> Vec<ColumnPath> {
            vec![ColumnPath::from("source.offset")]
        }

        fn coordinates(&self, position: &Offset) -> Vec<Coordinate> {
            vec![Coordinate::Number(position.0 as i64)]
        }
    }

    impl RowChangeLog for Log {
        fn dml_topic(&self, table: &TableId) -> String {
            format!("mysql_{}_{}", table.schema(), table.table())
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
        hinted(at, Vec::new(), tables)
    }

    fn hinted(at: i64, hints: Vec<Hint>, tables: Vec<TableChange>) -> SchemaChange<Offset> {
        SchemaChange::new(
            Offset(at as u64),
            Ts::try_from(at).unwrap(),
            "",
            tables,
            hints,
        )
    }

    fn renamed(from: &str, to: &str) -> Hint {
        Hint::ColumnRename {
            table: TableId::new("shop", "customers"),
            from: from.to_owned(),
            to: to.to_owned(),
        }
    }

    fn moved_to(table: &TableId) -> Hint {
        Hint::TableRename {
            from: TableId::new("shop", "customers"),
            to: table.clone(),
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

    fn shop() -> &'static Snapshot {
        Box::leak(Box::new(Snapshot::new("mysql_shop", "20260920")))
    }

    fn window() -> Window {
        Window::new(Ts::try_from(100).unwrap(), Ts::try_from(200).unwrap())
    }

    fn snapshots_in<W: Warehouse>(
        warehouse: &W,
        created: TableModel,
        within: Vec<SchemaChange<Offset>>,
        base: impl Fn() -> Option<TablePath>,
    ) -> Result<Vec<SnapshotBuilder<'static, Log, W>>, Untold> {
        let mut catalog = CatalogModel::default();
        catalog.accept::<OutOfOrder, _>(&(), &change(1, TableChange::Created(created)), &mut ())?;
        let mut tracker = EraTracker::new(
            Window::new(Ts::try_from(100).unwrap(), Ts::try_from(200).unwrap()),
            |table: &TableId| {
                Ok::<_, Untold>(SnapshotBuilder::new(
                    shop(),
                    &Log,
                    table.clone(),
                    window(),
                    base(),
                )?)
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
        base: impl Fn() -> Option<TablePath>,
    ) -> Result<Vec<SnapshotBuilder<'static, Log, Engine>>, Untold> {
        snapshots_in(&Engine::default(), created, within, base)
    }

    fn statement<W: Warehouse>(
        snapshots: Result<Vec<SnapshotBuilder<'static, Log, W>>, Untold>,
    ) -> String {
        snapshots.unwrap()[0]
            .create(&Engine::default())
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
        let statements = snapshots(customers(), vec![], || None).unwrap()[0]
            .create(&Engine::default())
            .unwrap();

        assert_eq!(statements[0], "CREATE SCHEMA IF NOT EXISTS \"mysql_shop\"");
    }

    #[test]
    fn a_snapshot_reads_the_change_log_where_the_warehouse_reads_change_logs() {
        let statement = statement(snapshots_in(
            &Engine::default().reading_change_log("mysql_shop_customers", "changes_of_customers"),
            customers(),
            vec![],
            || None,
        ));

        assert!(statement.contains("FROM changes_of_customers"));
        assert!(!statement.contains("\"mysql_shop_cdc\".\"mysql_shop_customers\""));
    }

    #[test]
    fn a_first_snapshot_folds_the_window_onto_what_the_capture_read() {
        assert_eq!(
            statement(snapshots(customers(), vec![], || None)),
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
                "    ORDER BY \"ts_ms\" DESC, \"source\".\"offset\" DESC\n",
                "  ) = 1\n",
                "),\n",
                "\"changes_1\" AS (\n",
                "  SELECT *\n",
                "  FROM \"mysql_shop_cdc\".\"mysql_shop_customers\"\n",
                "  WHERE \"source\".\"ts_ms\" >= 100 AND \"source\".\"ts_ms\" < 200\n",
                "  QUALIFY ROW_NUMBER() OVER (\n",
                "    PARTITION BY COALESCE(\"after\".\"id\", \"before\".\"id\")\n",
                "    ORDER BY \"ts_ms\" DESC, \"source\".\"offset\" DESC\n",
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
            Some(TablePath::new("mysql_shop", "customers_20260919"))
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
    fn a_table_first_seen_altered_within_the_window_still_folds_onto_its_shard() {
        let orders = TableModel::new(
            TableId::new("shop", "orders"),
            vec![column("id", ColumnType::Int64)],
            vec!["id".to_owned()],
        );

        let snapshots = snapshots(
            customers(),
            vec![change(150, TableChange::Altered(orders))],
            || Some(TablePath::new("mysql_shop", "orders_20260919")),
        )
        .unwrap();
        let orders = snapshots
            .iter()
            .find(|snapshot| snapshot.table() == &TableId::new("shop", "orders"))
            .unwrap()
            .create(&Engine::default())
            .unwrap()
            .remove(1)
            .to_string();

        assert!(orders.contains("  FROM \"mysql_shop\".\"orders_20260919\"\n"));
    }

    #[test]
    fn a_table_renamed_within_the_window_folds_onto_the_shard_of_its_first_name() {
        let statement = statement(Ok(renamed_to_clients(|| {
            Some(TablePath::new("mysql_shop", "customers_20260919"))
        })));

        assert!(statement.contains("  FROM \"mysql_shop\".\"customers_20260919\"\n"));
        assert!(statement.contains("  FROM \"mysql_shop_cdc\".\"mysql_shop_clients\"\n"));
    }

    #[test]
    fn a_table_created_within_the_window_folds_onto_what_the_capture_read_rather_than_a_shard() {
        let orders = TableModel::new(
            TableId::new("shop", "orders"),
            vec![column("id", ColumnType::Int64)],
            vec!["id".to_owned()],
        );

        let snapshots = snapshots(
            customers(),
            vec![change(150, TableChange::Created(orders))],
            || Some(TablePath::new("mysql_shop", "customers_20260919")),
        )
        .unwrap();
        let statement = |table: &str| {
            snapshots
                .iter()
                .find(|snapshot| snapshot.table() == &TableId::new("shop", table))
                .unwrap()
                .create(&Engine::default())
                .unwrap()
                .remove(1)
                .to_string()
        };

        assert!(statement("customers").contains("  FROM \"mysql_shop\".\"customers_20260919\"\n"));
        assert!(statement("orders").contains("  WHERE \"op\" = 'r'\n"));
        assert!(!statement("orders").contains("customers_20260919"));
    }

    #[test]
    fn a_bound_in_time_is_placed_on_the_instant_a_change_occurred_at_the_source() {
        assert_eq!(
            Bound::<Offset>::Time(Ts::try_from(150).unwrap()).placed(&Log),
            [(ColumnPath::from("source.ts_ms"), Coordinate::Number(150))]
        );
    }

    #[test]
    fn a_bound_at_a_position_is_placed_on_the_instant_the_log_recorded_then_the_columns_it_orders_by()
     {
        assert_eq!(
            Bound::Position {
                at: Ts::try_from(150).unwrap(),
                position: Offset(7),
            }
            .placed(&Log),
            [
                (ColumnPath::from("ts_ms"), Coordinate::Number(150)),
                (ColumnPath::from("source.offset"), Coordinate::Number(7))
            ]
        );
    }

    #[test]
    fn a_schema_change_splits_the_change_log_at_its_position_within_its_instant() {
        let statement = statement(snapshots(
            customers(),
            vec![SchemaChange::new(
                Offset(7),
                Ts::try_from(150).unwrap(),
                "",
                vec![TableChange::Altered(widened())],
                Vec::new(),
            )],
            || None,
        ));

        assert!(statement.contains(concat!(
            "  WHERE \"source\".\"ts_ms\" >= 100",
            " AND (\"ts_ms\" < 150 OR (\"ts_ms\" = 150 AND \"source\".\"offset\" < 7))\n"
        )));
        assert!(statement.contains(concat!(
            "  WHERE (\"ts_ms\" > 150 OR (\"ts_ms\" = 150 AND \"source\".\"offset\" >= 7))",
            " AND \"source\".\"ts_ms\" < 200\n"
        )));
    }

    #[test]
    fn the_latest_change_of_a_key_is_the_one_recorded_last_then_logged_last() {
        let statement = statement(snapshots(customers(), vec![], || None));

        assert!(statement.contains("    ORDER BY \"ts_ms\" DESC, \"source\".\"offset\" DESC\n"));
    }

    #[test]
    fn a_column_added_within_the_window_fills_in_the_rows_folded_before_it() {
        let statement = statement(snapshots(
            customers(),
            vec![change(150, TableChange::Altered(widened()))],
            || None,
        ));

        assert!(statement.contains("  WHERE \"source\".\"ts_ms\" >= 100 AND (\"ts_ms\" < 150 OR (\"ts_ms\" = 150 AND \"source\".\"offset\" < 150))\n"));
        assert!(statement.contains(concat!(
            "\"era_2\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    \"name\" AS \"name\",\n",
            "    TRY_CAST('none' AS TEXT) AS \"email\"\n",
            "  FROM \"era_1\"\n",
            "),\n",
        )));
        assert!(statement.contains("  WHERE (\"ts_ms\" > 150 OR (\"ts_ms\" = 150 AND \"source\".\"offset\" >= 150)) AND \"source\".\"ts_ms\" < 200\n"));
        assert!(statement.contains("    CAST(\"after\".\"email\" AS TEXT) AS \"email\"\n"));
        assert!(statement.ends_with("SELECT * FROM \"era_3\""));
    }

    #[test]
    fn the_columns_a_statement_adds_are_carried_in_one_relation() {
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
            || None,
        ));

        assert!(statement.contains(concat!(
            "    CAST(NULL AS TEXT) AS \"email\",\n",
            "    CAST(NULL AS TEXT) AS \"phone\"\n",
            "  FROM \"era_1\"\n"
        )));
        assert!(statement.ends_with("SELECT * FROM \"era_3\""));
    }

    #[test]
    fn a_column_dropped_and_one_added_by_one_statement_are_carried_in_one_relation() {
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
            || None,
        ));

        assert!(statement.contains(concat!(
            "\"era_2\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    CAST(NULL AS TEXT) AS \"email\"\n",
            "  FROM \"era_1\"\n",
            "),\n",
        )));
        assert!(statement.ends_with("SELECT * FROM \"era_3\""));
    }

    #[test]
    fn a_column_retyped_and_one_added_by_one_statement_are_carried_in_one_relation() {
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
            || None,
        ));

        assert!(statement.contains(concat!(
            "\"era_2\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    CAST(\"name\" AS BIGINT) AS \"name\",\n",
            "    CAST(NULL AS TEXT) AS \"email\"\n",
            "  FROM \"era_1\"\n",
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
            || None,
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
            || None,
        ));

        assert!(statement.contains(concat!(
            "  WHERE (\"ts_ms\" > 130 OR (\"ts_ms\" = 130 AND \"source\".\"offset\" >= 130)) AND (\"ts_ms\" < 160 OR (\"ts_ms\" = 160 AND \"source\".\"offset\" < 160))\n",
            "  QUALIFY ROW_NUMBER() OVER (\n",
            "    PARTITION BY COALESCE(\"after\".\"id\", \"before\".\"id\")\n",
            "    ORDER BY \"ts_ms\" DESC, \"source\".\"offset\" DESC\n",
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
        assert!(statement.contains("  WHERE (\"ts_ms\" > 160 OR (\"ts_ms\" = 160 AND \"source\".\"offset\" >= 160)) AND \"source\".\"ts_ms\" < 200\n"));
        assert!(statement.ends_with("SELECT * FROM \"era_5\""));
    }

    #[test]
    fn a_column_described_within_the_window_snapshots_as_if_nothing_changed() {
        let described = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("name", ColumnType::Text).with_comment(Some("how to call them")),
            ],
            &["id"],
        );

        let statement = statement(snapshots(
            customers(),
            vec![change(150, TableChange::Altered(described))],
            || None,
        ));

        assert_eq!(
            statement,
            self::statement(snapshots(customers(), vec![], || None))
        );
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
            || None,
        ));

        assert!(statement.contains("    CAST(\"name\" AS BIGINT) AS \"name\"\n  FROM \"era_1\"\n"));
    }

    #[test]
    fn a_column_dropped_within_the_window_leaves_the_rows_folded_before_it() {
        let narrowed = customers_with(vec![column("id", ColumnType::Int64)], &["id"]);

        let statement = statement(snapshots(
            customers(),
            vec![change(150, TableChange::Altered(narrowed))],
            || None,
        ));

        assert!(statement.contains(concat!(
            "\"era_2\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\"\n",
            "  FROM \"era_1\"\n",
        )));
    }

    #[test]
    fn a_table_whose_primary_key_changed_within_the_window_cannot_be_snapshotted() {
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
                || None
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
            || None,
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
    fn a_key_changed_after_columns_by_one_statement_still_cannot_be_snapshotted() {
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
                || None
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
            vec![hinted(
                150,
                vec![renamed("name", "full_name")],
                vec![TableChange::Altered(renamed_to)],
            )],
            || None,
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
    fn a_column_renamed_and_another_added_by_one_statement_are_carried_in_one_relation() {
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
            vec![hinted(
                150,
                vec![renamed("name", "full_name")],
                vec![TableChange::Altered(reshaped)],
            )],
            || None,
        ));

        assert!(statement.contains(concat!(
            "\"era_2\" AS (\n",
            "  SELECT\n",
            "    \"id\" AS \"id\",\n",
            "    \"name\" AS \"full_name\",\n",
            "    CAST(NULL AS TEXT) AS \"email\"\n",
            "  FROM \"era_1\"\n",
            "),\n",
        )));
    }

    #[test]
    fn a_column_renamed_and_retyped_by_one_statement_casts_the_rows_under_its_old_name() {
        let reshaped = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("full_name", ColumnType::Int64),
            ],
            &["id"],
        );

        let statement = statement(snapshots(
            customers(),
            vec![hinted(
                150,
                vec![renamed("name", "full_name")],
                vec![TableChange::Altered(reshaped)],
            )],
            || None,
        ));

        assert!(statement.contains(concat!(
            "    \"id\" AS \"id\",\n",
            "    CAST(\"name\" AS BIGINT) AS \"full_name\"\n",
            "  FROM \"era_1\"\n",
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
            vec![hinted(
                150,
                vec![renamed("id", "customer_id")],
                vec![TableChange::Altered(rekeyed)],
            )],
            || None,
        ));

        assert!(statement.contains(concat!(
            "  WHERE \"source\".\"ts_ms\" >= 100 AND (\"ts_ms\" < 150 OR (\"ts_ms\" = 150 AND \"source\".\"offset\" < 150))\n",
            "  QUALIFY ROW_NUMBER() OVER (\n",
            "    PARTITION BY COALESCE(\"after\".\"id\", \"before\".\"id\")\n",
        )));
        assert!(statement.contains("    \"id\" AS \"customer_id\",\n"));
        assert!(statement.contains(concat!(
            "  WHERE (\"ts_ms\" > 150 OR (\"ts_ms\" = 150 AND \"source\".\"offset\" >= 150)) AND \"source\".\"ts_ms\" < 200\n",
            "  QUALIFY ROW_NUMBER() OVER (\n",
            "    PARTITION BY COALESCE(\"after\".\"customer_id\", \"before\".\"customer_id\")\n",
        )));
        assert!(statement.contains(concat!(
            "    WHERE COALESCE(\"changes_3\".\"after\".\"customer_id\", \"changes_3\".\"before\".\"customer_id\")",
            " = \"era_2\".\"customer_id\"\n"
        )));
    }

    fn renamed_to_clients(
        base: impl Fn() -> Option<TablePath>,
    ) -> Vec<SnapshotBuilder<'static, Log, Engine>> {
        let clients = TableId::new("shop", "clients");

        snapshots(
            customers(),
            vec![hinted(
                150,
                vec![moved_to(&clients)],
                vec![TableChange::Altered(customers().with_id(&clients))],
            )],
            base,
        )
        .unwrap()
    }

    #[test]
    fn a_table_renamed_within_the_window_reads_each_era_from_the_change_log_of_its_name() {
        let statement = statement(Ok(renamed_to_clients(|| None)));

        assert!(statement.contains(concat!(
            "\"changes_1\" AS (\n",
            "  SELECT *\n",
            "  FROM \"mysql_shop_cdc\".\"mysql_shop_customers\"\n",
            "  WHERE \"source\".\"ts_ms\" >= 100 AND (\"ts_ms\" < 150 OR (\"ts_ms\" = 150 AND \"source\".\"offset\" < 150))\n",
        )));
        assert!(statement.contains(concat!(
            "\"changes_2\" AS (\n",
            "  SELECT *\n",
            "  FROM \"mysql_shop_cdc\".\"mysql_shop_clients\"\n",
            "  WHERE (\"ts_ms\" > 150 OR (\"ts_ms\" = 150 AND \"source\".\"offset\" >= 150)) AND \"source\".\"ts_ms\" < 200\n",
        )));
        assert!(statement.ends_with("SELECT * FROM \"era_2\""));
    }

    #[test]
    fn a_table_renamed_within_the_window_starts_from_what_the_capture_read_under_its_first_name() {
        let statement = statement(Ok(renamed_to_clients(|| None)));

        assert!(statement.contains(concat!(
            "  FROM \"mysql_shop_cdc\".\"mysql_shop_customers\"\n",
            "  WHERE \"op\" = 'r'\n",
        )));
    }

    #[test]
    fn a_table_renamed_within_the_window_is_snapshotted_under_its_last_name() {
        let snapshots = renamed_to_clients(|| None);
        let statements = snapshots[0].create(&Engine::default()).unwrap();

        assert_eq!(snapshots[0].table(), &TableId::new("shop", "clients"));
        assert!(
            statements[1]
                .as_str()
                .starts_with("CREATE OR REPLACE TABLE \"mysql_shop\".\"clients_20260920\" AS\n")
        );
    }

    #[test]
    fn a_table_renamed_and_altered_by_one_statement_carries_its_rows_under_its_new_name() {
        let clients = TableId::new("shop", "clients");
        let widened = customers_with(
            vec![
                column("id", ColumnType::Int64),
                column("name", ColumnType::Text),
                column("email", ColumnType::Text),
            ],
            &["id"],
        )
        .with_id(&clients);

        let statement = statement(snapshots(
            customers(),
            vec![hinted(
                150,
                vec![moved_to(&clients)],
                vec![TableChange::Altered(widened)],
            )],
            || None,
        ));

        assert!(statement.contains("    CAST(NULL AS TEXT) AS \"email\"\n  FROM \"era_1\"\n"));
        assert!(statement.contains("  FROM \"mysql_shop_cdc\".\"mysql_shop_clients\"\n"));
        assert!(statement.ends_with("SELECT * FROM \"era_3\""));
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

        let statement = statement(snapshots(items, vec![], || None));

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
            snapshots(tagged, vec![], || None).map(drop).unwrap_err(),
            Untold::Unbuildable(UnbuildableArtifact::from(UnsupportedColumn::new(
                &ColumnType::Json
            )))
        );
    }

    #[test]
    fn a_table_without_a_primary_key_cannot_be_snapshotted() {
        let keyless = customers_with(vec![column("id", ColumnType::Int64)], &[]);

        assert_eq!(
            snapshots(keyless, vec![], || None).map(drop).unwrap_err(),
            Untold::Unbuildable(UnbuildableArtifact::WithoutPrimaryKey(TableId::new(
                "shop",
                "customers"
            )))
        );
    }

    #[test]
    fn a_snapshot_told_of_no_era_cannot_be_built() {
        let snapshot: SnapshotBuilder<'_, Log, Engine> = SnapshotBuilder::new(
            shop(),
            &Log,
            TableId::new("shop", "customers"),
            window(),
            None,
        )
        .unwrap();

        assert_eq!(
            snapshot.create(&Engine::default()).unwrap_err(),
            UnbuildableArtifact::WithoutEra(TableId::new("shop", "customers"))
        );
    }

    #[test]
    fn a_snapshot_is_named_after_its_table_and_the_last_day_its_window_holds() {
        let snapshot = Snapshot::new("mysql_shop", "%Y%m%d");

        for opened in [0, 1789862400000] {
            let window = Window::new(
                Ts::try_from(opened).unwrap(),
                Ts::try_from(1789948800000).unwrap(),
            );

            assert_eq!(
                snapshot
                    .target(&TableId::new("shop", "customers"), &window)
                    .unwrap(),
                TablePath::new("mysql_shop", "customers_20260920")
            );
        }
    }

    #[test]
    fn a_snapshot_hands_a_builder_that_builds_it_from_the_eras_it_is_told_of() {
        let window = window();
        let mut fold = shop()
            .builder::<Log, Engine>(&Log, TableId::new("shop", "customers"), &window, None)
            .unwrap();

        fold.era_boundary(
            &Engine::default(),
            &Era::new(
                customers(),
                Bound::Time(Ts::try_from(100).unwrap()),
                Bound::Time(Ts::try_from(200).unwrap()),
            ),
        )
        .unwrap();

        assert!(
            fold.create(&Engine::default()).unwrap()[1]
                .as_str()
                .starts_with("CREATE OR REPLACE TABLE \"mysql_shop\".\"customers_20260920\" AS\n")
        );
    }

    #[test]
    fn a_snapshot_is_logged_folding_onto_the_shard_its_table_was_last_built_into() {
        let window = window();
        let base = TablePath::new("mysql_shop", "customers_20260919");
        let run = Run::new(
            ulid::Ulid::nil(),
            DateTime::UNIX_EPOCH,
            "snapshot",
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let finished_at = DateTime::from_timestamp_millis(1789949000000).unwrap();
        let previous = BuildLog::new(
            &run,
            Some(TableId::new("shop", "customers")),
            Some(TableId::new("shop", "customers")),
            None,
            Some(base.clone()),
            Window::new(Ts::try_from(0).unwrap(), Ts::try_from(100).unwrap()),
            Outcome::new(Status::Succeeded, finished_at),
        );
        let builder = shop()
            .builder::<Log, Engine>(
                &Log,
                TableId::new("shop", "customers"),
                &window,
                Some(&previous),
            )
            .unwrap();

        assert_eq!(
            builder.build_log(&run, Outcome::new(Status::Succeeded, finished_at)),
            BuildLog::new(
                &run,
                Some(TableId::new("shop", "customers")),
                Some(TableId::new("shop", "customers")),
                Some(base),
                Some(TablePath::new("mysql_shop", "customers_20260920")),
                window,
                Outcome::new(Status::Succeeded, finished_at),
            )
        );
    }

    #[test]
    fn a_table_renamed_within_the_window_is_logged_from_its_first_name_to_its_last() {
        let run = Run::new(
            ulid::Ulid::nil(),
            DateTime::UNIX_EPOCH,
            "snapshot",
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let outcome = Outcome::new(
            Status::Succeeded,
            DateTime::from_timestamp_millis(1789949000000).unwrap(),
        );
        let base = TablePath::new("mysql_shop", "customers_20260919");
        let renamed = renamed_to_clients(|| Some(base.clone())).remove(0);

        assert_eq!(
            renamed.build_log(&run, outcome.clone()),
            BuildLog::new(
                &run,
                Some(TableId::new("shop", "customers")),
                Some(TableId::new("shop", "clients")),
                Some(base.clone()),
                Some(TablePath::new("mysql_shop", "clients_20260920")),
                window(),
                outcome,
            )
        );
    }

    #[test]
    fn a_table_created_within_the_window_is_logged_as_opening_it_under_no_name() {
        let run = Run::new(
            ulid::Ulid::nil(),
            DateTime::UNIX_EPOCH,
            "snapshot",
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let outcome = Outcome::new(
            Status::Succeeded,
            DateTime::from_timestamp_millis(1789949000000).unwrap(),
        );
        let orders = TableModel::new(
            TableId::new("shop", "orders"),
            vec![column("id", ColumnType::Int64)],
            vec!["id".to_owned()],
        );
        let created = snapshots(
            customers(),
            vec![change(150, TableChange::Created(orders))],
            || Some(TablePath::new("mysql_shop", "customers_20260919")),
        )
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.table() == &TableId::new("shop", "orders"))
        .unwrap();

        assert_eq!(
            created.build_log(&run, outcome.clone()),
            BuildLog::new(
                &run,
                None,
                Some(TableId::new("shop", "orders")),
                None,
                Some(TablePath::new("mysql_shop", "orders_20260920")),
                window(),
                outcome,
            )
        );
    }

    #[test]
    fn a_table_captured_within_the_window_is_logged_as_opening_it_under_its_name() {
        let run = Run::new(
            ulid::Ulid::nil(),
            DateTime::UNIX_EPOCH,
            "snapshot",
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let outcome = Outcome::new(
            Status::Succeeded,
            DateTime::from_timestamp_millis(1789949000000).unwrap(),
        );
        let orders = TableModel::new(
            TableId::new("shop", "orders"),
            vec![column("id", ColumnType::Int64)],
            vec!["id".to_owned()],
        );
        let captured = snapshots(
            customers(),
            vec![change(150, TableChange::Created(orders)).captured()],
            || None,
        )
        .unwrap()
        .into_iter()
        .find(|snapshot| snapshot.table() == &TableId::new("shop", "orders"))
        .unwrap();

        assert_eq!(
            captured.build_log(&run, outcome.clone()).table_from(),
            Some(&TableId::new("shop", "orders"))
        );
    }

    #[test]
    fn a_table_dropped_within_the_window_is_logged_without_a_target() {
        let run = Run::new(
            ulid::Ulid::nil(),
            DateTime::UNIX_EPOCH,
            "snapshot",
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let outcome = Outcome::new(
            Status::Succeeded,
            DateTime::from_timestamp_millis(1789949000000).unwrap(),
        );
        let base = TablePath::new("mysql_shop", "customers_20260919");
        let dropped = snapshots(
            customers(),
            vec![change(
                150,
                TableChange::Dropped(TableId::new("shop", "customers")),
            )],
            || Some(base.clone()),
        )
        .unwrap()
        .remove(0);

        assert_eq!(
            dropped.create(&Engine::default()).unwrap(),
            Vec::<SqlString>::new()
        );
        assert_eq!(
            dropped.build_log(&run, outcome.clone()),
            BuildLog::new(
                &run,
                Some(TableId::new("shop", "customers")),
                None,
                Some(base),
                None,
                window(),
                outcome,
            )
        );
    }

    #[test]
    fn a_table_renamed_and_then_dropped_within_the_window_is_logged_from_its_first_name() {
        let run = Run::new(
            ulid::Ulid::nil(),
            DateTime::UNIX_EPOCH,
            "snapshot",
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let outcome = Outcome::new(
            Status::Succeeded,
            DateTime::from_timestamp_millis(1789949000000).unwrap(),
        );
        let clients = TableId::new("shop", "clients");
        let dropped = snapshots(
            customers(),
            vec![
                hinted(
                    150,
                    vec![moved_to(&clients)],
                    vec![TableChange::Altered(customers().with_id(&clients))],
                ),
                change(160, TableChange::Dropped(clients)),
            ],
            || None,
        )
        .unwrap()
        .remove(0)
        .build_log(&run, outcome);

        assert_eq!(
            dropped.table_from(),
            Some(&TableId::new("shop", "customers"))
        );
        assert_eq!(dropped.lifecycle(), Some(Lifecycle::Dropped));
    }

    #[test]
    fn a_table_discovered_within_the_window_misses_a_base_it_was_not_given_but_one_created_does_not()
     {
        let orders = TableModel::new(
            TableId::new("shop", "orders"),
            vec![column("id", ColumnType::Int64)],
            vec!["id".to_owned()],
        );
        let missing = |change: TableChange| {
            snapshots(customers(), vec![self::change(150, change)], || None)
                .unwrap()
                .into_iter()
                .find(|snapshot| snapshot.table() == &TableId::new("shop", "orders"))
                .unwrap()
                .missing_base()
                .cloned()
        };

        assert_eq!(
            missing(TableChange::Altered(orders.clone())),
            Some(TableId::new("shop", "orders"))
        );
        assert_eq!(missing(TableChange::Created(orders)), None);
    }

    #[test]
    fn a_snapshot_is_created_from_the_select_its_builder_folds() {
        let snapshot = snapshots(customers(), vec![], || None).unwrap().remove(0);
        let select = snapshot.select().unwrap();

        assert!(select.as_str().starts_with("WITH \"base\" AS (\n"));
        assert_eq!(
            snapshot.create(&Engine::default()).unwrap()[1],
            format!("CREATE OR REPLACE TABLE \"mysql_shop\".\"customers_20260920\" AS\n{select}")
                .as_str()
        );
    }
}
