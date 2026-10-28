use std::convert::Infallible;
use std::marker::PhantomData;

use serde::Deserialize;

use lithify_core::artifact::{
    Artifact, ArtifactBuilder, ArtifactName, Placement, UnbuildableArtifact,
};
use lithify_core::build_log::{Base, BuildLog, Series, TableBuild};
use lithify_core::era::{Bound, Era, EraTracker};
use lithify_core::listener::{
    Alteration, Finish, ReplayEnd, SchemaChangeListener, TableAltered, TableCreated, TableRenamed,
};
use lithify_core::model::{ColumnModel, TableId, TableModel};
use lithify_core::schedule::Timeline;
use lithify_core::sink::{Namespace, TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
use lithify_core::sql::SqlString;
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

use crate::snapshot::Snapshot;
use fold::Fold;
use query::ChangeLogQuery;

mod fold;
pub(crate) mod query;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scd1Config {
    base: ArtifactName,
}

impl Scd1Config {
    pub fn base(&self) -> &ArtifactName {
        &self.base
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scd1 {
    name: ArtifactName,
    base: Series,
    namespace: Namespace,
    copies: Vec<Namespace>,
}

impl Scd1 {
    pub fn new(namespace: impl Into<Namespace>) -> Self {
        Self {
            name: ArtifactName::from(Self::KIND),
            base: Snapshot::days(&ArtifactName::from(Snapshot::KIND)),
            namespace: namespace.into(),
            copies: Vec::new(),
        }
    }

    pub fn named(self, name: ArtifactName) -> Self {
        Self { name, ..self }
    }

    pub fn based_on(self, base: &Snapshot) -> Self {
        Self {
            base: base.series(),
            ..self
        }
    }

    pub fn copied_into(self, copies: Vec<Namespace>) -> Self {
        Self { copies, ..self }
    }
}

impl Artifact for Scd1 {
    const KIND: &'static str = "scd1";

    type From = Snapshot;

    type Config = Scd1Config;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = Scd1Builder<'a, Self, L, W>;

    fn declared(
        name: &ArtifactName,
        _: &Scd1Config,
        placement: &Placement,
        _: chrono_tz::Tz,
    ) -> Self {
        Self::new(placement.namespace().clone())
            .named(name.clone())
            .copied_into(placement.copies().to_vec())
    }

    fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    fn series(&self) -> Series {
        Series::new(self.name.clone(), "current")
    }

    fn window(&self, day: &Window, timeline: &Timeline) -> Option<Window> {
        match timeline.reached(day) {
            true => None,
            false => timeline.rest(*day.from()),
        }
    }

    fn base<'b>(
        &self,
        built: &'b [BuildLog],
        table: &TableId,
        window: &Window,
    ) -> Option<&'b BuildLog> {
        Snapshot::shard_closing(&self.base, built, table, *window.from())
    }

    fn target(&self, table: &TableId, _: &Window) -> Result<TablePath, UnbuildableArtifact> {
        Ok(TablePath::new(
            self.namespace.clone(),
            &format!("{}_scd_type_1", table.table()),
        ))
    }

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        log: &'a L,
        table: TableId,
        window: &'a Window,
        previous: Option<&BuildLog>,
    ) -> Self::Builder<'a, L, W> {
        Scd1Builder::new(
            self,
            log,
            table,
            window.clone(),
            previous.and_then(BuildLog::target).cloned(),
        )
    }
}

impl Overwrite for Scd1 {
    fn until(&self, _: &Window) -> Option<Ts<Event>> {
        None
    }

    fn copies(&self) -> &[Namespace] {
        &self.copies
    }

    fn materialize<W: Warehouse>(
        &self,
        warehouse: &W,
        target: &TablePath,
        select: &SqlString,
    ) -> SqlString {
        warehouse.create_view(target, select)
    }
}

pub trait Overwrite: Artifact {
    fn until(&self, window: &Window) -> Option<Ts<Event>>;

    fn copies(&self) -> &[Namespace];

    fn materialize<W: Warehouse>(
        &self,
        warehouse: &W,
        target: &TablePath,
        select: &SqlString,
    ) -> SqlString;
}

// A builder keeps what the listener tells it in the fold as it is told, and nothing else, so what
// it holds besides the fold is what it was handed when made, where the era running now began, and
// the name the table opened the window under, which the build log records.
pub struct Scd1Builder<'a, A, L: RowChangeLog, W> {
    artifact: &'a A,
    query: ChangeLogQuery<'a, L>,
    window: Window,
    base: Base,
    eras: EraTracker<L::Position>,
    // None for a table created within the window.
    opened_as: Option<TableId>,
    fold: Fold,
    warehouse: PhantomData<fn(&W)>,
}

struct Folding<'f, 'a, L> {
    fold: &'f mut Fold,
    query: &'f ChangeLogQuery<'a, L>,
    base: &'f Base,
}

impl<L: RowChangeLog> Folding<'_, '_, L> {
    fn begin<W: Warehouse>(
        &mut self,
        warehouse: &W,
        table: &TableModel,
    ) -> Result<(), UnbuildableArtifact> {
        if self.fold.previous().is_some() {
            return Ok(());
        }
        if table.primary_key().is_empty() {
            return Err(UnbuildableArtifact::WithoutPrimaryKey(table.id().clone()));
        }

        let base = self.select_base(warehouse, table)?;

        self.fold.push(warehouse.identifier("base"), base);

        Ok(())
    }

    fn select_base<W: Warehouse>(
        &self,
        warehouse: &W,
        table: &TableModel,
    ) -> Result<SqlString, UnbuildableArtifact> {
        match self.base.as_built() {
            Some(base) => Ok(self.query.shard(warehouse, base, table)),
            None => self.query.captured(
                warehouse,
                &self.query.change_log(warehouse, table.id()),
                table,
            ),
        }
    }

    fn previous(&self, table: &TableId) -> Result<SqlString, UnbuildableArtifact> {
        self.fold
            .previous()
            .cloned()
            .ok_or_else(|| UnbuildableArtifact::WithoutEra(table.clone()))
    }

    fn era<W: Warehouse>(
        &mut self,
        warehouse: &W,
        era: &Era<L::Position>,
    ) -> Result<(), UnbuildableArtifact> {
        self.begin(warehouse, era.table())?;

        let step = self.fold.step();
        let previous = self.previous(era.table().id())?;
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

    fn carry<W: Warehouse>(
        &mut self,
        warehouse: &W,
        next: &TableModel,
        carried: impl Fn(&ColumnModel, &SqlString) -> Result<SqlString, UnbuildableArtifact>,
    ) -> Result<(), UnbuildableArtifact> {
        let step = self.fold.step();
        let carry = self
            .query
            .carry(warehouse, next, &self.previous(next.id())?, carried)?;

        self.fold
            .push(warehouse.identifier(&format!("era_{step}")), carry);

        Ok(())
    }
}

impl<'a, A: Overwrite, L: RowChangeLog, W: Warehouse> Scd1Builder<'a, A, L, W> {
    pub(crate) fn new(
        artifact: &'a A,
        log: &'a L,
        table: TableId,
        window: Window,
        base: Option<TablePath>,
    ) -> Self {
        Self {
            artifact,
            query: ChangeLogQuery::new(log),
            eras: EraTracker::since(Bound::EventTime(*window.from())),
            window,
            base: Base::from(base),
            opened_as: Some(table),
            fold: Fold::default(),
            warehouse: PhantomData,
        }
    }

    fn folding(&mut self) -> Folding<'_, 'a, L> {
        Folding {
            fold: &mut self.fold,
            query: &self.query,
            base: &self.base,
        }
    }

    fn target(&self, end: &ReplayEnd<UnbuildableArtifact>) -> Option<TablePath> {
        end.model()
            .and_then(|table| self.artifact.target(table.id(), &self.window).ok())
    }

    fn closed(
        &self,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
    ) -> Result<Option<Fold>, UnbuildableArtifact> {
        if let Some(refused) = end.refusal() {
            return Err(refused.clone());
        }
        let Some(table) = end.model() else {
            return Ok(None);
        };
        let until = self
            .artifact
            .until(&self.window)
            .map_or(Bound::Open, Bound::EventTime);
        let mut fold = self.fold.clone();
        Folding {
            fold: &mut fold,
            query: &self.query,
            base: &self.base,
        }
        .era(warehouse, &self.eras.last(table, until))?;

        Ok(Some(fold))
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
                |(previous, _)| warehouse.column_ref(previous.name()),
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

impl<A: Overwrite, L: RowChangeLog, W: Warehouse> SchemaChangeListener<L::Position>
    for Scd1Builder<'_, A, L, W>
{
    type Context = W;
    type Error = UnbuildableArtifact;

    fn table_created(
        &mut self,
        _: &W,
        event: &TableCreated<'_, L::Position>,
    ) -> Result<(), UnbuildableArtifact> {
        self.eras = EraTracker::since(Bound::from(event.change()));
        self.base.appeared(event.appearance());
        if event.appearance().is_creation() {
            self.opened_as = None;
        }

        Ok(())
    }

    fn table_renamed(
        &mut self,
        warehouse: &W,
        event: &TableRenamed<'_, L::Position>,
    ) -> Result<(), UnbuildableArtifact> {
        if let Some(era) = self.eras.renamed(event) {
            self.folding().era(warehouse, &era)?;
        }

        self.folding().begin(warehouse, event.before())
    }

    fn table_altered(
        &mut self,
        warehouse: &W,
        event: &TableAltered<'_, L::Position>,
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
        if let Some(era) = self.eras.altered(event) {
            self.folding().era(warehouse, &era)?;
        }
        let mut folding = self.folding();
        folding.begin(warehouse, event.before())?;

        folding.carry(warehouse, event.after(), |column, name| {
            Self::carried(warehouse, event.alterations(), column, name)
        })
    }
}

impl<A: Overwrite, L: RowChangeLog, W: Warehouse> Finish<L::Position> for Scd1Builder<'_, A, L, W> {
    type Input = ReplayEnd<UnbuildableArtifact>;
    type Output = TableBuild;
    type FinishError = Infallible;

    fn finish(
        self,
        warehouse: &W,
        end: ReplayEnd<UnbuildableArtifact>,
    ) -> Result<TableBuild, Infallible> {
        Ok(TableBuild::new(
            self.opened_as.clone(),
            end.model().map(|table| table.id().clone()),
            self.base.clone(),
            self.target(&end),
            self.window.clone(),
            self.create(warehouse, &end),
        ))
    }
}

impl<A: Overwrite, L: RowChangeLog, W: Warehouse> ArtifactBuilder<L::Position, W>
    for Scd1Builder<'_, A, L, W>
{
    fn select(
        &self,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
    ) -> Result<SqlString, UnbuildableArtifact> {
        match self.closed(warehouse, end)? {
            Some(fold) => Ok(fold.select()),
            None => Err(UnbuildableArtifact::WithoutEra(end.table().clone())),
        }
    }

    fn create(
        &self,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
    ) -> Result<Vec<SqlString>, UnbuildableArtifact> {
        let Some(target) = self.target(end) else {
            if let Some(refused) = end.refusal() {
                return Err(refused.clone());
            }

            return Ok(Vec::new());
        };
        let select = self.select(warehouse, end)?;
        let copied = SqlString::from(format!("SELECT * FROM {}", warehouse.table_ref(&target)));

        Ok([
            warehouse.create_namespace(target.namespace()),
            self.artifact.materialize(warehouse, &target, &select),
        ]
        .into_iter()
        .chain(self.artifact.copies().iter().flat_map(|copy| {
            [
                warehouse.create_namespace(copy),
                self.artifact.materialize(
                    warehouse,
                    &TablePath::new(copy.clone(), target.name()),
                    &copied,
                ),
            ]
        }))
        .collect())
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono_tz::Tz;

    use lithify_core::build_log::{Run, TableBuilt};
    use lithify_core::schedule::{Schedule, Timeline};

    use super::*;
    use crate::shard::Shard;

    #[test]
    fn an_scd1_is_one_table_named_after_the_table_it_holds_the_current_state_of() {
        assert_eq!(
            Scd1::new("mysql_shop_scd1")
                .target(
                    &TableId::new("shop", "people"),
                    &Window::new(Ts::try_from(10).unwrap(), Ts::try_from(20).unwrap())
                )
                .unwrap(),
            TablePath::new("mysql_shop_scd1", "people_scd_type_1")
        );
    }

    fn until(until: i64) -> Timeline {
        Timeline::new(
            Schedule::default(),
            Ts::try_from(0).unwrap()..Ts::try_from(until).unwrap(),
        )
    }

    const DAY: i64 = 86_400_000;

    fn scd1() -> Scd1 {
        Scd1::new("mysql_shop_scd1")
    }

    fn ts(at: i64) -> Ts<Event> {
        Ts::try_from(at).unwrap()
    }

    #[test]
    fn an_scd1_updates_from_the_start_of_the_day_still_open_up_to_now() {
        let today = Window::new(ts(DAY), ts(2 * DAY));

        assert_eq!(
            scd1().window(&today, &until(DAY + 5)),
            Some(Window::new(ts(DAY), ts(DAY + 5)))
        );
        assert_eq!(scd1().window(&today, &until(2 * DAY)), None);
    }

    #[test]
    fn an_scd1_reads_the_change_log_past_where_its_window_closes() {
        assert_eq!(scd1().until(&Window::new(ts(DAY), ts(DAY + 5))), None);
    }

    use lithify_core::artifact::{Artifact, UnbuildableArtifact};
    use lithify_core::build_log::{Lifecycle, Status};
    use lithify_core::era::Bound;
    use lithify_core::event::{Hint, SchemaChange, TableChange};
    use lithify_core::listener::{CatalogModel, OutOfOrder};
    use lithify_core::model::{ColumnModel, ColumnType, TableId, TableModel};
    use lithify_core::position::Coordinate;
    use lithify_core::sink::{TablePath, UnsupportedColumn};
    use lithify_core::source::ColumnPath;
    use lithify_core::table_listeners::TableListeners;
    use lithify_testing::Engine;

    use lithify_core::ts::Ts;
    use lithify_core::window::Window;

    use lithify_testing::{Log, Offset, Untold};

    fn column(name: &str, column_type: ColumnType) -> ColumnModel {
        let type_name = format!("{column_type:?}");

        ColumnModel::new(name, &type_name, column_type, false, None)
    }

    fn customers_with(columns: Vec<ColumnModel>, key: &[&str]) -> TableModel {
        TableModel::new(
            TableId::new("shop", "customers"),
            columns,
            key.iter().map(|&column| column.into()).collect(),
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
            from: from.into(),
            to: to.into(),
        }
    }

    fn moved_to(table: &TableId) -> Hint {
        Hint::TableRename {
            from: TableId::new("shop", "customers"),
            to: table.clone(),
        }
    }

    fn shop() -> &'static Snapshot {
        Box::leak(Box::new(Snapshot::new(
            "mysql_shop",
            Shard::new("20260920", Tz::UTC),
        )))
    }

    fn window() -> Window {
        Window::new(Ts::try_from(100).unwrap(), Ts::try_from(200).unwrap())
    }

    struct Kept<'a, W>(Scd1Builder<'a, Snapshot, Log, W>);

    impl<W: Warehouse> SchemaChangeListener<Offset> for Kept<'_, W> {
        type Context = W;
        type Error = UnbuildableArtifact;

        fn table_created(
            &mut self,
            warehouse: &W,
            event: &TableCreated<'_, Offset>,
        ) -> Result<(), UnbuildableArtifact> {
            self.0.table_created(warehouse, event)
        }

        fn table_renamed(
            &mut self,
            warehouse: &W,
            event: &TableRenamed<'_, Offset>,
        ) -> Result<(), UnbuildableArtifact> {
            self.0.table_renamed(warehouse, event)
        }

        fn table_altered(
            &mut self,
            warehouse: &W,
            event: &TableAltered<'_, Offset>,
        ) -> Result<(), UnbuildableArtifact> {
            self.0.table_altered(warehouse, event)
        }
    }

    // Finished, a builder is left to the test with the end of its replay rather than built, so what
    // it selects can be read whatever it would be built onto.
    impl<'a, W: Warehouse + Clone> Finish<Offset> for Kept<'a, W> {
        type Input = ReplayEnd<UnbuildableArtifact>;
        type Output = Ended<'a, W>;
        type FinishError = UnbuildableArtifact;

        fn finish(
            self,
            warehouse: &W,
            end: ReplayEnd<UnbuildableArtifact>,
        ) -> Result<Ended<'a, W>, UnbuildableArtifact> {
            if end.model().is_some() {
                self.0.select(warehouse, &end)?;
            }

            Ok(Ended(self.0, end, warehouse.clone()))
        }
    }

    struct Ended<'a, W>(
        Scd1Builder<'a, Snapshot, Log, W>,
        ReplayEnd<UnbuildableArtifact>,
        W,
    );

    impl<W: Warehouse> Ended<'_, W> {
        fn table(&self) -> &TableId {
            self.1.table()
        }

        fn select(&self) -> Result<SqlString, UnbuildableArtifact> {
            self.0.select(&self.2, &self.1)
        }

        fn create(&self) -> Result<Vec<SqlString>, UnbuildableArtifact> {
            self.0.create(&self.2, &self.1)
        }

        fn build(self) -> TableBuild {
            Finish::<Offset>::finish(self.0, &self.2, self.1).unwrap()
        }
    }

    fn logged(
        snapshot: Ended<'_, Engine>,
        run: &Run,
        finished_at: DateTime<chrono::Utc>,
    ) -> BuildLog {
        snapshot
            .build()
            .built(Status::Succeeded, Ts::from(finished_at))
            .logged(run)
    }

    fn snapshots_in<W: Warehouse + Clone>(
        warehouse: W,
        created: TableModel,
        within: Vec<SchemaChange<Offset>>,
        base: impl Fn() -> Option<TablePath>,
    ) -> Result<Vec<Ended<'static, W>>, Untold> {
        let mut catalog = CatalogModel::default();
        catalog.accept::<OutOfOrder, _>(&(), &change(1, TableChange::Created(created)), &mut ())?;
        let mut listeners = TableListeners::new(
            |table: &TableId| {
                Kept(Scd1Builder::new(
                    shop(),
                    &Log,
                    table.clone(),
                    window(),
                    base(),
                ))
            },
            |_: &TableId| true,
        );
        for change in &within {
            catalog.accept::<Untold, _>(&warehouse, change, &mut listeners)?;
        }

        Ok(listeners.finish(&warehouse, catalog)?)
    }

    fn snapshots(
        created: TableModel,
        within: Vec<SchemaChange<Offset>>,
        base: impl Fn() -> Option<TablePath>,
    ) -> Result<Vec<Ended<'static, Engine>>, Untold> {
        snapshots_in(Engine::default(), created, within, base)
    }

    fn statement(snapshots: Result<Vec<Ended<'static, Engine>>, Untold>) -> String {
        snapshots.unwrap()[0]
            .create()
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
            .create()
            .unwrap();

        assert_eq!(statements[0], "CREATE SCHEMA IF NOT EXISTS \"mysql_shop\"");
    }

    #[test]
    fn a_snapshot_reads_the_change_log_where_the_warehouse_reads_change_logs() {
        let statement = statement(snapshots_in(
            Engine::default().reading_change_log("mysql_shop_customers", "changes_of_customers"),
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
            ["id"].into(),
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
            .create()
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
            ["id"].into(),
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
                .create()
                .unwrap()
                .remove(1)
                .to_string()
        };

        assert!(statement("customers").contains("  FROM \"mysql_shop\".\"customers_20260919\"\n"));
        assert!(statement("orders").contains("  WHERE \"op\" = 'r'\n"));
        assert!(!statement("orders").contains("customers_20260919"));
    }

    #[test]
    fn a_bound_in_event_time_is_placed_on_the_event_time_of_the_log() {
        assert_eq!(
            Bound::<Offset>::EventTime(Ts::try_from(150).unwrap()).placed(&Log),
            [(ColumnPath::from("source.ts_ms"), Coordinate::Number(150))]
        );
    }

    #[test]
    fn a_bound_at_a_position_is_placed_on_its_processing_time_then_the_columns_the_log_orders_by() {
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
    fn the_latest_change_of_a_key_is_the_last_in_processing_time_then_position() {
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

    fn renamed_to_clients(base: impl Fn() -> Option<TablePath>) -> Vec<Ended<'static, Engine>> {
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
        let statements = snapshots[0].create().unwrap();

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
            ["order_id", "line_no"].into(),
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
    fn a_snapshot_of_a_table_gone_by_the_end_of_its_window_selects_nothing_and_creates_nothing() {
        let snapshot: Scd1Builder<'_, Snapshot, Log, Engine> = Scd1Builder::new(
            shop(),
            &Log,
            TableId::new("shop", "customers"),
            window(),
            None,
        );
        let end = ReplayEnd::dropped(TableId::new("shop", "customers"), None);

        assert_eq!(
            snapshot.select(&Engine::default(), &end).unwrap_err(),
            UnbuildableArtifact::WithoutEra(TableId::new("shop", "customers"))
        );
        assert_eq!(
            snapshot.create(&Engine::default(), &end).unwrap(),
            Vec::<SqlString>::new()
        );
    }

    #[test]
    fn a_snapshot_hands_a_builder_that_builds_it_from_the_table_it_is_told_of() {
        let window = window();
        let fold =
            shop().builder::<Log, Engine>(&Log, TableId::new("shop", "customers"), &window, None);

        assert!(
            fold.create(&Engine::default(), &ReplayEnd::standing(customers(), None))
                .unwrap()[1]
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
            Ts::from(DateTime::UNIX_EPOCH),
            &"snapshot".into(),
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let finished_at = DateTime::from_timestamp_millis(1789949000000).unwrap();
        let previous = TableBuilt::new(
            Some(TableId::new("shop", "customers")),
            Some(TableId::new("shop", "customers")),
            None,
            Some(base.clone()),
            Window::new(Ts::try_from(0).unwrap(), Ts::try_from(100).unwrap()),
            Status::Succeeded,
            Ts::from(finished_at),
        )
        .logged(&run);
        let builder = shop().builder::<Log, Engine>(
            &Log,
            TableId::new("shop", "customers"),
            &window,
            Some(&previous),
        );

        assert_eq!(
            logged(
                Ended(
                    builder,
                    ReplayEnd::standing(customers(), None),
                    Engine::default()
                ),
                &run,
                finished_at
            ),
            TableBuilt::new(
                Some(TableId::new("shop", "customers")),
                Some(TableId::new("shop", "customers")),
                Some(base),
                Some(TablePath::new("mysql_shop", "customers_20260920")),
                window,
                Status::Succeeded,
                Ts::from(finished_at)
            )
            .logged(&run)
        );
    }

    #[test]
    fn a_table_renamed_within_the_window_is_logged_from_its_first_name_to_its_last() {
        let run = Run::new(
            ulid::Ulid::nil(),
            Ts::from(DateTime::UNIX_EPOCH),
            &"snapshot".into(),
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let finished_at = DateTime::from_timestamp_millis(1789949000000).unwrap();
        let base = TablePath::new("mysql_shop", "customers_20260919");
        let renamed = renamed_to_clients(|| Some(base.clone())).remove(0);

        assert_eq!(
            logged(renamed, &run, finished_at),
            TableBuilt::new(
                Some(TableId::new("shop", "customers")),
                Some(TableId::new("shop", "clients")),
                Some(base.clone()),
                Some(TablePath::new("mysql_shop", "clients_20260920")),
                window(),
                Status::Succeeded,
                Ts::from(finished_at)
            )
            .logged(&run)
        );
    }

    #[test]
    fn a_table_created_within_the_window_is_logged_as_opening_it_under_no_name() {
        let run = Run::new(
            ulid::Ulid::nil(),
            Ts::from(DateTime::UNIX_EPOCH),
            &"snapshot".into(),
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let finished_at = DateTime::from_timestamp_millis(1789949000000).unwrap();
        let orders = TableModel::new(
            TableId::new("shop", "orders"),
            vec![column("id", ColumnType::Int64)],
            ["id"].into(),
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
            logged(created, &run, finished_at),
            TableBuilt::new(
                None,
                Some(TableId::new("shop", "orders")),
                None,
                Some(TablePath::new("mysql_shop", "orders_20260920")),
                window(),
                Status::Succeeded,
                Ts::from(finished_at)
            )
            .logged(&run)
        );
    }

    #[test]
    fn a_table_captured_within_the_window_is_logged_as_opening_it_under_its_name() {
        let run = Run::new(
            ulid::Ulid::nil(),
            Ts::from(DateTime::UNIX_EPOCH),
            &"snapshot".into(),
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let finished_at = DateTime::from_timestamp_millis(1789949000000).unwrap();
        let orders = TableModel::new(
            TableId::new("shop", "orders"),
            vec![column("id", ColumnType::Int64)],
            ["id"].into(),
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
            logged(captured, &run, finished_at).table_from(),
            Some(&TableId::new("shop", "orders"))
        );
    }

    #[test]
    fn a_table_dropped_within_the_window_is_logged_without_a_target() {
        let run = Run::new(
            ulid::Ulid::nil(),
            Ts::from(DateTime::UNIX_EPOCH),
            &"snapshot".into(),
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let finished_at = DateTime::from_timestamp_millis(1789949000000).unwrap();
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

        assert_eq!(dropped.create().unwrap(), Vec::<SqlString>::new());
        assert_eq!(
            logged(dropped, &run, finished_at),
            TableBuilt::new(
                Some(TableId::new("shop", "customers")),
                None,
                Some(base),
                None,
                window(),
                Status::Succeeded,
                Ts::from(finished_at)
            )
            .logged(&run)
        );
    }

    #[test]
    fn a_table_renamed_and_then_dropped_within_the_window_is_logged_from_its_first_name() {
        let run = Run::new(
            ulid::Ulid::nil(),
            Ts::from(DateTime::UNIX_EPOCH),
            &"snapshot".into(),
            "daily",
            "primary",
            "{}",
            "0.1.0",
        );
        let finished_at = DateTime::from_timestamp_millis(1789949000000).unwrap();
        let clients = TableId::new("shop", "clients");
        let dropped = logged(
            snapshots(
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
            .remove(0),
            &run,
            finished_at,
        );

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
            ["id"].into(),
        );
        let missing = |change: TableChange| {
            snapshots(customers(), vec![self::change(150, change)], || None)
                .unwrap()
                .into_iter()
                .find(|snapshot| snapshot.table() == &TableId::new("shop", "orders"))
                .unwrap()
                .build()
                .refusal()
        };

        assert_eq!(
            missing(TableChange::Altered(orders.clone())),
            Some(UnbuildableArtifact::WithoutBase(TableId::new(
                "shop", "orders"
            )))
        );
        assert_eq!(missing(TableChange::Created(orders)), None);
    }

    #[test]
    fn a_snapshot_is_created_from_the_select_its_builder_folds() {
        let snapshot = snapshots(customers(), vec![], || None).unwrap().remove(0);
        let select = snapshot.select().unwrap();

        assert!(select.as_str().starts_with("WITH \"base\" AS (\n"));
        assert_eq!(
            snapshot.create().unwrap()[1],
            format!("CREATE OR REPLACE TABLE \"mysql_shop\".\"customers_20260920\" AS\n{select}")
                .as_str()
        );
    }

    #[test]
    fn a_snapshot_placed_more_than_once_is_copied_from_its_first_placement_into_the_others() {
        let artifact = Snapshot::new("mysql_shop", Shard::new("20260920", Tz::UTC))
            .copied_into(vec![lithify_core::sink::Namespace::from("analytics.shop")]);
        let window = window();
        let snapshot =
            artifact.builder::<Log, Engine>(&Log, customers().id().clone(), &window, None);

        let statements = snapshot
            .create(&Engine::default(), &ReplayEnd::standing(customers(), None))
            .unwrap();

        assert_eq!(
            statements[2..],
            [
                "CREATE SCHEMA IF NOT EXISTS \"analytics\".\"shop\"",
                "CREATE OR REPLACE TABLE \"analytics\".\"shop\".\"customers_20260920\" AS\nSELECT * FROM \"mysql_shop\".\"customers_20260920\""
            ]
        );
    }
}
