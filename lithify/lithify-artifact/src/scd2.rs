use std::convert::Infallible;
use std::marker::PhantomData;

use chrono_tz::Tz;
use serde::Deserialize;

use lithify_core::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use lithify_core::build_log::{Base, BuildLog, Series, TableBuild};
use lithify_core::era::{Bound, Era, EraTracker};
use lithify_core::listener::{
    Alteration, Finish, ReplayEnd, SchemaChangeListener, TableAltered, TableCreated, TableRenamed,
};
use lithify_core::model::TableId;
use lithify_core::schedule::{Schedule, Timeline};
use lithify_core::sink::{Namespace, TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
use lithify_core::sql::SqlString;
use lithify_core::window::Window;

use crate::scd1::query::ChangeLogQuery;
use crate::shard::Shard;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    shard: String,
}

impl Config {
    pub fn shard(&self) -> &str {
        &self.shard
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scd2 {
    namespace: Namespace,
    open: Shard,
    closed: Shard,
}

impl Scd2 {
    pub const VALID_FROM: &'static str = "_valid_from";
    pub const VALID_TO: &'static str = "_valid_to";

    pub fn new(namespace: Namespace, shard: &str, zone: Tz) -> Self {
        Self {
            namespace,
            open: Shard::new(&format!("open_{shard}"), zone),
            closed: Shard::new(&format!("closed_{shard}"), zone),
        }
    }

    // The versions of a day are those it closed and those it left open as it ended, so both are
    // named after the last instant the window holds, as a snapshot of that day is.
    pub fn closed(&self, table: &TableId, window: &Window) -> TablePath {
        TablePath::new(
            self.namespace.clone(),
            &self.closed.name(table, window.last()),
        )
    }
}

impl Artifact for Scd2 {
    const KIND: &'static str = "scd2";

    type From = Self;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = Scd2Builder<'a, L, W>;

    fn series(&self) -> Series {
        Series::new(Self::KIND, &Schedule::default().to_string())
    }

    fn window(&self, day: &Window, _: &Timeline) -> Option<Window> {
        Some(day.clone())
    }

    fn base<'b>(
        &self,
        built: &'b [BuildLog],
        table: &TableId,
        window: &Window,
    ) -> Option<&'b BuildLog> {
        built.iter().find(|log| {
            log.advances(&self.series())
                && log.table_to() == Some(table)
                && log.window().to() == window.from()
        })
    }

    fn target(&self, table: &TableId, window: &Window) -> Result<TablePath, UnbuildableArtifact> {
        Ok(TablePath::new(
            self.namespace.clone(),
            &self.open.name(table, window.last()),
        ))
    }

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        log: &'a L,
        table: TableId,
        window: &'a Window,
        previous: Option<&BuildLog>,
    ) -> Self::Builder<'a, L, W> {
        Scd2Builder {
            scd2: self,
            query: ChangeLogQuery::new(log),
            window: window.clone(),
            base: Base::from(previous.and_then(BuildLog::target).cloned()),
            eras: EraTracker::since(Bound::EventTime(*window.from())),
            opened_as: Some(table),
            warehouse: PhantomData,
        }
    }
}

// Holds what Scd1Builder holds but its fold, as an scd2 refuses a window its table changed its
// schema within, and so folds the versions of that table from its one era as it is finished.
pub struct Scd2Builder<'a, L: RowChangeLog, W> {
    scd2: &'a Scd2,
    query: ChangeLogQuery<'a, L>,
    window: Window,
    base: Base,
    eras: EraTracker<L::Position>,
    // The table as the window opened, which the build log records; None for one created within it.
    opened_as: Option<TableId>,
    warehouse: PhantomData<fn(&W)>,
}

struct Versions {
    closed: SqlString,
    open: SqlString,
}

impl<L: RowChangeLog, W: Warehouse> Scd2Builder<'_, L, W> {
    fn target(&self, end: &ReplayEnd<UnbuildableArtifact>) -> Option<TablePath> {
        end.model()
            .and_then(|table| self.scd2.target(table.id(), &self.window).ok())
    }

    fn versions_at(
        &self,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
    ) -> Result<Option<Versions>, UnbuildableArtifact> {
        if let Some(refused) = end.refusal() {
            return Err(refused.clone());
        }
        let Some(table) = end.model() else {
            return Ok(None);
        };
        if table.primary_key().next().is_none() {
            return Err(UnbuildableArtifact::WithoutPrimaryKey(table.id().clone()));
        }
        let era = self.eras.last(table, Bound::EventTime(*self.window.to()));

        self.versions(warehouse, &era).map(Some)
    }

    fn versions(
        &self,
        warehouse: &W,
        era: &Era<L::Position>,
    ) -> Result<Versions, UnbuildableArtifact> {
        let table = era.table();
        let ident = |name: &str| warehouse.identifier(name);
        let [changes, versions, base] = ["changes", "versions", "base"].map(ident);
        let [op, at, next, last, valid_from, valid_to] = [
            "_op",
            "_at",
            "_next",
            "_last",
            Scd2::VALID_FROM,
            Scd2::VALID_TO,
        ]
        .map(ident);
        let columns: Vec<String> = table
            .columns()
            .map(|column| ident(column.name()).to_string())
            .collect();
        let columns = columns.join(", ");
        let keys: Vec<SqlString> = table.primary_key().map(ident).collect();
        let partition = keys
            .iter()
            .map(SqlString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let sequence = self.query.sequenced(warehouse);
        let ordered = |direction: &str| {
            sequence
                .iter()
                .map(|column| format!("{column}{direction}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let matched = keys
            .iter()
            .map(|key| format!("{changes}.{key} = {base}.{key}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let read = self.query.versions(
            warehouse,
            &self.query.change_log(warehouse, table.id()),
            era,
        )?;
        let opened = match self.base.as_built() {
            Some(base) => format!(
                "  SELECT {columns}, {valid_from}, {valid_to}\n  FROM {}",
                warehouse.table_ref(base)
            ),
            None => format!(
                "  SELECT {columns}, {at} AS {valid_from}, {at} AS {valid_to}\n  FROM {changes}\n  WHERE FALSE"
            ),
        };
        let with = format!(
            concat!(
                "WITH {changes} AS (\n{read}\n),\n",
                "{versions} AS (\n",
                "  SELECT *,\n",
                "    LEAD({at}) OVER (PARTITION BY {partition} ORDER BY {forward}) AS {next},\n",
                "    ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {backward}) AS {last}\n",
                "  FROM {changes}\n",
                "),\n",
                "{base} AS (\n{opened}\n)\n"
            ),
            changes = changes,
            read = read,
            versions = versions,
            at = at,
            partition = partition,
            forward = ordered(""),
            backward = ordered(" DESC"),
            next = next,
            last = last,
            base = base,
            opened = opened,
        );

        Ok(Versions {
            closed: SqlString::from(format!(
                concat!(
                    "{with}",
                    "SELECT {columns}, {valid_from}, (SELECT MIN({changes}.{at}) FROM {changes} WHERE {matched}) AS {valid_to}\n",
                    "FROM {base}\n",
                    "WHERE EXISTS (SELECT 1 FROM {changes} WHERE {matched})\n",
                    "UNION ALL\n",
                    "SELECT {columns}, {at} AS {valid_from}, {next} AS {valid_to}\n",
                    "FROM {versions}\n",
                    "WHERE {op} <> 'd' AND {next} IS NOT NULL"
                ),
                with = with,
                columns = columns,
                valid_from = valid_from,
                changes = changes,
                at = at,
                matched = matched,
                valid_to = valid_to,
                base = base,
                next = next,
                versions = versions,
                op = op,
            )),
            open: SqlString::from(format!(
                concat!(
                    "{with}",
                    "SELECT {columns}, {valid_from}, {valid_to}\n",
                    "FROM {base}\n",
                    "WHERE NOT EXISTS (SELECT 1 FROM {changes} WHERE {matched})\n",
                    "UNION ALL\n",
                    "SELECT {columns}, {at} AS {valid_from}, NULLIF({at}, {at}) AS {valid_to}\n",
                    "FROM {versions}\n",
                    "WHERE {op} <> 'd' AND {last} = 1"
                ),
                with = with,
                columns = columns,
                valid_from = valid_from,
                valid_to = valid_to,
                base = base,
                changes = changes,
                matched = matched,
                at = at,
                versions = versions,
                op = op,
                last = last,
            )),
        })
    }
}

impl<L: RowChangeLog, W: Warehouse> SchemaChangeListener<L::Position> for Scd2Builder<'_, L, W> {
    type Context = W;
    type Error = UnbuildableArtifact;

    fn table_created(
        &mut self,
        _: &W,
        event: &TableCreated<'_, L::Position>,
    ) -> Result<(), Self::Error> {
        self.eras = EraTracker::since(Bound::from(event.change()));
        self.base.appeared(event.appearance());
        if event.appearance().is_creation() {
            self.opened_as = None;
        }

        Ok(())
    }

    // TODO: carry the versions across a schema change, closing the era before it as the builder of
    // an scd1 does, rather than refusing the window.
    fn table_renamed(
        &mut self,
        _: &W,
        event: &TableRenamed<'_, L::Position>,
    ) -> Result<(), Self::Error> {
        Err(UnbuildableArtifact::AcrossEras(event.before().id().clone()))
    }

    fn table_altered(
        &mut self,
        _: &W,
        event: &TableAltered<'_, L::Position>,
    ) -> Result<(), Self::Error> {
        match event.alterations().iter().all(Alteration::is_metadata_only) {
            true => Ok(()),
            false => Err(UnbuildableArtifact::AcrossEras(event.before().id().clone())),
        }
    }
}

impl<L: RowChangeLog, W: Warehouse> Finish<L::Position> for Scd2Builder<'_, L, W> {
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

impl<L: RowChangeLog, W: Warehouse> ArtifactBuilder<L::Position, W> for Scd2Builder<'_, L, W> {
    fn select(
        &self,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
    ) -> Result<SqlString, UnbuildableArtifact> {
        self.versions_at(warehouse, end)?
            .map(|versions| versions.open)
            .ok_or_else(|| UnbuildableArtifact::WithoutEra(end.table().clone()))
    }

    fn create(
        &self,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
    ) -> Result<Vec<SqlString>, UnbuildableArtifact> {
        let versions = self.versions_at(warehouse, end)?;
        let (Some(target), Some(versions)) = (self.target(end), versions) else {
            return Ok(Vec::new());
        };
        let closed = self.scd2.closed(end.table(), &self.window);

        Ok(vec![
            warehouse.create_namespace(target.namespace()),
            warehouse.create_table_as(&closed, &versions.closed),
            warehouse.create_table_as(&target, &versions.open),
        ])
    }
}

#[cfg(test)]
mod tests {
    use lithify_core::event::{SchemaChange, TableChange};
    use lithify_core::listener::{CatalogModel, OutOfOrder};
    use lithify_core::model::{ColumnModel, ColumnType, TableModel};
    use lithify_core::schedule::Timeline;
    use lithify_core::testing::Engine;
    use lithify_core::ts::Ts;

    use super::*;
    use lithify_core::testing::{Log, Offset, Untold};

    const DAY: i64 = 86_400_000;

    #[test]
    fn an_scd2_appends_the_day_it_is_built_for() {
        let scd2 = Scd2::new(Namespace::from("mysql_shop_scd2"), "%Y%m%d", Tz::UTC);
        let timeline = Timeline::new(
            Schedule::default(),
            Ts::try_from(0).unwrap()..Ts::try_from(DAY * 3 + 5).unwrap(),
        );
        let day = Window::new(Ts::try_from(DAY).unwrap(), Ts::try_from(DAY * 2).unwrap());

        assert_eq!(scd2.window(&day, &timeline), Some(day));
    }

    fn people(columns: &[&str]) -> TableModel {
        TableModel::new(
            TableId::new("shop", "people"),
            columns
                .iter()
                .map(|name| ColumnModel::new(name, "BIGINT", ColumnType::Int64, false, None))
                .collect(),
            vec!["id".to_owned()],
        )
    }

    fn change(at: i64, table: TableChange) -> SchemaChange<Offset> {
        SchemaChange::new(
            Offset(at as u64),
            Ts::try_from(at).unwrap(),
            "",
            vec![table],
            vec![],
        )
    }

    #[test]
    fn an_scd2_whose_table_changed_its_schema_within_the_window_is_refused() {
        let scd2 = Scd2::new(Namespace::from("mysql_shop_scd2"), "%Y%m%d", Tz::UTC);
        let window = Window::new(Ts::try_from(10).unwrap(), Ts::try_from(20).unwrap());
        let mut versions =
            scd2.builder::<Log, Engine>(&Log, TableId::new("shop", "people"), &window, None);
        let mut catalog = CatalogModel::default();
        catalog
            .accept::<OutOfOrder, _>(
                &(),
                &change(1, TableChange::Created(people(&["id"]))),
                &mut (),
            )
            .unwrap();

        assert_eq!(
            catalog.accept::<Untold, _>(
                &Engine::default(),
                &change(15, TableChange::Altered(people(&["id", "name"]))),
                &mut versions,
            ),
            Err(Untold::Unbuildable(UnbuildableArtifact::AcrossEras(
                TableId::new("shop", "people")
            )))
        );
    }
}
