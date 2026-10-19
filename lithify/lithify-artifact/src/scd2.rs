use std::marker::PhantomData;

use chrono_tz::Tz;
use serde::Deserialize;

use lithify_core::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use lithify_core::build_log::{BuildLog, Outcome, Run, Series};
use lithify_core::era::Era;
use lithify_core::listener::{
    Alteration, Appearance, SchemaChangeListener, TableAltered, TableCreated, TableDropped,
    TableRenamed,
};
use lithify_core::model::TableId;
use lithify_core::schedule::Schedule;
use lithify_core::sink::{Namespace, TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
use lithify_core::sql::SqlString;
use lithify_core::window::Window;

use crate::snapshot::Snapshot;
use crate::snapshot::query::ChangeLogQuery;

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
    open: Snapshot,
    closed: Snapshot,
}

impl Scd2 {
    pub const VALID_FROM: &'static str = "_valid_from";
    pub const VALID_TO: &'static str = "_valid_to";

    pub fn new(namespace: Namespace, shard: &str, zone: Tz) -> Self {
        Self {
            open: Snapshot::new(namespace.clone(), &format!("open_{shard}")).named_in(zone),
            closed: Snapshot::new(namespace, &format!("closed_{shard}")).named_in(zone),
        }
    }

    pub fn closed(
        &self,
        table: &TableId,
        window: &Window,
    ) -> Result<TablePath, UnbuildableArtifact> {
        self.closed.target(table, window)
    }
}

impl Artifact for Scd2 {
    const KIND: &'static str = "scd2";

    type From = Self;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = Scd2Builder<'a, L, W>;

    fn series(&self) -> Series {
        Series::new(Self::KIND, &Schedule::default().to_string())
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
        self.open.target(table, window)
    }

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        log: &'a L,
        table: TableId,
        window: &'a Window,
        previous: Option<&BuildLog>,
    ) -> Result<Self::Builder<'a, L, W>, UnbuildableArtifact> {
        Ok(Scd2Builder {
            scd2: self,
            query: ChangeLogQuery::new(log),
            target: Some(self.target(&table, window)?),
            table_from: Some(table.clone()),
            table,
            appearance: None,
            window: window.clone(),
            base: previous.and_then(BuildLog::target).cloned(),
            versions: None,
            warehouse: PhantomData,
        })
    }
}

pub struct Scd2Builder<'a, L, W> {
    scd2: &'a Scd2,
    query: ChangeLogQuery<'a, L>,
    table_from: Option<TableId>,
    table: TableId,
    appearance: Option<Appearance>,
    window: Window,
    base: Option<TablePath>,
    target: Option<TablePath>,
    versions: Option<Versions>,
    warehouse: PhantomData<fn(&W)>,
}

struct Versions {
    closed: SqlString,
    open: SqlString,
}

impl<L: RowChangeLog, W: Warehouse> Scd2Builder<'_, L, W> {
    fn across_eras(&self) -> UnbuildableArtifact {
        UnbuildableArtifact::AcrossEras(self.table.clone())
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
        let opened = match &self.base {
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

    fn table_created(&mut self, _: &W, event: &TableCreated<'_>) -> Result<(), Self::Error> {
        self.appearance = Some(event.appearance());
        if event.appearance().is_creation() {
            self.table_from = None;
        }
        if event.appearance().is_logged_from_start() {
            self.base = None;
        }

        Ok(())
    }

    fn table_dropped(&mut self, _: &W, _: &TableDropped<'_>) -> Result<(), Self::Error> {
        self.target = None;

        Ok(())
    }

    fn era_boundary(
        &mut self,
        warehouse: &W,
        closed_era: &Era<L::Position>,
    ) -> Result<(), Self::Error> {
        if self.versions.is_some() {
            return Err(self.across_eras());
        }
        if closed_era.table().primary_key().next().is_none() {
            return Err(UnbuildableArtifact::WithoutPrimaryKey(self.table.clone()));
        }
        self.versions = Some(self.versions(warehouse, closed_era)?);

        Ok(())
    }

    fn table_renamed(&mut self, _: &W, _: &TableRenamed<'_>) -> Result<(), Self::Error> {
        Err(self.across_eras())
    }

    fn table_altered(&mut self, _: &W, event: &TableAltered<'_>) -> Result<(), Self::Error> {
        match event.alterations().iter().all(Alteration::is_metadata_only) {
            true => Ok(()),
            false => Err(self.across_eras()),
        }
    }
}

impl<L: RowChangeLog, W: Warehouse> ArtifactBuilder for Scd2Builder<'_, L, W> {
    fn table(&self) -> &TableId {
        &self.table
    }

    fn missing_base(&self) -> Option<&TableId> {
        let logged_from_start = self
            .appearance
            .is_some_and(|appearance| appearance.is_logged_from_start());

        match logged_from_start || self.base.is_some() {
            true => None,
            false => self.table_from.as_ref(),
        }
    }

    fn select(&self) -> Result<SqlString, UnbuildableArtifact> {
        self.versions
            .as_ref()
            .map(|versions| versions.open.clone())
            .ok_or_else(|| UnbuildableArtifact::WithoutEra(self.table.clone()))
    }

    fn create<V: Warehouse>(&self, warehouse: &V) -> Result<Vec<SqlString>, UnbuildableArtifact> {
        let Some(target) = &self.target else {
            return Ok(Vec::new());
        };
        let versions = self
            .versions
            .as_ref()
            .ok_or_else(|| UnbuildableArtifact::WithoutEra(self.table.clone()))?;
        let closed = self.scd2.closed(&self.table, &self.window)?;

        Ok(vec![
            warehouse.create_namespace(target.namespace()),
            warehouse.create_table_as(&closed, &versions.closed),
            warehouse.create_table_as(target, &versions.open),
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
    use lithify_core::schedule::Timeline;
    use lithify_core::ts::Ts;

    use super::*;

    const DAY: i64 = 86_400_000;

    #[test]
    fn an_scd2_appends_each_day_after_the_last_it_appended_as_a_snapshot_is_built() {
        let scd2 = Scd2::new(Namespace::from("mysql_shop_scd2"), "%Y%m%d", Tz::UTC);
        let timeline = Timeline::new(
            Schedule::default(),
            Ts::try_from(0).unwrap(),
            Ts::try_from(DAY * 3 + 5).unwrap(),
        );
        let first = scd2.next(&timeline, None, None).unwrap();

        assert_eq!(
            first,
            Window::new(Ts::try_from(0).unwrap(), Ts::try_from(DAY * 3).unwrap())
        );
        assert_eq!(scd2.next(&timeline, Some(&first), None), None);
    }
}
