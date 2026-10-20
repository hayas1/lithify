use std::ops::{self, RangeBounds, RangeFrom};

use serde::Deserialize;

use lithify_core::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use lithify_core::build_log::{BuildLog, Outcome, Run, Series};
use lithify_core::era::{Bound, Era};
use lithify_core::event::SchemaChange;
use lithify_core::listener::{
    SchemaChangeListener, TableAltered, TableCreated, TableDropped, TableRenamed,
};
use lithify_core::model::TableId;
use lithify_core::schedule::Timeline;
use lithify_core::sink::{TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
use lithify_core::sql::SqlString;
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

use crate::snapshot::{Snapshot, SnapshotBuilder};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scd1 {
    snapshot: Snapshot,
}

impl Scd1 {
    pub fn new(snapshot: Snapshot) -> Self {
        Self { snapshot }
    }
}

impl Artifact for Scd1 {
    const KIND: &'static str = "scd1";

    type From = Snapshot;

    type Open = RangeFrom<Ts<Event>>;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = Scd1Builder<'a, L, W>;

    fn series(&self) -> Series {
        Series::new(Self::KIND, "current")
    }

    fn window(&self, interval: &Window, until: Ts<Event>) -> Option<Window> {
        (*interval.to() < until).then(|| Window::new(*interval.to(), until))
    }

    fn open(&self, window: &Window) -> Self::Open {
        *window.to()..
    }

    fn next(
        &self,
        timeline: &Timeline,
        built: Option<&Window>,
        base: Option<Ts<Event>>,
    ) -> Option<Window> {
        let interval = timeline.ending(base?)?;

        self.window(&interval, timeline.until())
            .filter(|window| built.is_none_or(|built| built.from() < window.from()))
    }

    fn base<'b>(
        &self,
        built: &'b [BuildLog],
        table: &TableId,
        window: &Window,
    ) -> Option<&'b BuildLog> {
        Snapshot::shard_closing(built, table, *window.from())
    }

    fn target(&self, table: &TableId, window: &Window) -> Result<TablePath, UnbuildableArtifact> {
        Ok(self.snapshot.placed(table, window))
    }

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        log: &'a L,
        table: TableId,
        window: &'a Window,
        open: Self::Open,
        previous: Option<&BuildLog>,
    ) -> Result<Self::Builder<'a, L, W>, UnbuildableArtifact> {
        Ok(Scd1Builder {
            snapshot: SnapshotBuilder::new(
                &self.snapshot,
                log,
                table,
                window.clone(),
                previous.and_then(BuildLog::target).cloned(),
            ),
            open,
        })
    }
}

pub struct Scd1Builder<'a, L, W> {
    snapshot: SnapshotBuilder<'a, L, W>,
    open: RangeFrom<Ts<Event>>,
}

impl<L: RowChangeLog, W: Warehouse> Scd1Builder<'_, L, W> {
    fn reaching(&self, era: &Era<L::Position>) -> Era<L::Position> {
        let to = match era.to() {
            Bound::EventTime(to) if *to == self.open.start => match self.open.end_bound() {
                ops::Bound::Included(end) | ops::Bound::Excluded(end) => Bound::EventTime(*end),
                ops::Bound::Unbounded => Bound::Open,
            },
            to => to.clone(),
        };

        Era::new(era.table().clone(), era.from().clone(), to)
    }
}

impl<L: RowChangeLog, W: Warehouse> SchemaChangeListener<L::Position> for Scd1Builder<'_, L, W> {
    type Context = W;
    type Error = UnbuildableArtifact;

    fn visit_statement(
        &mut self,
        warehouse: &W,
        change: &SchemaChange<L::Position>,
    ) -> Result<(), UnbuildableArtifact> {
        self.snapshot.visit_statement(warehouse, change)
    }

    fn leave_statement(
        &mut self,
        warehouse: &W,
        change: &SchemaChange<L::Position>,
    ) -> Result<(), UnbuildableArtifact> {
        self.snapshot.leave_statement(warehouse, change)
    }

    fn era_boundary(
        &mut self,
        warehouse: &W,
        closed_era: &Era<L::Position>,
    ) -> Result<(), UnbuildableArtifact> {
        let reached = self.reaching(closed_era);

        self.snapshot.era_boundary(warehouse, &reached)
    }

    fn table_created(
        &mut self,
        warehouse: &W,
        event: &TableCreated<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        self.snapshot.table_created(warehouse, event)
    }

    fn table_dropped(
        &mut self,
        warehouse: &W,
        event: &TableDropped<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        self.snapshot.table_dropped(warehouse, event)
    }

    fn table_renamed(
        &mut self,
        warehouse: &W,
        event: &TableRenamed<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        self.snapshot.table_renamed(warehouse, event)
    }

    fn table_altered(
        &mut self,
        warehouse: &W,
        event: &TableAltered<'_>,
    ) -> Result<(), UnbuildableArtifact> {
        self.snapshot.table_altered(warehouse, event)
    }
}

impl<L: RowChangeLog, W: Warehouse> ArtifactBuilder for Scd1Builder<'_, L, W> {
    fn table(&self) -> &TableId {
        self.snapshot.table()
    }

    fn missing_base(&self) -> Option<&TableId> {
        self.snapshot.missing_base()
    }

    fn select(&self) -> Result<SqlString, UnbuildableArtifact> {
        self.snapshot.select()
    }

    fn create<V: Warehouse>(&self, warehouse: &V) -> Result<Vec<SqlString>, UnbuildableArtifact> {
        self.snapshot.create(warehouse)
    }

    fn build_log(&self, run: &Run, outcome: Outcome) -> BuildLog {
        self.snapshot.build_log(run, outcome)
    }
}

#[cfg(test)]
mod tests {
    use lithify_core::schedule::{Schedule, Timeline};
    use lithify_core::ts::Ts;

    use super::*;

    #[test]
    fn an_scd1_is_one_table_named_after_the_table_it_holds_the_current_state_of() {
        assert_eq!(
            Scd1::new(Snapshot::current("mysql_shop_scd1"))
                .target(
                    &TableId::new("shop", "people"),
                    &Window::new(Ts::try_from(10).unwrap(), Ts::try_from(20).unwrap())
                )
                .unwrap(),
            TablePath::new("mysql_shop_scd1", "people")
        );
    }

    fn until(until: i64) -> Timeline {
        Timeline::new(
            Schedule::default(),
            Ts::try_from(0).unwrap(),
            Ts::try_from(until).unwrap(),
        )
    }

    const DAY: i64 = 86_400_000;

    fn day(from: i64) -> Window {
        Window::new(
            Ts::try_from(from * DAY).unwrap(),
            Ts::try_from((from + 1) * DAY).unwrap(),
        )
    }

    fn scd1() -> Scd1 {
        Scd1::new(Snapshot::current("mysql_shop_scd1"))
    }

    fn ts(at: i64) -> Ts<Event> {
        Ts::try_from(at).unwrap()
    }

    #[test]
    fn an_scd1_updates_from_the_end_of_the_day_it_is_built_onto_up_to_now() {
        assert_eq!(
            scd1().window(&day(0), ts(DAY + 5)),
            Some(Window::new(ts(DAY), ts(DAY + 5)))
        );
        assert_eq!(scd1().window(&day(0), ts(DAY)), None);
    }

    #[test]
    fn an_scd1_stays_open_past_where_its_window_closes() {
        assert_eq!(
            scd1().open(&Window::new(ts(DAY), ts(DAY + 5))),
            ts(DAY + 5)..
        );
    }

    #[test]
    fn an_scd1_is_built_once_onto_each_day_its_snapshot_is_built_for() {
        let built = Window::new(ts(DAY), ts(DAY + 5));

        assert_eq!(
            scd1().next(&until(DAY + 5), None, Some(ts(DAY))),
            Some(built.clone())
        );
        assert_eq!(
            scd1().next(&until(DAY + 7), Some(&built), Some(ts(DAY))),
            None
        );
        assert_eq!(
            scd1().next(&until(2 * DAY + 5), Some(&built), Some(ts(2 * DAY))),
            Some(Window::new(ts(2 * DAY), ts(2 * DAY + 5)))
        );
        assert_eq!(scd1().next(&until(DAY + 5), None, None), None);
    }
}
