use serde::Deserialize;

use lithify_core::artifact::{Artifact, UnbuildableArtifact};
use lithify_core::build_log::{BuildLog, Series};
use lithify_core::model::TableId;
use lithify_core::schedule::Timeline;
use lithify_core::sink::{TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
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

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = SnapshotBuilder<'a, L, W>;

    fn series(&self) -> Series {
        Series::new(Self::KIND, "current")
    }

    fn window(&self, day: &Window) -> Option<Window> {
        Some(Window::open(*day.to()))
    }

    fn next(
        &self,
        timeline: &Timeline,
        built: Option<&Window>,
        base: Option<Ts<Event>>,
    ) -> Option<Window> {
        let base = base?;

        self.window(&timeline.onto(base, built)?)
            .filter(|window| *window.from() <= base && timeline.reached(window))
    }

    fn base<'b>(
        &self,
        built: &'b [BuildLog],
        table: &TableId,
        window: &Window,
    ) -> Option<&'b BuildLog> {
        Snapshot::shard_before(built, table, window)
    }

    fn target(&self, table: &TableId, window: &Window) -> Result<TablePath, UnbuildableArtifact> {
        self.snapshot.target(table, window)
    }

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        log: &'a L,
        table: TableId,
        window: &'a Window,
        previous: Option<&BuildLog>,
    ) -> Result<Self::Builder<'a, L, W>, UnbuildableArtifact> {
        self.snapshot.builder(log, table, window, previous)
    }
}

#[cfg(test)]
mod tests {
    use lithify_core::schedule::{Schedule, Timeline};
    use lithify_core::ts::Ts;

    use super::*;

    #[test]
    fn an_scd1_is_one_table_named_after_the_table_it_holds_the_current_state_of() {
        let window = Window::new(Ts::try_from(0).unwrap(), Ts::try_from(10).unwrap());

        assert_eq!(
            Scd1::new(Snapshot::current("mysql_shop_scd1"))
                .target(&TableId::new("shop", "people"), &window)
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

    fn scd1() -> Scd1 {
        Scd1::new(Snapshot::current("mysql_shop_scd1"))
    }

    #[test]
    fn an_scd1_holds_everything_after_the_day_it_is_built_onto() {
        let day = Window::new(Ts::try_from(0).unwrap(), Ts::try_from(DAY).unwrap());

        assert_eq!(
            scd1().window(&day),
            Some(Window::open(Ts::try_from(DAY).unwrap()))
        );
    }

    #[test]
    fn an_scd1_is_built_once_onto_each_day_its_snapshot_is_built_for() {
        let built = Window::open(Ts::try_from(DAY).unwrap());

        assert_eq!(
            scd1().next(&until(DAY + 5), None, Some(Ts::try_from(DAY).unwrap())),
            Some(built.clone())
        );
        assert_eq!(
            scd1().next(
                &until(DAY + 5),
                Some(&built),
                Some(Ts::try_from(DAY).unwrap())
            ),
            None
        );
        assert_eq!(
            scd1().next(
                &until(2 * DAY + 5),
                Some(&built),
                Some(Ts::try_from(2 * DAY).unwrap())
            ),
            Some(Window::open(Ts::try_from(2 * DAY).unwrap()))
        );
        assert_eq!(scd1().next(&until(DAY + 5), None, None), None);
    }
}
