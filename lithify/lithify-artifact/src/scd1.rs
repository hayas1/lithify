use serde::Deserialize;

use lithify_core::artifact::{Artifact, UnbuildableArtifact};
use lithify_core::build_log::{BuildLog, Series};
use lithify_core::model::TableId;
use lithify_core::sink::{TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
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

    fn windows(&self, daily: &Window) -> Vec<Window> {
        vec![daily.clone()]
    }

    fn open_ended(&self) -> bool {
        true
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

    #[test]
    fn an_scd1_leaves_the_change_log_after_its_window_opens_open() {
        assert!(Scd1::new(Snapshot::current("mysql_shop_scd1")).open_ended());
        assert!(!Snapshot::new("mysql_shop", "%Y%m%d").open_ended());
    }
}
