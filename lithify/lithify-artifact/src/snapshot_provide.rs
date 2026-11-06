use std::marker::PhantomData;

use chrono_tz::Tz;
use serde::Deserialize;

use lithify_core::artifact::{Artifact, ArtifactBuilder, ArtifactName, UnbuildableArtifact};
use lithify_core::build::Base;
use lithify_core::listener::{ReplayEnd, SchemaChangeListener};
use lithify_core::model::TableId;
use lithify_core::sink::{TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
use lithify_core::sql::SqlString;
use lithify_core::window::Window;
use lithify_pipeline::schedule::{Schedule, Timeline};

use lithify_pipeline::artifact::ArtifactControl;
use lithify_pipeline::build_log::{BuildLog, Series};

use crate::shard::Shard;
use crate::snapshot::Snapshot;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotProvideConfig {
    base: ArtifactName,
    shard: String,
}

impl SnapshotProvideConfig {
    pub fn base(&self) -> &ArtifactName {
        &self.base
    }

    pub fn shard(&self) -> &str {
        &self.shard
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotProvide {
    name: ArtifactName,
    shard: Shard,
}

impl SnapshotProvide {
    pub fn new(shard: Shard) -> Self {
        Self {
            name: ArtifactName::from(Self::KIND),
            shard,
        }
    }

    pub fn named(self, name: ArtifactName) -> Self {
        Self { name, ..self }
    }
}

impl Artifact for SnapshotProvide {
    const KIND: &'static str = "snapshot_provide";

    type From = Snapshot;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = SnapshotProvideBuilder<W>;

    // A shard provided is the snapshot of the day closing as its window opens, so it is named after
    // the last instant of that day, as the snapshot is.
    fn table_name(&self, table: &TableId, window: &Window) -> String {
        self.shard.name(table, window.last())
    }

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        _: &'a L,
        _: &'a Window,
        base: Base,
    ) -> SnapshotProvideBuilder<W> {
        SnapshotProvideBuilder {
            base,
            warehouse: PhantomData,
        }
    }

    fn create<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        builder: &SnapshotProvideBuilder<W>,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
        target: &TablePath,
    ) -> Result<Vec<SqlString>, UnbuildableArtifact> {
        let select = <SnapshotProvideBuilder<W> as ArtifactBuilder<L::Position>>::select(
            builder, warehouse, end,
        )?;

        Ok(vec![warehouse.create_table_as(target, &select)])
    }
}

impl ArtifactControl for SnapshotProvide {
    type Config = SnapshotProvideConfig;

    fn declared(name: &ArtifactName, config: &SnapshotProvideConfig, zone: Tz) -> Self {
        Self::new(Shard::new(config.shard(), zone)).named(name.clone())
    }

    fn series(&self) -> Series {
        Series::new(self.name.clone(), &Schedule::default().to_string())
    }

    // A window opens and closes as the day of the snapshot it provides closes, so it takes in no
    // change and holds the shard just as it was built.
    fn window(&self, day: &Window, _: &Timeline) -> Option<Window> {
        Some(Window::new(*day.to(), *day.to()))
    }

    fn base<'b>(
        &self,
        built: &'b [BuildLog],
        table: &TableId,
        window: &Window,
    ) -> Option<&'b BuildLog> {
        Snapshot::shard_closing(built, table, *window.from())
    }
}

pub struct SnapshotProvideBuilder<W> {
    base: Base,
    warehouse: PhantomData<fn(&W)>,
}

impl<P, W> SchemaChangeListener<P> for SnapshotProvideBuilder<W> {
    type Context = W;
    type Error = UnbuildableArtifact;
}

impl<P, W: Warehouse> ArtifactBuilder<P> for SnapshotProvideBuilder<W> {
    fn select(
        &self,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
    ) -> Result<SqlString, UnbuildableArtifact> {
        match self.base.as_built() {
            Some(shard) => Ok(SqlString::from(format!(
                "SELECT *\nFROM {}",
                warehouse.table_ref(shard)
            ))),
            None => Err(UnbuildableArtifact::WithoutBase(end.table().clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use lithify_core::model::TableModel;
    use lithify_core::ts::{Event, Ts};
    use lithify_testing::{Engine, Log};

    use super::*;

    fn at(rfc3339: &str) -> Ts<Event> {
        Ts::from(rfc3339.parse::<DateTime<Utc>>().unwrap())
    }

    fn provided() -> SnapshotProvide {
        SnapshotProvide::new(Shard::new("%Y%m%d", Tz::UTC))
    }

    fn closing() -> Window {
        Window::new(at("2026-09-21T00:00:00Z"), at("2026-09-21T00:00:00Z"))
    }

    fn people() -> TableModel {
        TableModel::new(
            TableId::new("shop", "people"),
            vec![],
            Vec::new().into_iter().collect(),
        )
    }

    #[test]
    fn a_snapshot_provided_opens_and_closes_its_window_as_the_day_closes() {
        let day = Window::new(at("2026-09-20T00:00:00Z"), at("2026-09-21T00:00:00Z"));

        assert_eq!(
            provided().window(
                &day,
                &Timeline::new(Schedule::default(), *day.from()..*day.to())
            ),
            Some(closing())
        );
    }

    #[test]
    fn a_snapshot_provided_is_named_after_the_day_its_snapshot_holds() {
        assert_eq!(
            provided().table_name(&TableId::new("shop", "people"), &closing()),
            "people_20260920"
        );
    }

    #[test]
    fn a_snapshot_provided_is_created_from_the_shard_closing_as_its_window_opens() {
        let window = closing();
        let artifact = provided();
        let shard = TablePath::new("mysql_shop", "people_20260920");
        let builder = artifact.builder::<Log, Engine>(&Log, &window, Base::Built(shard));

        assert_eq!(
            artifact
                .create::<Log, Engine>(
                    &builder,
                    &Engine::default(),
                    &ReplayEnd::standing(people(), None),
                    &TablePath::new("mysql_shop_provided", "people_20260920"),
                )
                .unwrap(),
            [SqlString::from(concat!(
                "CREATE OR REPLACE TABLE \"mysql_shop_provided\".\"people_20260920\" AS\n",
                "SELECT *\n",
                "FROM \"mysql_shop\".\"people_20260920\""
            ))]
        );
    }

    #[test]
    fn a_snapshot_provided_without_its_shard_is_refused() {
        let window = closing();
        let artifact = provided();
        let builder = artifact.builder::<Log, Engine>(&Log, &window, Base::Missing);

        assert_eq!(
            artifact.create::<Log, Engine>(
                &builder,
                &Engine::default(),
                &ReplayEnd::standing(people(), None),
                &TablePath::new("mysql_shop_provided", "people_20260920"),
            ),
            Err(UnbuildableArtifact::WithoutBase(TableId::new(
                "shop", "people"
            )))
        );
    }
}
