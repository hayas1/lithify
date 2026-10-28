use chrono::NaiveTime;
use chrono_tz::Tz;
use serde::Deserialize;
use serde::de::{self, Deserializer};

use lithify_core::artifact::{Artifact, ArtifactName, UnbuildableArtifact};
use lithify_core::model::TableId;
use lithify_core::sink::{Namespace, TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
use lithify_core::sql::SqlString;
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;
use lithify_pipeline::schedule::{Schedule, Timeline};

use lithify_pipeline::artifact::{ArtifactControl, Placement};
use lithify_pipeline::build_log::{BuildLog, Series};

use crate::scd1::{Overwrite, Scd1Builder};
use crate::shard::Shard;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotConfig {
    #[serde(
        default = "SnapshotConfig::midnight",
        deserialize_with = "SnapshotConfig::time"
    )]
    at: NaiveTime,
    shard: String,
}

impl SnapshotConfig {
    fn midnight() -> NaiveTime {
        NaiveTime::MIN
    }

    pub(crate) fn time<'de, D: Deserializer<'de>>(deserializer: D) -> Result<NaiveTime, D::Error> {
        let written = String::deserialize(deserializer)?;

        NaiveTime::parse_from_str(&written, "%H:%M")
            .or_else(|_| NaiveTime::parse_from_str(&written, "%H:%M:%S"))
            .map_err(de::Error::custom)
    }

    pub fn shard(&self) -> &str {
        &self.shard
    }

    pub fn at(&self) -> NaiveTime {
        self.at
    }

    pub fn schedule(&self, zone: Tz) -> Schedule {
        Schedule::daily(self.at, zone)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    name: ArtifactName,
    namespace: Namespace,
    copies: Vec<Namespace>,
    shard: Shard,
    schedule: Schedule,
}

impl Snapshot {
    pub fn new(namespace: impl Into<Namespace>, shard: Shard) -> Self {
        Self {
            name: ArtifactName::from(Self::KIND),
            namespace: namespace.into(),
            copies: Vec::new(),
            shard,
            schedule: Schedule::default(),
        }
    }

    pub fn scheduled(self, schedule: Schedule) -> Self {
        Self { schedule, ..self }
    }

    pub fn schedule(&self) -> Schedule {
        self.schedule
    }

    pub fn named(self, name: ArtifactName) -> Self {
        Self { name, ..self }
    }

    pub fn copied_into(self, copies: Vec<Namespace>) -> Self {
        Self { copies, ..self }
    }

    pub fn days(name: &ArtifactName) -> Series {
        Series::new(name.clone(), &Schedule::default().to_string())
    }

    pub fn shard_closing<'b>(
        series: &Series,
        built: &'b [BuildLog],
        table: &TableId,
        at: Ts<Event>,
    ) -> Option<&'b BuildLog> {
        built.iter().find(|log| {
            log.advances(series) && log.table_to() == Some(table) && *log.window().to() == at
        })
    }
}

impl Artifact for Snapshot {
    const KIND: &'static str = "snapshot";

    type From = Self;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = Scd1Builder<'a, Self, L, W>;

    fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    // A snapshot is read as the data of a day, which means the day as it ended, so its shard is
    // named after the last instant its window holds rather than the instant it closes, which falls
    // on the next day; each artifact chooses the instant it is named after for what it is read as.
    fn target(&self, table: &TableId, window: &Window) -> Result<TablePath, UnbuildableArtifact> {
        Ok(TablePath::new(
            self.namespace.clone(),
            &self.shard.name(table, window.last()),
        ))
    }

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        log: &'a L,
        table: TableId,
        window: &'a Window,
        base: Option<TablePath>,
    ) -> Self::Builder<'a, L, W> {
        Scd1Builder::new(self, log, table, window.clone(), base)
    }
}

impl ArtifactControl for Snapshot {
    type Config = SnapshotConfig;

    fn declared(
        name: &ArtifactName,
        config: &SnapshotConfig,
        placement: &Placement,
        zone: Tz,
    ) -> Self {
        Self::new(
            placement.namespace().clone(),
            Shard::new(config.shard(), zone),
        )
        .named(name.clone())
        .scheduled(config.schedule(zone))
        .copied_into(placement.copies().to_vec())
    }

    fn series(&self) -> Series {
        Self::days(&self.name)
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
        Self::shard_closing(&self.series(), built, table, *window.from())
    }
}

impl Overwrite for Snapshot {
    fn until(&self, window: &Window) -> Option<Ts<Event>> {
        Some(*window.to())
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
        warehouse.create_table_as(target, select)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use lithify_pipeline::build_log::{Run, Status, TableBuilt};

    use super::*;

    const DAY: i64 = 86_400_000;

    fn built(artifact: &str, schedule: &str, table: &str, (from, to): (i64, i64)) -> BuildLog {
        TableBuilt::new(
            Some(TableId::new("shop", table)),
            Some(TableId::new("shop", table)),
            None,
            Some(TablePath::new("mysql_shop", &format!("{table}_{to}"))),
            Window::new(Ts::try_from(from).unwrap(), Ts::try_from(to).unwrap()),
            Status::Succeeded,
            Ts::from(DateTime::UNIX_EPOCH),
        )
        .logged(&Run::new(
            ulid::Ulid::nil(),
            Ts::from(DateTime::UNIX_EPOCH),
            &artifact.into(),
            schedule,
            "primary",
            "{}",
            "0.1.0",
        ))
    }

    #[test]
    fn a_snapshot_folds_onto_the_shard_its_table_closed_with_as_its_window_opened() {
        let built = [
            built("snapshot", "daily", "pets", (0, 10)),
            built("snapshot", "daily", "people", (0, 5)),
            built("instant", "09:00", "people", (0, 10)),
            built("snapshot", "daily", "people", (0, 10)),
        ];
        assert_eq!(
            Snapshot::new("mysql_shop", Shard::new("%Y%m%d", Tz::UTC)).base(
                &built,
                &TableId::new("shop", "people"),
                &Window::new(Ts::try_from(10).unwrap(), Ts::try_from(20).unwrap())
            ),
            Some(&built[3])
        );
    }

    #[test]
    fn a_snapshot_holds_the_day_it_is_built_for() {
        let timeline = Timeline::new(
            Schedule::default(),
            Ts::try_from(0).unwrap()..Ts::try_from(DAY * 3 + 5).unwrap(),
        );
        let day = Window::new(Ts::try_from(0).unwrap(), Ts::try_from(DAY * 3).unwrap());

        assert_eq!(
            Snapshot::new("mysql_shop", Shard::new("%Y%m%d", Tz::UTC)).window(&day, &timeline),
            Some(day)
        );
    }

    #[test]
    fn a_snapshot_is_declared_with_how_its_shards_are_named() {
        let config: SnapshotConfig = serde_json::from_value(serde_json::json!({
            "shard": "%Y%m%d"
        }))
        .unwrap();

        assert_eq!(config.shard(), "%Y%m%d");
        assert_eq!(config.schedule(Tz::UTC), Schedule::default());
    }

    #[test]
    fn a_snapshot_is_cut_at_the_instant_of_the_day_it_is_declared_with() {
        let config: SnapshotConfig = serde_json::from_value(serde_json::json!({
            "at": "09:00",
            "shard": "%Y%m%d"
        }))
        .unwrap();

        assert_eq!(
            config.schedule(chrono_tz::Asia::Tokyo),
            Schedule::daily(
                NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                chrono_tz::Asia::Tokyo
            )
        );
    }

    #[test]
    fn a_snapshot_declared_with_where_it_is_placed_is_rejected_as_the_warehouse_places_it() {
        assert!(
            serde_json::from_value::<SnapshotConfig>(serde_json::json!({
                "shard": "%Y%m%d",
                "place": ["mysql_shop"]
            }))
            .is_err()
        );
    }

    #[test]
    fn a_snapshot_is_named_after_its_table_and_the_last_day_its_window_holds() {
        let snapshot = Snapshot::new("mysql_shop", Shard::new("%Y%m%d", Tz::UTC));

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
}
