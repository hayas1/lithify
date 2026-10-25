use chrono::NaiveTime;
use chrono_tz::Tz;
use serde::Deserialize;
use serde::de::{self, Deserializer};

use lithify_core::artifact::{Artifact, UnbuildableArtifact};
use lithify_core::build_log::{BuildLog, Series};
use lithify_core::model::TableId;
use lithify_core::schedule::{Schedule, Timeline};
use lithify_core::sink::{Namespace, TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
use lithify_core::sql::SqlString;
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

use crate::scd1::{Overwrite, Scd1Builder};
use crate::shard::Shard;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "Config::midnight", deserialize_with = "Config::time")]
    at: NaiveTime,
    shard: String,
}

impl Config {
    fn midnight() -> NaiveTime {
        NaiveTime::MIN
    }

    fn time<'de, D: Deserializer<'de>>(deserializer: D) -> Result<NaiveTime, D::Error> {
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
    namespace: Namespace,
    copies: Vec<Namespace>,
    shard: Shard,
}

impl Snapshot {
    pub fn new(namespace: impl Into<Namespace>, shard: Shard) -> Self {
        Self {
            namespace: namespace.into(),
            copies: Vec::new(),
            shard,
        }
    }

    pub fn copied_into(self, copies: Vec<Namespace>) -> Self {
        Self { copies, ..self }
    }

    pub fn days() -> Series {
        Series::new(Self::KIND, &Schedule::default().to_string())
    }

    pub fn shard_closing<'b>(
        built: &'b [BuildLog],
        table: &TableId,
        at: Ts<Event>,
    ) -> Option<&'b BuildLog> {
        built.iter().find(|log| {
            log.advances(&Self::days()) && log.table_to() == Some(table) && *log.window().to() == at
        })
    }
}

impl Artifact for Snapshot {
    const KIND: &'static str = "snapshot";

    type From = Self;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = Scd1Builder<'a, Self, L, W>;

    fn series(&self) -> Series {
        Self::days()
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
        Self::shard_closing(built, table, *window.from())
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

    use lithify_core::build_log::{Run, Status, TableBuilt};

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
            artifact,
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
        let config: Config = serde_json::from_value(serde_json::json!({
            "shard": "%Y%m%d"
        }))
        .unwrap();

        assert_eq!(config.shard(), "%Y%m%d");
        assert_eq!(config.schedule(Tz::UTC), Schedule::default());
    }

    #[test]
    fn a_snapshot_is_cut_at_the_instant_of_the_day_it_is_declared_with() {
        let config: Config = serde_json::from_value(serde_json::json!({
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
            serde_json::from_value::<Config>(serde_json::json!({
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
