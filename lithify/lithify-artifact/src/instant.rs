use chrono::{DateTime, NaiveTime, Utc};
use chrono_tz::Tz;
use serde::Deserialize;

use lithify_core::artifact::{Artifact, ArtifactBuilder, ArtifactName, UnbuildableArtifact};
use lithify_core::build::Base;
use lithify_core::listener::ReplayEnd;
use lithify_core::model::TableId;
use lithify_core::sink::{TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
use lithify_core::sql::SqlString;
use lithify_core::ts::Ts;
use lithify_core::window::Window;
use lithify_pipeline::schedule::Timeline;

use lithify_pipeline::artifact::ArtifactControl;
use lithify_pipeline::build_log::{BuildLog, Series};

use crate::scd1::Scd1SelectBuilder;
use crate::shard::Shard;
use crate::snapshot::Snapshot;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstantConfig {
    base: ArtifactName,
    #[serde(deserialize_with = "crate::snapshot::SnapshotConfig::time")]
    at: NaiveTime,
    shard: String,
}

impl InstantConfig {
    pub fn base(&self) -> &ArtifactName {
        &self.base
    }

    pub fn at(&self) -> NaiveTime {
        self.at
    }

    pub fn shard(&self) -> &str {
        &self.shard
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instant {
    name: ArtifactName,
    at: NaiveTime,
    shard: Shard,
}

impl Instant {
    pub fn new(at: NaiveTime, shard: Shard) -> Self {
        Self {
            name: ArtifactName::from(Self::KIND),
            at,
            shard,
        }
    }

    pub fn named(self, name: ArtifactName) -> Self {
        Self { name, ..self }
    }

    pub fn cut(at: NaiveTime) -> String {
        at.format("%H:%M").to_string()
    }
}

impl Artifact for Instant {
    const KIND: &'static str = "instant";

    type From = Snapshot;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = Scd1SelectBuilder<'a, L, W>;

    // An instant is read as the data at the instant it is cut, unlike a snapshot read as a day, so
    // it is named after the instant its window closes.
    fn table_name(&self, table: &TableId, window: &Window) -> String {
        self.shard.name(table, *window.to())
    }

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        log: &'a L,
        window: &'a Window,
        base: Base,
    ) -> Self::Builder<'a, L, W> {
        Scd1SelectBuilder::new(log, window, base, Some(*window.to()))
    }

    fn create<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        builder: &Self::Builder<'a, L, W>,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
        target: &TablePath,
    ) -> Result<Vec<SqlString>, UnbuildableArtifact> {
        Ok(vec![
            warehouse.create_table_as(target, &builder.select(warehouse, end)?),
        ])
    }
}

impl ArtifactControl for Instant {
    type Config = InstantConfig;

    fn declared(name: &ArtifactName, config: &InstantConfig, zone: Tz) -> Self {
        Self::new(config.at(), Shard::new(config.shard(), zone)).named(name.clone())
    }

    fn series(&self) -> Series {
        Series::new(self.name.clone(), &Self::cut(self.at))
    }

    // TODO: a day that daylight saving time shortens or lengthens moves the cut off the time of
    // day it is declared at, since the delay is counted in elapsed time.
    fn window(&self, day: &Window, timeline: &Timeline) -> Option<Window> {
        let cut = DateTime::<Utc>::from(*day.from()).checked_add_signed(timeline.delay(self.at))?;
        let window = Window::new(*day.from(), Ts::from(cut));

        (window.to() <= day.to()).then_some(window)
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

#[cfg(test)]
mod tests {
    use ulid::Ulid;

    use lithify_core::sink::TablePath;
    use lithify_core::ts::Event;
    use lithify_pipeline::build_log::{Run, Status, TableBuilt};
    use lithify_pipeline::schedule::{Schedule, Timeline};

    use super::*;

    fn at(rfc3339: &str) -> Ts<Event> {
        Ts::from(rfc3339.parse::<DateTime<Utc>>().unwrap())
    }

    fn nine_in_tokyo() -> Instant {
        Instant::new(
            NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            Shard::new("%Y%m%d%H", chrono_tz::Asia::Tokyo),
        )
    }

    fn built(artifact: &str, schedule: &str, (from, to): (&str, &str)) -> BuildLog {
        TableBuilt::new(
            Some(TableId::new("shop", "people")),
            Some(TableId::new("shop", "people")),
            None,
            Some(TablePath::new("mysql_shop", "people")),
            Window::new(at(from), at(to)),
            Status::Succeeded,
            Ts::from(DateTime::UNIX_EPOCH),
        )
        .logged(&Run::new(
            Ulid::nil(),
            Ts::from(DateTime::UNIX_EPOCH),
            &artifact.into(),
            schedule,
            "primary",
            "{}",
            "0.1.0",
        ))
    }

    #[test]
    fn an_instant_advances_a_series_of_its_own_for_the_instant_of_the_day_it_is_cut_at() {
        assert_eq!(
            nine_in_tokyo().series(),
            Series::new("instant".into(), "09:00")
        );
    }

    fn cut() -> Window {
        Window::new(at("2026-09-20T15:00:00Z"), at("2026-09-21T00:00:00Z"))
    }

    fn days_in_tokyo() -> Timeline {
        Timeline::new(
            Schedule::daily(NaiveTime::MIN, chrono_tz::Asia::Tokyo),
            at("1970-01-01T00:00:00Z")..at("2026-09-22T00:00:00Z"),
        )
    }

    #[test]
    fn an_instant_cuts_its_day_at_the_instant_it_is_cut_at() {
        let day = Window::new(at("2026-09-20T15:00:00Z"), at("2026-09-21T15:00:00Z"));

        assert_eq!(nine_in_tokyo().window(&day, &days_in_tokyo()), Some(cut()));
    }

    #[test]
    fn an_instant_cut_as_its_day_closes_holds_the_whole_day() {
        let midnight = Instant::new(
            NaiveTime::MIN,
            Shard::new("%Y%m%d%H", chrono_tz::Asia::Tokyo),
        );
        let day = Window::new(at("2026-09-20T15:00:00Z"), at("2026-09-21T15:00:00Z"));

        assert_eq!(midnight.window(&day, &days_in_tokyo()), Some(day));
    }

    #[test]
    fn an_instant_cuts_no_day_that_closes_before_it_is_reached() {
        let day = Window::new(at("2026-09-20T15:00:00Z"), at("2026-09-20T20:00:00Z"));

        assert_eq!(nine_in_tokyo().window(&day, &days_in_tokyo()), None);
    }

    #[test]
    fn an_instant_folds_onto_the_daily_snapshot_closing_as_its_day_opened() {
        let built = [
            built(
                "snapshot",
                "daily",
                ("2026-09-18T15:00:00Z", "2026-09-19T15:00:00Z"),
            ),
            built(
                "snapshot",
                "daily",
                ("2026-09-19T15:00:00Z", "2026-09-20T15:00:00Z"),
            ),
        ];

        assert_eq!(
            nine_in_tokyo().base(&built, &TableId::new("shop", "people"), &cut()),
            Some(&built[1])
        );
    }

    #[test]
    fn a_named_instant_advances_its_series_under_its_name() {
        assert_eq!(
            nine_in_tokyo().named("morning".into()).series(),
            Series::new("morning".into(), "09:00")
        );
    }

    #[test]
    fn an_instant_is_named_after_the_instant_it_is_cut_at() {
        assert_eq!(
            nine_in_tokyo().table_name(&TableId::new("shop", "people"), &cut()),
            "people_2026092109"
        );
    }
}
