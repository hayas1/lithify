use chrono::{DateTime, NaiveTime, TimeDelta, Utc};

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
use crate::snapshot::Snapshot;

pub type Config = Vec<crate::snapshot::Config>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instant {
    at: NaiveTime,
    delay: TimeDelta,
    namespace: Namespace,
    copies: Vec<Namespace>,
    shard: Shard,
}

impl Instant {
    pub fn new(
        at: NaiveTime,
        days: Schedule,
        namespace: impl Into<Namespace>,
        shard: Shard,
    ) -> Self {
        Self {
            at,
            delay: days.delay(at),
            namespace: namespace.into(),
            copies: Vec::new(),
            shard,
        }
    }

    pub fn copied_into(self, copies: Vec<Namespace>) -> Self {
        Self { copies, ..self }
    }

    pub fn cut(at: NaiveTime) -> String {
        at.format("%H:%M").to_string()
    }
}

impl Artifact for Instant {
    const KIND: &'static str = "instant";

    type From = Snapshot;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = Scd1Builder<'a, Self, L, W>;

    fn series(&self) -> Series {
        Series::new(Self::KIND, &Self::cut(self.at))
    }

    // TODO: a day that daylight saving time shortens or lengthens moves the cut off the time of
    // day it is declared at, since the delay is counted in elapsed time.
    fn window(&self, day: &Window, _: &Timeline) -> Option<Window> {
        let cut = DateTime::<Utc>::from(*day.from()).checked_add_signed(self.delay)?;
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

    // An instant is read as the data at the instant it is cut, unlike a snapshot read as a day, so
    // it is named after the instant its window closes.
    fn target(&self, table: &TableId, window: &Window) -> Result<TablePath, UnbuildableArtifact> {
        Ok(TablePath::new(
            self.namespace.clone(),
            &self.shard.name(table, *window.to()),
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

impl Overwrite for Instant {
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
    use ulid::Ulid;

    use lithify_core::build_log::{Run, Status, TableBuilt};
    use lithify_core::schedule::Timeline;

    use super::*;

    fn at(rfc3339: &str) -> Ts<Event> {
        Ts::from(rfc3339.parse::<DateTime<Utc>>().unwrap())
    }

    fn nine_in_tokyo() -> Instant {
        Instant::new(
            NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            Schedule::daily(NaiveTime::MIN, chrono_tz::Asia::Tokyo),
            "mysql_shop_0900",
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
            artifact,
            schedule,
            "primary",
            "{}",
            "0.1.0",
        ))
    }

    #[test]
    fn an_instant_advances_a_series_of_its_own_for_the_instant_of_the_day_it_is_cut_at() {
        assert_eq!(nine_in_tokyo().series(), Series::new("instant", "09:00"));
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
            Schedule::daily(NaiveTime::MIN, chrono_tz::Asia::Tokyo),
            "mysql_shop_0000",
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
                "instant",
                "09:00",
                ("2026-09-19T15:00:00Z", "2026-09-20T15:00:00Z"),
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
    fn an_instant_is_named_after_the_instant_it_is_cut_at() {
        assert_eq!(
            nine_in_tokyo()
                .target(&TableId::new("shop", "people"), &cut())
                .unwrap(),
            TablePath::new("mysql_shop_0900", "people_2026092109")
        );
    }
}
