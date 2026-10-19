use chrono::NaiveTime;
use chrono_tz::Tz;

use lithify_core::artifact::{Artifact, UnbuildableArtifact};
use lithify_core::build_log::{BuildLog, Series};
use lithify_core::model::TableId;
use lithify_core::schedule::Schedule;
use lithify_core::schedule::Timeline;
use lithify_core::sink::{TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

use crate::snapshot::{Snapshot, SnapshotBuilder};

pub type Config = Vec<crate::snapshot::Config>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instant {
    at: NaiveTime,
    cut: Schedule,
    snapshot: Snapshot,
}

impl Instant {
    pub fn new(at: NaiveTime, zone: Tz, snapshot: Snapshot) -> Self {
        Self {
            at,
            cut: Schedule::daily(at, zone),
            snapshot,
        }
    }

    pub fn cut(at: NaiveTime) -> String {
        at.format("%H:%M").to_string()
    }
}

impl Artifact for Instant {
    const KIND: &'static str = "instant";

    type From = Snapshot;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = SnapshotBuilder<'a, L, W>;

    fn series(&self) -> Series {
        Series::new(Self::KIND, &Self::cut(self.at))
    }

    fn window(&self, day: &Window) -> Option<Window> {
        self.cut
            .windows(*day.from(), *day.to())
            .next()
            .map(|cut| Window::new(*day.from(), *cut.to()))
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
    use chrono::{DateTime, Utc};
    use ulid::Ulid;

    use lithify_core::build_log::{Outcome, Run, Status};
    use lithify_core::schedule::Timeline;
    use lithify_core::ts::{Event, Ts};

    use super::*;

    fn at(rfc3339: &str) -> Ts<Event> {
        Ts::from(rfc3339.parse::<DateTime<Utc>>().unwrap())
    }

    fn nine_in_tokyo() -> Instant {
        Instant::new(
            NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            chrono_tz::Asia::Tokyo,
            Snapshot::new("mysql_shop_0900", "%Y%m%d%H").named_in(chrono_tz::Asia::Tokyo),
        )
    }

    fn day() -> Window {
        Window::new(at("2026-09-20T15:00:00Z"), at("2026-09-21T15:00:00Z"))
    }

    fn built(artifact: &str, schedule: &str, (from, to): (&str, &str)) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::nil(),
                DateTime::UNIX_EPOCH,
                artifact,
                schedule,
                "primary",
                "{}",
                "0.1.0",
            ),
            Some(TableId::new("shop", "people")),
            Some(TableId::new("shop", "people")),
            None,
            Some(TablePath::new("mysql_shop", "people")),
            Window::new(at(from), at(to)),
            Outcome::new(Status::Succeeded, DateTime::UNIX_EPOCH),
        )
    }

    #[test]
    fn an_instant_advances_a_series_of_its_own_for_the_instant_of_the_day_it_is_cut_at() {
        assert_eq!(nine_in_tokyo().series(), Series::new("instant", "09:00"));
    }

    #[test]
    fn an_instant_holds_a_daily_window_up_to_the_instant_it_is_cut_at() {
        assert_eq!(
            nine_in_tokyo().window(&day()),
            Some(Window::new(
                at("2026-09-20T15:00:00Z"),
                at("2026-09-21T00:00:00Z")
            ))
        );
    }

    #[test]
    fn an_instant_cut_after_a_daily_window_closes_holds_nothing_of_it() {
        let early = Window::new(at("2026-09-20T15:00:00Z"), at("2026-09-20T23:00:00Z"));

        assert_eq!(nine_in_tokyo().window(&early), None);
    }

    fn days_in_tokyo(until: &str) -> Timeline {
        Timeline::new(
            Schedule::daily(NaiveTime::MIN, chrono_tz::Asia::Tokyo),
            at("1970-01-01T00:00:00Z"),
            at(until),
        )
    }

    #[test]
    fn an_instant_never_cut_is_cut_first_in_the_last_day_its_snapshot_was_built_for() {
        let before = Window::new(at("2026-09-19T15:00:00Z"), at("2026-09-20T15:00:00Z"));

        assert_eq!(
            nine_in_tokyo().next(
                &days_in_tokyo("2026-09-21T01:00:00Z"),
                None,
                Some(at("2026-09-20T15:00:00Z"))
            ),
            nine_in_tokyo().window(&before)
        );
    }

    #[test]
    fn an_instant_is_cut_next_in_the_day_after_the_one_it_was_last_cut_in() {
        let cut = Window::new(at("2026-09-19T15:00:00Z"), at("2026-09-20T00:00:00Z"));

        assert_eq!(
            nine_in_tokyo().next(
                &days_in_tokyo("2026-09-21T01:00:00Z"),
                Some(&cut),
                Some(at("2026-09-21T15:00:00Z"))
            ),
            nine_in_tokyo().window(&day())
        );
    }

    #[test]
    fn an_instant_waits_for_its_snapshot_to_stand_at_the_day_it_is_cut_in() {
        let cut = Window::new(at("2026-09-19T15:00:00Z"), at("2026-09-20T00:00:00Z"));

        assert_eq!(
            nine_in_tokyo().next(
                &days_in_tokyo("2026-09-21T01:00:00Z"),
                Some(&cut),
                Some(at("2026-09-19T15:00:00Z"))
            ),
            None
        );
        assert_eq!(
            nine_in_tokyo().next(&days_in_tokyo("2026-09-21T01:00:00Z"), None, None),
            None
        );
    }

    #[test]
    fn an_instant_not_yet_reached_is_not_cut() {
        let cut = Window::new(at("2026-09-19T15:00:00Z"), at("2026-09-20T00:00:00Z"));

        assert_eq!(
            nine_in_tokyo().next(
                &days_in_tokyo("2026-09-20T23:00:00Z"),
                Some(&cut),
                Some(at("2026-09-20T15:00:00Z"))
            ),
            None
        );
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
            nine_in_tokyo().base(
                &built,
                &TableId::new("shop", "people"),
                &nine_in_tokyo().window(&day()).unwrap()
            ),
            Some(&built[1])
        );
    }

    #[test]
    fn an_instant_is_named_after_the_instant_it_is_cut_at() {
        assert_eq!(
            nine_in_tokyo()
                .target(
                    &TableId::new("shop", "people"),
                    &nine_in_tokyo().window(&day()).unwrap()
                )
                .unwrap(),
            TablePath::new("mysql_shop_0900", "people_2026092108")
        );
    }
}
