use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Days, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use lithify_core::position::RecordedAt;
use lithify_core::window::Window;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ArtifactKind {
    Snapshot,
    History,
}

impl Display for ArtifactKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot => formatter.write_str("snapshot"),
            Self::History => formatter.write_str("history"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Schedule {
    #[default]
    Daily,
}

impl Schedule {
    pub fn windows(self, from: RecordedAt, until: RecordedAt) -> impl Iterator<Item = Window> {
        std::iter::successors(self.window(from), move |window| self.window(*window.to()))
            .take_while(move |window| *window.to() <= until)
    }

    pub fn span(self, from: RecordedAt, until: RecordedAt) -> Option<Window> {
        self.windows(from, until)
            .last()
            .map(|last| Window::new(from, *last.to()))
    }

    pub fn shard(self, since: RecordedAt, date: NaiveDate) -> Option<Window> {
        match date.cmp(&DateTime::<Utc>::try_from(since).ok()?.date_naive()) {
            Ordering::Less => None,
            Ordering::Equal => self.window(since),
            Ordering::Greater => self.window(self.opening(date)?),
        }
    }

    pub fn date(self, shard: &Window) -> Option<NaiveDate> {
        let held = DateTime::<Utc>::try_from(shard.last()).ok()?;

        match self {
            Self::Daily => Some(held.date_naive()),
        }
    }

    fn opening(self, date: NaiveDate) -> Option<RecordedAt> {
        match self {
            Self::Daily => Some(RecordedAt::from(date.and_hms_opt(0, 0, 0)?.and_utc())),
        }
    }

    fn window(self, from: RecordedAt) -> Option<Window> {
        let opened = DateTime::<Utc>::try_from(from).ok()?;
        let closed = match self {
            Self::Daily => opened
                .date_naive()
                .checked_add_days(Days::new(1))?
                .and_hms_opt(0, 0, 0)?
                .and_utc(),
        };

        Some(Window::new(from, RecordedAt::from(closed)))
    }
}

impl Display for Schedule {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Daily => formatter.write_str("daily"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_artifact_is_named_as_the_pipeline_writes_it() {
        assert_eq!(ArtifactKind::Snapshot.to_string(), "snapshot");
        assert_eq!(ArtifactKind::History.to_string(), "history");
    }

    fn at(rfc3339: &str) -> RecordedAt {
        RecordedAt::from(rfc3339.parse::<DateTime<Utc>>().unwrap())
    }

    #[test]
    fn a_daily_schedule_closes_each_window_at_the_next_midnight() {
        assert_eq!(
            Schedule::Daily
                .windows(at("2026-09-20T10:00:00Z"), at("2026-09-22T05:00:00Z"))
                .collect::<Vec<_>>(),
            [
                Window::new(at("2026-09-20T10:00:00Z"), at("2026-09-21T00:00:00Z")),
                Window::new(at("2026-09-21T00:00:00Z"), at("2026-09-22T00:00:00Z")),
            ]
        );
    }

    #[test]
    fn a_daily_schedule_opens_no_window_that_has_not_closed() {
        assert_eq!(
            Schedule::Daily
                .windows(at("2026-09-20T00:00:00Z"), at("2026-09-20T23:59:59Z"))
                .count(),
            0
        );
    }

    #[test]
    fn a_daily_schedule_spans_every_window_closed_since_it_opened_as_one() {
        assert_eq!(
            Schedule::Daily.span(at("1970-01-01T00:00:00Z"), at("2026-09-22T05:00:00Z")),
            Some(Window::new(
                at("1970-01-01T00:00:00Z"),
                at("2026-09-22T00:00:00Z")
            ))
        );
        assert_eq!(
            Schedule::Daily.span(at("2026-09-22T00:00:00Z"), at("2026-09-22T05:00:00Z")),
            None
        );
    }

    fn date(iso: &str) -> NaiveDate {
        iso.parse().unwrap()
    }

    #[test]
    fn a_daily_shard_is_the_window_opening_on_its_date() {
        assert_eq!(
            Schedule::Daily.shard(at("2026-09-20T10:00:00Z"), date("2026-09-21")),
            Some(Window::new(
                at("2026-09-21T00:00:00Z"),
                at("2026-09-22T00:00:00Z")
            ))
        );
    }

    #[test]
    fn the_first_daily_shard_opens_when_the_change_log_does() {
        assert_eq!(
            Schedule::Daily.shard(at("2026-09-20T10:00:00Z"), date("2026-09-20")),
            Some(Window::new(
                at("2026-09-20T10:00:00Z"),
                at("2026-09-21T00:00:00Z")
            ))
        );
    }

    #[test]
    fn a_daily_shard_is_dated_by_the_window_it_builds() {
        for (since, date) in [
            ("2026-09-20T10:00:00Z", "2026-09-20"),
            ("2026-09-20T10:00:00Z", "2026-09-21"),
        ] {
            let window = Schedule::Daily.shard(at(since), self::date(date)).unwrap();

            assert_eq!(Schedule::Daily.date(&window), Some(self::date(date)));
        }
    }

    #[test]
    fn a_daily_shard_spanning_many_days_is_dated_by_the_last_day_it_holds() {
        let window = Window::new(at("1970-01-01T00:00:00Z"), at("2026-09-21T00:00:00Z"));

        assert_eq!(Schedule::Daily.date(&window), Some(date("2026-09-20")));
    }

    #[test]
    fn no_daily_shard_is_dated_before_the_change_log_opens() {
        assert_eq!(
            Schedule::Daily.shard(at("2026-09-20T10:00:00Z"), date("2026-09-19")),
            None
        );
    }

    #[test]
    fn a_schedule_is_named_as_the_pipeline_writes_it() {
        assert_eq!(Schedule::Daily.to_string(), "daily");
    }
}
