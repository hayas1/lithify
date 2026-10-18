use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Days, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;

use crate::ts::{Event, Ts};
use crate::window::Window;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Schedule {
    Daily(Cut),
}

impl Default for Schedule {
    fn default() -> Self {
        Self::daily(NaiveTime::MIN, Tz::UTC)
    }
}

impl Schedule {
    pub fn daily(at: NaiveTime, zone: Tz) -> Self {
        Self::Daily(Cut { at, zone })
    }

    pub fn windows(self, from: Ts<Event>, until: Ts<Event>) -> impl Iterator<Item = Window> {
        std::iter::successors(self.window(from), move |window| self.window(*window.to()))
            .take_while(move |window| *window.to() <= until)
    }

    pub fn span(self, from: Ts<Event>, until: Ts<Event>) -> Option<Window> {
        self.windows(from, until)
            .last()
            .map(|last| Window::new(from, *last.to()))
    }

    pub fn shard(self, since: Ts<Event>, date: NaiveDate) -> Option<Window> {
        let first = self.window(since)?;

        match date.cmp(&self.date(&first)?) {
            Ordering::Less => None,
            Ordering::Equal => Some(first),
            Ordering::Greater => match self {
                Self::Daily(cut) => {
                    let closed = cut.closing(date)?;
                    let opened = cut.on(cut.local(closed).date_naive().pred_opt()?)?;

                    Some(Window::new(opened, closed))
                }
            },
        }
    }

    pub fn date(self, shard: &Window) -> Option<NaiveDate> {
        match self {
            Self::Daily(cut) => Some(cut.local(shard.last()).date_naive()),
        }
    }

    pub fn window(self, from: Ts<Event>) -> Option<Window> {
        match self {
            Self::Daily(cut) => Some(Window::new(from, cut.after(from)?)),
        }
    }
}

impl Display for Schedule {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Daily(_) => formatter.write_str("daily"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cut {
    at: NaiveTime,
    zone: Tz,
}

impl Cut {
    fn local(self, instant: Ts<Event>) -> DateTime<Tz> {
        DateTime::<Utc>::from(instant).with_timezone(&self.zone)
    }

    fn on(self, date: NaiveDate) -> Option<Ts<Event>> {
        let cut = self
            .zone
            .from_local_datetime(&date.and_time(self.at))
            .earliest()?;

        Some(Ts::from(cut.with_timezone(&Utc)))
    }

    fn after(self, instant: Ts<Event>) -> Option<Ts<Event>> {
        let date = self.local(instant).date_naive();

        match self.on(date)? {
            cut if cut > instant => Some(cut),
            _ => self.on(date.checked_add_days(Days::new(1))?),
        }
    }

    fn closing(self, date: NaiveDate) -> Option<Ts<Event>> {
        let midnight = self
            .zone
            .from_local_datetime(&date.and_time(NaiveTime::MIN))
            .earliest()?;

        self.after(Ts::from(midnight.with_timezone(&Utc)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(rfc3339: &str) -> Ts<Event> {
        Ts::from(rfc3339.parse::<DateTime<Utc>>().unwrap())
    }

    #[test]
    fn a_daily_schedule_closes_each_window_at_the_next_midnight() {
        assert_eq!(
            Schedule::default()
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
            Schedule::default()
                .windows(at("2026-09-20T00:00:00Z"), at("2026-09-20T23:59:59Z"))
                .count(),
            0
        );
    }

    #[test]
    fn a_daily_schedule_spans_every_window_closed_since_it_opened_as_one() {
        assert_eq!(
            Schedule::default().span(at("1970-01-01T00:00:00Z"), at("2026-09-22T05:00:00Z")),
            Some(Window::new(
                at("1970-01-01T00:00:00Z"),
                at("2026-09-22T00:00:00Z")
            ))
        );
        assert_eq!(
            Schedule::default().span(at("2026-09-22T00:00:00Z"), at("2026-09-22T05:00:00Z")),
            None
        );
    }

    fn date(iso: &str) -> NaiveDate {
        iso.parse().unwrap()
    }

    #[test]
    fn a_daily_shard_is_the_window_opening_on_its_date() {
        assert_eq!(
            Schedule::default().shard(at("2026-09-20T10:00:00Z"), date("2026-09-21")),
            Some(Window::new(
                at("2026-09-21T00:00:00Z"),
                at("2026-09-22T00:00:00Z")
            ))
        );
    }

    #[test]
    fn the_first_daily_shard_opens_when_the_change_log_does() {
        assert_eq!(
            Schedule::default().shard(at("2026-09-20T10:00:00Z"), date("2026-09-20")),
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
            let window = Schedule::default()
                .shard(at(since), self::date(date))
                .unwrap();

            assert_eq!(Schedule::default().date(&window), Some(self::date(date)));
        }
    }

    #[test]
    fn a_daily_shard_spanning_many_days_is_dated_by_the_last_day_it_holds() {
        let window = Window::new(at("1970-01-01T00:00:00Z"), at("2026-09-21T00:00:00Z"));

        assert_eq!(Schedule::default().date(&window), Some(date("2026-09-20")));
    }

    #[test]
    fn no_daily_shard_is_dated_before_the_change_log_opens() {
        assert_eq!(
            Schedule::default().shard(at("2026-09-20T10:00:00Z"), date("2026-09-19")),
            None
        );
    }

    fn tokyo(at: &str) -> Schedule {
        Schedule::daily(at.parse().unwrap(), chrono_tz::Asia::Tokyo)
    }

    #[test]
    fn a_daily_schedule_closes_each_window_at_the_instant_of_the_day_it_is_cut_at_in_its_time_zone()
    {
        assert_eq!(
            tokyo("00:00:00")
                .windows(at("2026-09-20T10:00:00Z"), at("2026-09-22T05:00:00Z"))
                .collect::<Vec<_>>(),
            [
                Window::new(at("2026-09-20T10:00:00Z"), at("2026-09-20T15:00:00Z")),
                Window::new(at("2026-09-20T15:00:00Z"), at("2026-09-21T15:00:00Z")),
            ]
        );
        assert_eq!(
            tokyo("09:00:00")
                .windows(at("2026-09-20T10:00:00Z"), at("2026-09-22T05:00:00Z"))
                .collect::<Vec<_>>(),
            [
                Window::new(at("2026-09-20T10:00:00Z"), at("2026-09-21T00:00:00Z")),
                Window::new(at("2026-09-21T00:00:00Z"), at("2026-09-22T00:00:00Z")),
            ]
        );
    }

    #[test]
    fn a_daily_shard_is_dated_by_the_last_day_it_holds_in_its_time_zone() {
        let window = tokyo("00:00:00")
            .shard(at("2026-09-01T00:00:00Z"), date("2026-09-21"))
            .unwrap();

        assert_eq!(
            window,
            Window::new(at("2026-09-20T15:00:00Z"), at("2026-09-21T15:00:00Z"))
        );
        assert_eq!(tokyo("00:00:00").date(&window), Some(date("2026-09-21")));
    }

    #[test]
    fn a_daily_shard_cut_after_midnight_closes_on_its_date() {
        let window = tokyo("09:00:00")
            .shard(at("2026-09-01T00:00:00Z"), date("2026-09-21"))
            .unwrap();

        assert_eq!(
            window,
            Window::new(at("2026-09-20T00:00:00Z"), at("2026-09-21T00:00:00Z"))
        );
        assert_eq!(tokyo("09:00:00").date(&window), Some(date("2026-09-21")));
    }

    #[test]
    fn a_schedule_is_named_as_the_pipeline_writes_it() {
        assert_eq!(Schedule::default().to_string(), "daily");
    }
}
