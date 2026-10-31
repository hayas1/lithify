use std::fmt::{self, Display, Formatter};
use std::ops::Range;

use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, TimeZone, Utc};
use chrono_tz::Tz;

use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schedule {
    period: Period,
    zone: Tz,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Period {
    Daily(NaiveTime),
}

impl Default for Schedule {
    fn default() -> Self {
        Self::daily(NaiveTime::MIN, Tz::UTC)
    }
}

impl Schedule {
    pub fn daily(at: NaiveTime, zone: Tz) -> Self {
        Self {
            period: Period::Daily(at),
            zone,
        }
    }

    pub fn local(self, instant: Ts<Event>) -> DateTime<Tz> {
        DateTime::<Utc>::from(instant).with_timezone(&self.zone)
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

    pub fn through(self, since: Ts<Event>, date: NaiveDate) -> Option<Ts<Event>> {
        let end = self
            .zone
            .from_local_datetime(&date.succ_opt()?.and_time(NaiveTime::MIN))
            .earliest()?;

        self.span(since, Ts::from(end.with_timezone(&Utc)))
            .map(|span| *span.to())
    }

    pub fn delay(self, at: NaiveTime) -> TimeDelta {
        self.period.delay(at)
    }

    pub fn following(self, instant: Ts<Event>) -> Option<Window> {
        self.window(Ts::from(
            self.period
                .at_or_after(self.local(instant))?
                .with_timezone(&Utc),
        ))
    }

    pub fn window(self, from: Ts<Event>) -> Option<Window> {
        let to = self.period.after(self.local(from))?;

        Some(Window::new(from, Ts::from(to.with_timezone(&Utc))))
    }
}

impl Display for Schedule {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.period.fmt(formatter)
    }
}

impl Period {
    fn delay(self, at: NaiveTime) -> TimeDelta {
        match self {
            Self::Daily(opening) => match at - opening {
                delay if delay > TimeDelta::zero() => delay,
                delay => delay + TimeDelta::days(1),
            },
        }
    }

    fn after(self, instant: DateTime<Tz>) -> Option<DateTime<Tz>> {
        self.first(instant, |boundary| boundary > instant)
    }

    fn at_or_after(self, instant: DateTime<Tz>) -> Option<DateTime<Tz>> {
        self.first(instant, |boundary| boundary >= instant)
    }

    fn first(
        self,
        instant: DateTime<Tz>,
        reached: impl Fn(DateTime<Tz>) -> bool,
    ) -> Option<DateTime<Tz>> {
        match self {
            Self::Daily(at) => {
                let on = |date: NaiveDate| {
                    instant
                        .timezone()
                        .from_local_datetime(&date.and_time(at))
                        .earliest()
                };
                let today = instant.date_naive();

                match on(today)? {
                    boundary if reached(boundary) => Some(boundary),
                    _ => on(today.succ_opt()?),
                }
            }
        }
    }
}

impl Display for Period {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Daily(_) => formatter.write_str("daily"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timeline {
    schedule: Schedule,
    range: Range<Ts<Event>>,
}

impl Timeline {
    pub fn new(schedule: Schedule, range: Range<Ts<Event>>) -> Self {
        Self { schedule, range }
    }

    pub fn range(&self) -> &Range<Ts<Event>> {
        &self.range
    }

    pub fn up_to(self, until: Ts<Event>) -> Self {
        Self {
            range: self.range.start..until,
            ..self
        }
    }

    pub fn through(&self, date: NaiveDate) -> Option<Ts<Event>> {
        self.schedule.through(self.range.start, date)
    }

    pub fn days(&self, from: Ts<Event>) -> impl Iterator<Item = Window> {
        let schedule = self.schedule;
        let until = self.range.end;
        let first = match from == self.range.start {
            true => schedule.span(from, until),
            false => schedule.following(from),
        };

        std::iter::successors(first, move |day| schedule.window(*day.to()))
            .take_while(move |day| *day.from() < until)
    }

    pub fn delay(&self, at: NaiveTime) -> TimeDelta {
        self.schedule.delay(at)
    }

    pub fn rest(&self, from: Ts<Event>) -> Option<Window> {
        (from < self.range.end).then(|| Window::new(from, self.range.end))
    }

    pub fn reached(&self, window: &Window) -> bool {
        *window.to() <= self.range.end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(rfc3339: &str) -> Ts<Event> {
        Ts::from(rfc3339.parse::<DateTime<Utc>>().unwrap())
    }

    fn timeline() -> Timeline {
        Timeline::new(
            Schedule::default(),
            at("2026-09-18T10:00:00Z")..at("2026-09-22T05:00:00Z"),
        )
    }

    fn day(from: &str, to: &str) -> Window {
        Window::new(at(from), at(to))
    }

    #[test]
    fn a_timeline_begins_its_days_with_one_spanning_every_day_closed_since_it_began() {
        assert_eq!(
            timeline()
                .days(at("2026-09-18T10:00:00Z"))
                .collect::<Vec<_>>(),
            [
                day("2026-09-18T10:00:00Z", "2026-09-22T00:00:00Z"),
                day("2026-09-22T00:00:00Z", "2026-09-23T00:00:00Z"),
            ]
        );
    }

    #[test]
    fn a_timeline_steps_day_by_day_from_the_first_day_opening_at_or_after_an_instant() {
        assert_eq!(
            timeline()
                .days(at("2026-09-20T09:00:00Z"))
                .collect::<Vec<_>>(),
            [
                day("2026-09-21T00:00:00Z", "2026-09-22T00:00:00Z"),
                day("2026-09-22T00:00:00Z", "2026-09-23T00:00:00Z"),
            ]
        );
        assert_eq!(
            timeline()
                .days(at("2026-09-21T00:00:00Z"))
                .collect::<Vec<_>>(),
            [
                day("2026-09-21T00:00:00Z", "2026-09-22T00:00:00Z"),
                day("2026-09-22T00:00:00Z", "2026-09-23T00:00:00Z"),
            ]
        );
    }

    #[test]
    fn a_timeline_holds_the_rest_of_itself_from_an_instant() {
        assert_eq!(
            timeline().rest(at("2026-09-21T00:00:00Z")),
            Some(day("2026-09-21T00:00:00Z", "2026-09-22T05:00:00Z"))
        );
        assert_eq!(timeline().rest(at("2026-09-22T05:00:00Z")), None);
    }

    #[test]
    fn a_timeline_reaches_a_window_closing_by_its_end() {
        assert!(timeline().reached(&day("2026-09-21T00:00:00Z", "2026-09-22T05:00:00Z")));
        assert!(!timeline().reached(&day("2026-09-21T00:00:00Z", "2026-09-22T06:00:00Z")));
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
    fn a_daily_schedule_has_closed_through_a_date_at_the_last_window_closing_by_its_end() {
        assert_eq!(
            Schedule::default().through(at("2026-09-20T10:00:00Z"), date("2026-09-21")),
            Some(at("2026-09-22T00:00:00Z"))
        );
        assert_eq!(
            Schedule::default().through(at("2026-09-20T10:00:00Z"), date("2026-09-20")),
            Some(at("2026-09-21T00:00:00Z"))
        );
    }

    #[test]
    fn a_daily_schedule_has_closed_through_no_date_ending_before_its_first_window_closes() {
        assert_eq!(
            Schedule::default().through(at("2026-09-20T10:00:00Z"), date("2026-09-19")),
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
    fn a_daily_schedule_has_closed_through_a_date_by_the_end_of_that_date_in_its_time_zone() {
        assert_eq!(
            tokyo("00:00:00").through(at("2026-09-01T00:00:00Z"), date("2026-09-21")),
            Some(at("2026-09-21T15:00:00Z"))
        );
        assert_eq!(
            tokyo("09:00:00").through(at("2026-09-01T00:00:00Z"), date("2026-09-21")),
            Some(at("2026-09-21T00:00:00Z"))
        );
    }

    #[test]
    fn a_daily_schedule_delays_a_time_of_day_from_the_instant_each_window_opens() {
        assert_eq!(
            tokyo("09:00:00").delay("15:30:00".parse().unwrap()),
            TimeDelta::minutes(6 * 60 + 30)
        );
        assert_eq!(
            tokyo("09:00:00").delay("06:00:00".parse().unwrap()),
            TimeDelta::hours(21)
        );
        assert_eq!(
            tokyo("09:00:00").delay("09:00:00".parse().unwrap()),
            TimeDelta::days(1)
        );
    }

    #[test]
    fn a_timeline_delays_a_time_of_day_from_the_instant_each_of_its_days_opens() {
        let timeline = Timeline::new(tokyo("09:00:00"), timeline().range().clone());

        assert_eq!(
            timeline.delay("15:30:00".parse().unwrap()),
            TimeDelta::minutes(6 * 60 + 30)
        );
    }

    #[test]
    fn a_schedule_reads_an_instant_in_its_time_zone() {
        assert_eq!(
            tokyo("09:00:00")
                .local(at("2026-09-21T00:00:00Z"))
                .to_rfc3339(),
            "2026-09-21T09:00:00+09:00"
        );
    }

    #[test]
    fn a_schedule_is_named_as_the_pipeline_writes_it() {
        assert_eq!(Schedule::default().to_string(), "daily");
    }
}
