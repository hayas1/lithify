use std::fmt;
use std::marker::PhantomData;

use chrono::{DateTime, DurationRound, TimeDelta, Utc};

use crate::position::Coordinate;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Event {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Processing {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Replay {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ts<A> {
    at: DateTime<Utc>,
    axis: PhantomData<A>,
}

impl<A> Ts<A> {
    pub fn milliseconds_since_epoch(&self) -> i64 {
        self.at.timestamp_millis()
    }
}

impl Ts<Replay> {
    pub fn now() -> Self {
        Self::from(Utc::now())
    }

    pub fn as_event(self) -> Ts<Event> {
        Ts::from(self.at)
    }
}

impl<A> fmt::Display for Ts<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.at.to_rfc3339())
    }
}

impl<A> From<DateTime<Utc>> for Ts<A> {
    fn from(at: DateTime<Utc>) -> Self {
        Self {
            at: at.duration_trunc(TimeDelta::milliseconds(1)).unwrap_or(at),
            axis: PhantomData,
        }
    }
}

impl<A> From<Ts<A>> for DateTime<Utc> {
    fn from(ts: Ts<A>) -> Self {
        ts.at
    }
}

impl<A> TryFrom<i64> for Ts<A> {
    type Error = i64;

    fn try_from(milliseconds_since_epoch: i64) -> Result<Self, Self::Error> {
        DateTime::from_timestamp_millis(milliseconds_since_epoch)
            .map(Self::from)
            .ok_or(milliseconds_since_epoch)
    }
}

impl<A> From<Ts<A>> for Coordinate {
    fn from(ts: Ts<A>) -> Self {
        Self::Number(ts.milliseconds_since_epoch())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(rfc3339: &str) -> DateTime<Utc> {
        rfc3339.parse().unwrap()
    }

    #[test]
    fn a_ts_is_the_milliseconds_that_passed_since_the_epoch() {
        assert_eq!(
            Ts::<Event>::from(at("2026-09-20T00:00:00Z")).milliseconds_since_epoch(),
            1789862400000
        );
        assert_eq!(
            Ts::<Event>::try_from(1789862400000).unwrap(),
            Ts::from(at("2026-09-20T00:00:00Z"))
        );
    }

    #[test]
    fn a_ts_given_in_another_zone_is_the_instant_it_names() {
        assert_eq!(
            Ts::<Processing>::from(at("2026-09-20T09:00:00+09:00")),
            Ts::from(at("2026-09-20T00:00:00Z"))
        );
    }

    #[test]
    fn a_ts_holds_no_more_than_the_milliseconds_a_change_log_carries() {
        assert_eq!(
            DateTime::from(Ts::<Event>::from(at("2026-09-20T00:00:00.123456789Z"))),
            at("2026-09-20T00:00:00.123Z")
        );
    }

    #[test]
    fn a_ts_reads_as_the_instant_it_is() {
        assert_eq!(
            Ts::<Event>::try_from(1789862400000).unwrap().to_string(),
            "2026-09-20T00:00:00+00:00"
        );
    }

    #[test]
    fn milliseconds_no_instant_can_hold_are_handed_back() {
        assert_eq!(Ts::<Event>::try_from(i64::MAX).unwrap_err(), i64::MAX);
    }

    #[test]
    fn an_instant_of_a_replay_is_read_as_an_event_at_the_same_instant() {
        assert_eq!(
            Ts::<Replay>::from(at("2026-09-20T05:00:00Z")).as_event(),
            Ts::<Event>::from(at("2026-09-20T05:00:00Z"))
        );
    }

    #[test]
    fn a_ts_is_compared_on_a_change_log_as_its_milliseconds() {
        assert_eq!(
            Coordinate::from(Ts::<Event>::try_from(1789862400000).unwrap()),
            Coordinate::Number(1789862400000)
        );
    }
}
