use std::fmt;

use chrono::{DateTime, Utc};

use crate::sink::Warehouse;
use crate::sql::SqlString;

pub trait Position: PartialOrd + Clone + fmt::Debug {
    fn coordinates(&self) -> Vec<Coordinate>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Coordinate {
    Text(String),
    Number(i64),
}

impl Coordinate {
    pub fn sql<W: Warehouse>(&self, warehouse: &W) -> SqlString {
        match self {
            Self::Text(text) => warehouse.literal(text),
            Self::Number(number) => SqlString::from(number.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordedAt(i64);

impl RecordedAt {
    pub fn milliseconds_since_epoch(&self) -> i64 {
        self.0
    }
}

impl From<i64> for RecordedAt {
    fn from(milliseconds_since_epoch: i64) -> Self {
        Self(milliseconds_since_epoch)
    }
}

impl From<DateTime<Utc>> for RecordedAt {
    fn from(at: DateTime<Utc>) -> Self {
        Self(at.timestamp_millis())
    }
}

impl TryFrom<RecordedAt> for DateTime<Utc> {
    type Error = RecordedAt;

    fn try_from(recorded_at: RecordedAt) -> Result<Self, Self::Error> {
        Self::from_timestamp_millis(recorded_at.0).ok_or(recorded_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(rfc3339: &str) -> DateTime<Utc> {
        rfc3339.parse().unwrap()
    }

    #[test]
    fn an_instant_is_recorded_as_the_milliseconds_that_passed_since_the_epoch() {
        assert_eq!(
            RecordedAt::from(at("2026-09-20T00:00:00Z")),
            RecordedAt::from(1789862400000)
        );
    }

    #[test]
    fn an_instant_recorded_in_another_zone_is_the_instant_it_names() {
        assert_eq!(
            RecordedAt::from(at("2026-09-20T09:00:00+09:00")),
            RecordedAt::from(at("2026-09-20T00:00:00Z"))
        );
    }

    #[test]
    fn a_recorded_instant_is_read_back_as_the_instant_it_was() {
        let recorded_at = RecordedAt::from(1789862400000);

        assert_eq!(
            DateTime::try_from(recorded_at).unwrap(),
            at("2026-09-20T00:00:00Z")
        );
    }

    #[test]
    fn a_recording_no_instant_can_hold_is_handed_back() {
        let recorded_at = RecordedAt::from(i64::MAX);

        assert_eq!(DateTime::try_from(recorded_at).unwrap_err(), recorded_at);
    }
}
