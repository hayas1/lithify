use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Days, Utc};
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
    fn a_schedule_is_named_as_the_pipeline_writes_it() {
        assert_eq!(Schedule::Daily.to_string(), "daily");
    }
}
