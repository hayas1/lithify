use std::ops::Range;

use chrono::DateTime;

use crate::ts::{Event, Ts};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window {
    range: Range<Ts<Event>>,
}

impl Window {
    pub fn new(from: Ts<Event>, to: Ts<Event>) -> Self {
        Self { range: from..to }
    }

    pub fn until(to: Ts<Event>) -> Self {
        Self::new(Ts::from(DateTime::UNIX_EPOCH), to)
    }

    pub fn range(&self) -> &Range<Ts<Event>> {
        &self.range
    }

    pub fn from(&self) -> &Ts<Event> {
        &self.range.start
    }

    pub fn to(&self) -> &Ts<Event> {
        &self.range.end
    }
}
