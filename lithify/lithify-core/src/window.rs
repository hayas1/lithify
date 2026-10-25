use std::ops::Range;

use chrono::{DateTime, TimeDelta, Utc};

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

    pub fn last(&self) -> Ts<Event> {
        Ts::from(DateTime::<Utc>::from(self.range.end) - TimeDelta::milliseconds(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_window_holds_last_the_millisecond_before_it_closes() {
        let window = Window::new(Ts::try_from(100).unwrap(), Ts::try_from(200).unwrap());

        assert_eq!(window.last(), Ts::try_from(199).unwrap());
    }
}
