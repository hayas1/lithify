use chrono::{DateTime, TimeDelta};

use crate::ts::{Event, Ts};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window {
    from: Ts<Event>,
    to: Ts<Event>,
}

impl Window {
    pub fn new(from: Ts<Event>, to: Ts<Event>) -> Self {
        Self { from, to }
    }

    pub fn until(to: Ts<Event>) -> Self {
        Self::new(Ts::from(DateTime::UNIX_EPOCH), to)
    }

    pub fn from(&self) -> &Ts<Event> {
        &self.from
    }

    pub fn to(&self) -> &Ts<Event> {
        &self.to
    }

    pub fn last(&self) -> Ts<Event> {
        Ts::from(DateTime::from(self.to) - TimeDelta::milliseconds(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_window_last_holds_the_instant_before_it_closes() {
        let window = Window::new(Ts::try_from(100).unwrap(), Ts::try_from(200).unwrap());

        assert_eq!(window.last(), Ts::try_from(199).unwrap());
    }
}
