use chrono::{DateTime, TimeDelta};

use crate::ts::{Event, Ts};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window<A = Event> {
    from: Ts<A>,
    to: Ts<A>,
}

impl<A> Window<A> {
    pub fn new(from: Ts<A>, to: Ts<A>) -> Self {
        Self { from, to }
    }

    pub fn until(to: Ts<A>) -> Self {
        Self::new(Ts::from(DateTime::UNIX_EPOCH), to)
    }

    pub fn from(&self) -> &Ts<A> {
        &self.from
    }

    pub fn to(&self) -> &Ts<A> {
        &self.to
    }

    pub fn last(&self) -> Ts<A>
    where
        A: Copy,
    {
        Ts::from(DateTime::from(self.to) - TimeDelta::milliseconds(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_window_last_holds_the_instant_before_it_closes() {
        let window: Window = Window::new(Ts::try_from(100).unwrap(), Ts::try_from(200).unwrap());

        assert_eq!(window.last(), Ts::try_from(199).unwrap());
    }
}
