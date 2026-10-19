use chrono::{DateTime, TimeDelta, Utc};

use crate::ts::{Event, Ts};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window<A = Event> {
    from: Ts<A>,
    to: Ts<A>,
}

impl<A> Window<A> {
    // The last instant a BigQuery TIMESTAMP holds, so a window left open is logged as any other.
    const OPEN: i64 = 253_402_300_799_999;

    pub fn new(from: Ts<A>, to: Ts<A>) -> Self {
        Self { from, to }
    }

    pub fn open(from: Ts<A>) -> Self {
        Self::new(
            from,
            Ts::from(
                DateTime::from_timestamp_millis(Self::OPEN).unwrap_or(DateTime::<Utc>::MAX_UTC),
            ),
        )
    }

    pub fn is_open(&self) -> bool {
        self.to.milliseconds_since_epoch() == Self::OPEN
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

    #[test]
    fn a_window_left_open_holds_whatever_comes_after_it_opens() {
        let open: Window = Window::open(Ts::try_from(100).unwrap());
        let closed: Window = Window::new(Ts::try_from(100).unwrap(), Ts::try_from(200).unwrap());

        assert!(open.is_open());
        assert!(!closed.is_open());
        assert_eq!(open.from(), closed.from());
    }
}
