use crate::position::RecordedAt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window {
    from: RecordedAt,
    to: RecordedAt,
}

impl Window {
    pub fn new(from: RecordedAt, to: RecordedAt) -> Self {
        Self { from, to }
    }

    pub fn until(to: RecordedAt) -> Self {
        Self::new(RecordedAt::from(0), to)
    }

    pub fn from(&self) -> &RecordedAt {
        &self.from
    }

    pub fn to(&self) -> &RecordedAt {
        &self.to
    }

    pub fn last(&self) -> RecordedAt {
        RecordedAt::from(self.to.milliseconds_since_epoch() - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_window_last_holds_the_instant_before_it_closes() {
        let window = Window::new(RecordedAt::from(100), RecordedAt::from(200));

        assert_eq!(window.last(), RecordedAt::from(199));
    }
}
