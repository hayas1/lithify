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
}
