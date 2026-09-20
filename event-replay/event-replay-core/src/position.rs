use std::fmt;

pub trait Position: PartialOrd + Clone + fmt::Debug {}

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
