use std::fmt;

pub trait Position: PartialOrd + Clone + fmt::Debug {}

// The unit tests here cannot take the Offset of lithify-testing, which links a build of
// lithify-core of its own whose Position is another trait.
#[cfg(test)]
impl Position for u64 {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Coordinate {
    Text(String),
    Number(i64),
}
