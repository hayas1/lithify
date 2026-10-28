use std::fmt;

use crate::sink::WarehouseSql;
use crate::sql::SqlString;

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

impl Coordinate {
    pub fn sql<W: WarehouseSql>(&self, warehouse: &W) -> SqlString {
        match self {
            Self::Text(text) => warehouse.literal(text),
            Self::Number(number) => SqlString::from(number.to_string()),
        }
    }
}
