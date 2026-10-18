use std::fmt;

use crate::sink::WarehouseSql;
use crate::sql::SqlString;

pub trait Position: PartialOrd + Clone + fmt::Debug {}

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
