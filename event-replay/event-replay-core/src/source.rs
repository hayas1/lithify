use std::fmt;

use crate::position::Position;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPath(Vec<String>);

impl ColumnPath {
    pub fn parts(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }
}

impl From<&str> for ColumnPath {
    fn from(path: &str) -> Self {
        Self(path.split('.').map(str::to_owned).collect())
    }
}

impl fmt::Display for ColumnPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.join("."))
    }
}

pub trait ChangeLog {
    type Position: Position;

    fn key(&self) -> ColumnPath;

    fn operation(&self) -> ColumnPath;

    fn before(&self) -> ColumnPath;

    fn after(&self) -> ColumnPath;

    fn recorded_at(&self) -> ColumnPath;

    fn ordering(&self) -> Vec<ColumnPath>;
}
