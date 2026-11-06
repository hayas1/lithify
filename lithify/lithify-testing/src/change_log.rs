use std::convert::Infallible;

use lithify_build::artifact::UnbuildableArtifact;
use lithify_build::sink::TablePath;
use lithify_build::source::RowChangeLog;
use lithify_schema::listener::OutOfOrder;
use lithify_schema::model::TableId;
use lithify_schema::position::{Coordinate, Position};
use lithify_schema::source::{ChangeLog, ColumnPath};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(pub u64);

impl Position for Offset {}

pub struct Log;

impl ChangeLog for Log {
    type Position = Offset;

    fn envelope(&self) -> Option<ColumnPath> {
        None
    }

    fn event_time(&self) -> ColumnPath {
        ColumnPath::from("source.ts_ms")
    }

    fn processing_time(&self) -> ColumnPath {
        ColumnPath::from("ts_ms")
    }

    fn ordering(&self) -> Vec<ColumnPath> {
        vec![ColumnPath::from("source.offset")]
    }

    fn coordinates(&self, position: &Offset) -> Vec<Coordinate> {
        vec![Coordinate::Number(position.0 as i64)]
    }
}

impl RowChangeLog for Log {
    fn dml_landed_table(&self, table: &TableId) -> TablePath {
        TablePath::new(
            "mysql_shop_cdc",
            &format!("mysql_{}_{}", table.schema(), table.table()),
        )
    }

    fn operation(&self) -> ColumnPath {
        ColumnPath::from("op")
    }

    fn before(&self) -> ColumnPath {
        ColumnPath::from("before")
    }

    fn after(&self) -> ColumnPath {
        ColumnPath::from("after")
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Untold {
    OutOfOrder,
    Unbuildable(UnbuildableArtifact),
}

impl From<Infallible> for Untold {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

impl From<OutOfOrder> for Untold {
    fn from(_: OutOfOrder) -> Self {
        Self::OutOfOrder
    }
}

impl From<UnbuildableArtifact> for Untold {
    fn from(unbuildable: UnbuildableArtifact) -> Self {
        Self::Unbuildable(unbuildable)
    }
}
