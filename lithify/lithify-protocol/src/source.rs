pub mod debezium;

use lithify_core::event::{MalformedEvent, SchemaChange};
use lithify_core::model::TableId;
use lithify_core::position::{Coordinate, Position};
use lithify_core::source::ColumnPath;

pub trait CdcFormat {
    type Position: Position;
    type Record;

    fn ddl_topic(&self) -> String;

    fn dml_topic(&self, table: &TableId) -> String;

    fn event_time(&self) -> ColumnPath;

    fn processing_time(&self) -> ColumnPath;

    fn ordering(&self) -> Vec<ColumnPath>;

    fn coordinates(&self, position: &Self::Position) -> Vec<Coordinate>;

    fn operation(&self) -> ColumnPath;

    fn before(&self) -> ColumnPath;

    fn after(&self) -> ColumnPath;

    fn schema_change(
        &self,
        record: Self::Record,
    ) -> Result<SchemaChange<Self::Position>, MalformedEvent>;
}
