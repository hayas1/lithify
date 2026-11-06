pub mod debezium;

use lithify_schema::event::{MalformedEvent, SchemaChange};
use lithify_schema::model::TableId;
use lithify_schema::position::{Coordinate, Position};
use lithify_schema::source::ColumnPath;

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
