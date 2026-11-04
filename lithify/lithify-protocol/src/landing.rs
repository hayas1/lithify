use lithify_core::event::{MalformedEvent, SchemaChange};
use lithify_core::model::TableId;
use lithify_core::position::Coordinate;
use lithify_core::sink::{ChangeLogSql, Namespace, TablePath, Warehouse};
use lithify_core::source::{ChangeLog, ChangeLogs, ColumnPath, RowChangeLog, SchemaChangeLog};

use crate::source::CdcFormat;

pub trait Landing<C> {
    type Warehouse: Warehouse;

    fn envelope(&self) -> Option<ColumnPath>;

    fn landed_table(&self, change_log: &Namespace, topic: &str) -> TablePath;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Landed<C, L> {
    cdc: C,
    landing: L,
    change_log: Namespace,
}

impl<C, L: Landing<C>> Landed<C, L> {
    pub fn new(cdc: C, landing: L, warehouse: &L::Warehouse) -> Self {
        Self {
            cdc,
            landing,
            change_log: warehouse.change_log_namespace().clone(),
        }
    }

    pub fn cdc(&self) -> &C {
        &self.cdc
    }

    fn path(&self, path: ColumnPath) -> ColumnPath {
        match self.landing.envelope() {
            Some(envelope) => ColumnPath::from(format!("{envelope}.{path}").as_str()),
            None => path,
        }
    }
}

impl<C: CdcFormat, L: Landing<C>> ChangeLog for Landed<C, L> {
    type Position = C::Position;

    fn envelope(&self) -> Option<ColumnPath> {
        self.landing.envelope()
    }

    fn event_time(&self) -> ColumnPath {
        self.path(self.cdc.event_time())
    }

    fn processing_time(&self) -> ColumnPath {
        self.path(self.cdc.processing_time())
    }

    fn ordering(&self) -> Vec<ColumnPath> {
        self.cdc
            .ordering()
            .into_iter()
            .map(|path| self.path(path))
            .collect()
    }

    fn coordinates(&self, position: &Self::Position) -> Vec<Coordinate> {
        self.cdc.coordinates(position)
    }
}

impl<C: CdcFormat, L: Landing<C>> ChangeLogs for Landed<C, L> {
    type Position = C::Position;
    type Schema = Self;
    type Row = Self;

    fn schema_changes(&self) -> &Self::Schema {
        self
    }

    fn row_changes(&self) -> &Self::Row {
        self
    }
}

impl<C: CdcFormat, L: Landing<C>> SchemaChangeLog for Landed<C, L> {
    type Record = C::Record;

    fn ddl_landed_table(&self) -> TablePath {
        self.landing
            .landed_table(&self.change_log, &self.cdc.ddl_topic())
    }

    fn schema_change(
        &self,
        record: Self::Record,
    ) -> Result<SchemaChange<Self::Position>, MalformedEvent> {
        self.cdc.schema_change(record)
    }
}

impl<C: CdcFormat, L: Landing<C>> RowChangeLog for Landed<C, L> {
    fn dml_landed_table(&self, table: &TableId) -> TablePath {
        self.landing
            .landed_table(&self.change_log, &self.cdc.dml_topic(table))
    }

    fn operation(&self) -> ColumnPath {
        self.path(self.cdc.operation())
    }

    fn before(&self) -> ColumnPath {
        self.path(self.cdc.before())
    }

    fn after(&self) -> ColumnPath {
        self.path(self.cdc.after())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::debezium::Debezium;
    use crate::source::debezium::mysql::MySql;
    use crate::{BigQuerySink, BigQueryWarehouse, DuckDbWarehouse, S3Sink};

    fn landed_in_s3() -> Landed<Debezium<MySql>, S3Sink> {
        Landed::new(
            Debezium::new("mysql", MySql).with_ddl_topic("mysql.schema-changes"),
            S3Sink,
            &DuckDbWarehouse::new("mysql_shop_cdc"),
        )
    }

    fn landed_in_bigquery() -> Landed<Debezium<MySql>, BigQuerySink> {
        Landed::new(
            Debezium::new("mysql", MySql).with_ddl_topic("mysql.schema-changes"),
            BigQuerySink,
            &BigQueryWarehouse::new("local-project", "mysql_shop_cdc"),
        )
    }

    #[test]
    fn a_change_log_lands_in_the_namespace_of_the_warehouse_in_the_table_its_sink_names_after_the_topic()
     {
        assert_eq!(
            landed_in_s3().ddl_landed_table(),
            TablePath::new("mysql_shop_cdc", "mysql.schema-changes")
        );
        assert_eq!(
            landed_in_s3().dml_landed_table(&TableId::new("shop", "order-lines")),
            TablePath::new("mysql_shop_cdc", "mysql.shop.order-lines")
        );
        assert_eq!(
            landed_in_bigquery().ddl_landed_table(),
            TablePath::new("mysql_shop_cdc", "mysql_schema_changes")
        );
        assert_eq!(
            landed_in_bigquery().dml_landed_table(&TableId::new("shop", "order-lines")),
            TablePath::new("mysql_shop_cdc", "mysql_shop_order_lines")
        );
    }

    #[test]
    fn a_change_log_is_read_under_the_envelope_its_sink_wrapped_each_record_in() {
        assert_eq!(
            landed_in_s3().event_time(),
            ColumnPath::from("value.source.ts_ms")
        );
        assert_eq!(landed_in_s3().operation(), ColumnPath::from("value.op"));
        assert_eq!(
            landed_in_bigquery().event_time(),
            ColumnPath::from("source.ts_ms")
        );
        assert_eq!(landed_in_bigquery().operation(), ColumnPath::from("op"));
    }
}
