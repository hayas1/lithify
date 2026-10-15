#[cfg(feature = "duckdb")]
pub mod client;
pub mod config;
#[cfg(feature = "duckdb")]
pub mod from_sql;
#[cfg(feature = "duckdb")]
pub mod store;

use lithify_core::model::ColumnType;
use lithify_core::sink::{TablePath, UnsupportedColumn, Warehouse};
use lithify_core::sql::SqlString;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDbWarehouse {
    change_log: String,
    store: Option<config::Store>,
}

impl DuckDbWarehouse {
    pub fn new(change_log: &str) -> Self {
        Self {
            change_log: change_log.to_owned(),
            store: None,
        }
    }

    pub fn over(change_log: &str, store: config::Store) -> Self {
        Self {
            change_log: change_log.to_owned(),
            store: Some(store),
        }
    }
}

impl Warehouse for DuckDbWarehouse {
    type Config = config::Config;

    fn identifier(&self, name: &str) -> SqlString {
        SqlString::from(format!("\"{name}\""))
    }

    fn table_ref(&self, table: &TablePath) -> SqlString {
        SqlString::from(format!(
            "{}.{}",
            self.namespace_ref(table.namespace()),
            self.identifier(table.name())
        ))
    }

    fn change_log_namespace(&self) -> &str {
        &self.change_log
    }

    fn change_log_ref(&self, topic: &str) -> SqlString {
        let table = self.change_log(topic);

        match &self.store {
            Some(store) => store.scan(&table),
            None => self.table_ref(&table),
        }
    }

    fn column_type(&self, column_type: &ColumnType) -> Result<SqlString, UnsupportedColumn> {
        match column_type {
            ColumnType::Bool => Ok(SqlString::from("BOOLEAN")),
            ColumnType::Int16 => Ok(SqlString::from("SMALLINT")),
            ColumnType::Int32 => Ok(SqlString::from("INTEGER")),
            ColumnType::Int64 => Ok(SqlString::from("BIGINT")),
            ColumnType::Float32 => Ok(SqlString::from("REAL")),
            ColumnType::Float64 => Ok(SqlString::from("DOUBLE")),
            ColumnType::Decimal { precision, scale } => {
                Ok(SqlString::from(format!("DECIMAL({precision}, {scale})")))
            }
            ColumnType::Text => Ok(SqlString::from("VARCHAR")),
            ColumnType::Bytes => Ok(SqlString::from("BLOB")),
            ColumnType::Date => Ok(SqlString::from("DATE")),
            ColumnType::Time => Ok(SqlString::from("TIME")),
            ColumnType::Timestamp => Ok(SqlString::from("TIMESTAMP")),
            ColumnType::ZonedTimestamp => Ok(SqlString::from("TIMESTAMPTZ")),
            ColumnType::Json => Ok(SqlString::from("JSON")),
            ColumnType::Array(element) => {
                Ok(SqlString::from(format!("{}[]", self.column_type(element)?)))
            }
            unsupported @ ColumnType::Unsupported(_) => Err(UnsupportedColumn::new(unsupported)),
        }
    }

    fn unix_millis(&self, expression: &SqlString) -> SqlString {
        SqlString::from(format!("epoch_ms({expression})"))
    }

    fn select_existence(&self, table: &TablePath) -> SqlString {
        SqlString::from(format!(
            "SELECT COUNT(*) AS {} FROM information_schema.tables WHERE table_schema = {} AND table_name = {}",
            self.identifier("count"),
            self.literal(table.namespace()),
            self.literal(table.name())
        ))
    }

    fn timestamp_millis(&self, milliseconds: i64) -> SqlString {
        SqlString::from(format!(
            "(TIMESTAMPTZ '1970-01-01 00:00:00+00' + INTERVAL ({milliseconds}) MILLISECOND)"
        ))
    }

    fn cast_or_null(
        &self,
        expression: &SqlString,
        column_type: &ColumnType,
    ) -> Result<SqlString, UnsupportedColumn> {
        Ok(SqlString::from(format!(
            "TRY_CAST({expression} AS {})",
            self.column_type(column_type)?
        )))
    }

    fn read(
        &self,
        expression: &SqlString,
        column_type: &ColumnType,
    ) -> Result<SqlString, UnsupportedColumn> {
        match column_type {
            ColumnType::Date => Ok(SqlString::from(format!(
                "(DATE '1970-01-01' + ({expression})::INTEGER)"
            ))),
            ColumnType::Time => Ok(SqlString::from(format!(
                "(TIME '00:00:00' + INTERVAL (({expression})::BIGINT) MICROSECOND)"
            ))),
            ColumnType::Timestamp => Ok(SqlString::from(format!(
                "make_timestamp(({expression})::BIGINT)"
            ))),
            other => Ok(SqlString::from(format!(
                "CAST({expression} AS {})",
                self.column_type(other)?
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::duckdb::config::tests::s3;
    use crate::source::debezium::Debezium;
    use crate::source::debezium::mysql::MySql;
    use lithify_core::model::ColumnModel;
    use lithify_core::position::{Coordinate, RecordedAt};
    use lithify_core::window::Window;

    fn read(column_type: ColumnType) -> SqlString {
        DuckDbWarehouse::new("mysql_shop_cdc")
            .read(&SqlString::from("\"after\".\"amount\""), &column_type)
            .unwrap()
    }

    #[test]
    fn a_column_is_filled_with_its_default_or_else_with_null() {
        let warehouse = DuckDbWarehouse::new("mysql_shop_cdc");
        let with_default =
            ColumnModel::new("note", "VARCHAR", ColumnType::Text, true, Some("none"));
        let without = ColumnModel::new("note", "VARCHAR", ColumnType::Text, true, None);

        assert_eq!(
            warehouse.fill(&with_default).unwrap(),
            "TRY_CAST('none' AS VARCHAR)"
        );
        assert_eq!(warehouse.fill(&without).unwrap(), "CAST(NULL AS VARCHAR)");
    }

    #[test]
    fn a_coordinate_is_written_as_a_literal_or_a_number() {
        let warehouse = DuckDbWarehouse::new("mysql_shop_cdc");

        assert_eq!(
            Coordinate::Text("binlog.000002".to_owned()).sql(&warehouse),
            "'binlog.000002'"
        );
        assert_eq!(Coordinate::Number(448).sql(&warehouse), "448");
    }

    #[test]
    fn a_value_that_cannot_take_the_type_of_its_column_is_cast_into_null() {
        assert_eq!(
            DuckDbWarehouse::new("mysql_shop_cdc")
                .cast_or_null(
                    &SqlString::from("'CURRENT_TIMESTAMP'"),
                    &ColumnType::Timestamp
                )
                .unwrap(),
            "TRY_CAST('CURRENT_TIMESTAMP' AS TIMESTAMP)"
        );
    }

    #[test]
    fn a_table_is_qualified_by_the_schema_it_lives_in() {
        assert_eq!(
            DuckDbWarehouse::new("mysql_shop_cdc")
                .table_ref(&TablePath::new("mysql_shop", "customers_20260920")),
            "\"mysql_shop\".\"customers_20260920\""
        );
    }

    #[test]
    fn whole_numbers_keep_the_width_the_source_declared() {
        assert_eq!(
            DuckDbWarehouse::new("mysql_shop_cdc")
                .column_type(&ColumnType::Int16)
                .unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            DuckDbWarehouse::new("mysql_shop_cdc")
                .column_type(&ColumnType::Int32)
                .unwrap(),
            "INTEGER"
        );
        assert_eq!(
            DuckDbWarehouse::new("mysql_shop_cdc")
                .column_type(&ColumnType::Int64)
                .unwrap(),
            "BIGINT"
        );
    }

    #[test]
    fn a_decimal_keeps_the_precision_and_scale_the_source_declared() {
        assert_eq!(
            DuckDbWarehouse::new("mysql_shop_cdc")
                .column_type(&ColumnType::Decimal {
                    precision: 12,
                    scale: 2
                })
                .unwrap(),
            "DECIMAL(12, 2)"
        );
    }

    #[test]
    fn a_decimal_is_read_from_the_string_debezium_publishes() {
        assert_eq!(
            read(ColumnType::Decimal {
                precision: 12,
                scale: 2
            }),
            "CAST(\"after\".\"amount\" AS DECIMAL(12, 2))"
        );
    }

    #[test]
    fn a_date_is_read_from_the_days_debezium_counts_since_the_epoch() {
        assert_eq!(
            read(ColumnType::Date),
            "(DATE '1970-01-01' + (\"after\".\"amount\")::INTEGER)"
        );
    }

    #[test]
    fn a_timestamp_is_read_from_the_microseconds_debezium_counts_since_the_epoch() {
        assert_eq!(
            read(ColumnType::Timestamp),
            "make_timestamp((\"after\".\"amount\")::BIGINT)"
        );
    }

    #[test]
    fn a_zoned_timestamp_is_read_from_the_text_debezium_publishes() {
        assert_eq!(
            read(ColumnType::ZonedTimestamp),
            "CAST(\"after\".\"amount\" AS TIMESTAMPTZ)"
        );
    }

    #[test]
    fn an_array_takes_the_type_of_its_elements() {
        assert_eq!(
            DuckDbWarehouse::new("mysql_shop_cdc")
                .column_type(&ColumnType::Array(Box::new(ColumnType::Text)))
                .unwrap(),
            "VARCHAR[]"
        );
    }

    #[test]
    fn a_type_no_dialect_mapped_has_no_column() {
        let unsupported = ColumnType::Unsupported("tsvector".to_owned());

        assert_eq!(
            DuckDbWarehouse::new("mysql_shop_cdc")
                .column_type(&unsupported)
                .unwrap_err(),
            UnsupportedColumn::new(&unsupported)
        );
    }
    #[test]
    fn the_schema_changes_of_a_window_are_read_from_the_envelope_in_the_order_they_were_recorded_then_logged()
     {
        let log = Debezium::under("value", "mysql", MySql);
        let window = Window::new(RecordedAt::from(0), RecordedAt::from(10));

        assert_eq!(
            DuckDbWarehouse::new("mysql_shop_cdc").select_schema_changes(&log, &window),
            concat!(
                "SELECT \"value\"\n",
                "FROM \"mysql_shop_cdc\".\"mysql\"\n",
                "WHERE \"value\".\"ts_ms\" >= 0 AND \"value\".\"ts_ms\" < 10\n",
                "ORDER BY \"value\".\"ts_ms\", \"value\".\"source\".\"file\", \"value\".\"source\".\"pos\"",
            )
        );
    }

    #[test]
    fn a_change_log_lives_in_the_database_unless_it_was_sunk_into_object_storage() {
        assert_eq!(
            DuckDbWarehouse::new("mysql_shop_cdc").change_log_ref("mysql"),
            "\"mysql_shop_cdc\".\"mysql\""
        );
        assert_eq!(
            DuckDbWarehouse::over("mysql_shop_cdc", s3("http://rustfs:9000"))
                .change_log_ref("mysql"),
            "read_parquet('s3://lithify/mysql_shop_cdc/mysql/*.parquet', union_by_name = true)"
        );
    }

    #[test]
    fn what_was_built_lives_in_the_database_even_when_the_change_log_does_not() {
        assert_eq!(
            DuckDbWarehouse::over("mysql_shop_cdc", s3("http://rustfs:9000"))
                .table_ref(&TablePath::new("mysql_shop", "customers")),
            "\"mysql_shop\".\"customers\""
        );
    }
}
