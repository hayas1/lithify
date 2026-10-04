#[cfg(feature = "duckdb")]
pub mod client;
pub mod config;
#[cfg(feature = "duckdb")]
pub mod from_sql;
#[cfg(feature = "duckdb")]
pub mod store;

use event_replay_core::model::ColumnType;
use event_replay_core::sink::{TablePath, UnsupportedColumn, Warehouse};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DuckDbWarehouse {
    change_logs: Option<config::Store>,
}

impl DuckDbWarehouse {
    pub fn over(store: config::Store) -> Self {
        Self {
            change_logs: Some(store),
        }
    }
}

impl Warehouse for DuckDbWarehouse {
    type Config = config::Store;

    fn identifier(&self, name: &str) -> String {
        format!("\"{name}\"")
    }

    fn table_ref(&self, table: &TablePath) -> String {
        format!(
            "{}.{}",
            self.namespace_ref(table.namespace()),
            self.identifier(table.name())
        )
    }

    fn change_log_ref(&self, table: &TablePath) -> String {
        match &self.change_logs {
            Some(store) => store.scan(table),
            None => self.table_ref(table),
        }
    }

    fn column_type(&self, column_type: &ColumnType) -> Result<String, UnsupportedColumn> {
        match column_type {
            ColumnType::Bool => Ok("BOOLEAN".to_owned()),
            ColumnType::Int16 => Ok("SMALLINT".to_owned()),
            ColumnType::Int32 => Ok("INTEGER".to_owned()),
            ColumnType::Int64 => Ok("BIGINT".to_owned()),
            ColumnType::Float32 => Ok("REAL".to_owned()),
            ColumnType::Float64 => Ok("DOUBLE".to_owned()),
            ColumnType::Decimal { precision, scale } => {
                Ok(format!("DECIMAL({precision}, {scale})"))
            }
            ColumnType::Text => Ok("VARCHAR".to_owned()),
            ColumnType::Bytes => Ok("BLOB".to_owned()),
            ColumnType::Date => Ok("DATE".to_owned()),
            ColumnType::Time => Ok("TIME".to_owned()),
            ColumnType::Timestamp => Ok("TIMESTAMP".to_owned()),
            ColumnType::ZonedTimestamp => Ok("TIMESTAMPTZ".to_owned()),
            ColumnType::Json => Ok("JSON".to_owned()),
            ColumnType::Array(element) => Ok(format!("{}[]", self.column_type(element)?)),
            unsupported @ ColumnType::Unsupported(_) => Err(UnsupportedColumn::new(unsupported)),
        }
    }

    fn read(
        &self,
        expression: &str,
        column_type: &ColumnType,
    ) -> Result<String, UnsupportedColumn> {
        match column_type {
            ColumnType::Date => Ok(format!("(DATE '1970-01-01' + ({expression})::INTEGER)")),
            ColumnType::Time => Ok(format!(
                "(TIME '00:00:00' + INTERVAL (({expression})::BIGINT) MICROSECOND)"
            )),
            ColumnType::Timestamp => Ok(format!("make_timestamp(({expression})::BIGINT)")),
            other => Ok(format!(
                "CAST({expression} AS {})",
                self.column_type(other)?
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::duckdb::config::tests::s3;
    use crate::source::debezium::Debezium;
    use crate::source::debezium::mysql::MySql;
    use event_replay_core::position::RecordedAt;
    use event_replay_core::window::Window;

    fn read(column_type: ColumnType) -> String {
        DuckDbWarehouse::default()
            .read("\"after\".\"amount\"", &column_type)
            .unwrap()
    }

    #[test]
    fn a_table_is_qualified_by_the_schema_it_lives_in() {
        assert_eq!(
            DuckDbWarehouse::default()
                .table_ref(&TablePath::new("mysql_shop", "customers_20260920")),
            "\"mysql_shop\".\"customers_20260920\""
        );
    }

    #[test]
    fn whole_numbers_keep_the_width_the_source_declared() {
        assert_eq!(
            DuckDbWarehouse::default()
                .column_type(&ColumnType::Int16)
                .unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            DuckDbWarehouse::default()
                .column_type(&ColumnType::Int32)
                .unwrap(),
            "INTEGER"
        );
        assert_eq!(
            DuckDbWarehouse::default()
                .column_type(&ColumnType::Int64)
                .unwrap(),
            "BIGINT"
        );
    }

    #[test]
    fn a_decimal_keeps_the_precision_and_scale_the_source_declared() {
        assert_eq!(
            DuckDbWarehouse::default()
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
            DuckDbWarehouse::default()
                .column_type(&ColumnType::Array(Box::new(ColumnType::Text)))
                .unwrap(),
            "VARCHAR[]"
        );
    }

    #[test]
    fn a_type_no_dialect_mapped_has_no_column() {
        let unsupported = ColumnType::Unsupported("tsvector".to_owned());

        assert_eq!(
            DuckDbWarehouse::default()
                .column_type(&unsupported)
                .unwrap_err(),
            UnsupportedColumn::new(&unsupported)
        );
    }
    #[test]
    fn the_schema_changes_of_a_window_are_read_from_the_envelope_in_source_order() {
        let log = Debezium::under("value", MySql);
        let window = Window::new(RecordedAt::from(0), RecordedAt::from(10));

        assert_eq!(
            DuckDbWarehouse::default().select_schema_changes(
                &log,
                &TablePath::new("mysql_shop_cdc", "mysql"),
                &window
            ),
            concat!(
                "SELECT \"value\"\n",
                "FROM \"mysql_shop_cdc\".\"mysql\"\n",
                "WHERE \"value\".\"ts_ms\" >= 0 AND \"value\".\"ts_ms\" < 10\n",
                "ORDER BY \"value\".\"source\".\"file\", \"value\".\"source\".\"pos\"",
            )
        );
    }

    #[test]
    fn a_change_log_lives_in_the_database_unless_it_was_sunk_into_object_storage() {
        let table = TablePath::new("mysql_shop_cdc", "mysql");

        assert_eq!(
            DuckDbWarehouse::default().change_log_ref(&table),
            "\"mysql_shop_cdc\".\"mysql\""
        );
        assert_eq!(
            DuckDbWarehouse::over(s3("http://minio:9000")).change_log_ref(&table),
            "read_parquet('s3://event-replay/mysql_shop_cdc/mysql/*.parquet')"
        );
    }

    #[test]
    fn what_was_built_lives_in_the_database_even_when_the_change_log_does_not() {
        assert_eq!(
            DuckDbWarehouse::over(s3("http://minio:9000"))
                .table_ref(&TablePath::new("mysql_shop", "customers")),
            "\"mysql_shop\".\"customers\""
        );
    }
}
