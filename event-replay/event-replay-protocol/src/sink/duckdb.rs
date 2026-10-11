#[cfg(feature = "duckdb")]
pub mod client;
pub mod config;
#[cfg(feature = "duckdb")]
pub mod from_sql;
#[cfg(feature = "duckdb")]
pub mod store;

use event_replay_core::model::ColumnType;
use event_replay_core::sink::{TablePath, UnsupportedColumn, Warehouse};
use event_replay_core::sql::Sql;

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

    fn identifier(&self, name: &str) -> Sql {
        Sql::from(format!("\"{name}\""))
    }

    fn table_ref(&self, table: &TablePath) -> Sql {
        Sql::from(format!(
            "{}.{}",
            self.namespace_ref(table.namespace()),
            self.identifier(table.name())
        ))
    }

    fn change_log_ref(&self, table: &TablePath) -> Sql {
        match &self.change_logs {
            Some(store) => store.scan(table),
            None => self.table_ref(table),
        }
    }

    fn column_type(&self, column_type: &ColumnType) -> Result<Sql, UnsupportedColumn> {
        match column_type {
            ColumnType::Bool => Ok(Sql::from("BOOLEAN")),
            ColumnType::Int16 => Ok(Sql::from("SMALLINT")),
            ColumnType::Int32 => Ok(Sql::from("INTEGER")),
            ColumnType::Int64 => Ok(Sql::from("BIGINT")),
            ColumnType::Float32 => Ok(Sql::from("REAL")),
            ColumnType::Float64 => Ok(Sql::from("DOUBLE")),
            ColumnType::Decimal { precision, scale } => {
                Ok(Sql::from(format!("DECIMAL({precision}, {scale})")))
            }
            ColumnType::Text => Ok(Sql::from("VARCHAR")),
            ColumnType::Bytes => Ok(Sql::from("BLOB")),
            ColumnType::Date => Ok(Sql::from("DATE")),
            ColumnType::Time => Ok(Sql::from("TIME")),
            ColumnType::Timestamp => Ok(Sql::from("TIMESTAMP")),
            ColumnType::ZonedTimestamp => Ok(Sql::from("TIMESTAMPTZ")),
            ColumnType::Json => Ok(Sql::from("JSON")),
            ColumnType::Array(element) => {
                Ok(Sql::from(format!("{}[]", self.column_type(element)?)))
            }
            unsupported @ ColumnType::Unsupported(_) => Err(UnsupportedColumn::new(unsupported)),
        }
    }

    fn read(&self, expression: &Sql, column_type: &ColumnType) -> Result<Sql, UnsupportedColumn> {
        match column_type {
            ColumnType::Date => Ok(Sql::from(format!(
                "(DATE '1970-01-01' + ({expression})::INTEGER)"
            ))),
            ColumnType::Time => Ok(Sql::from(format!(
                "(TIME '00:00:00' + INTERVAL (({expression})::BIGINT) MICROSECOND)"
            ))),
            ColumnType::Timestamp => {
                Ok(Sql::from(format!("make_timestamp(({expression})::BIGINT)")))
            }
            other => Ok(Sql::from(format!(
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
    use event_replay_core::position::RecordedAt;
    use event_replay_core::window::Window;

    fn read(column_type: ColumnType) -> Sql {
        DuckDbWarehouse::default()
            .read(&Sql::from("\"after\".\"amount\""), &column_type)
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
            "read_parquet('s3://event-replay/mysql_shop_cdc/mysql/*.parquet', union_by_name = true)"
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
