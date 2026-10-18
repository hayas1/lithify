#[cfg(feature = "bigquery")]
pub mod client;
pub mod config;
#[cfg(feature = "bigquery")]
pub mod from_sql;

use lithify_core::model::ColumnType;
use lithify_core::sink::{ChangeLogSql, TablePath, UnsupportedColumn, WarehouseSql};
use lithify_core::sql::SqlString;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQueryWarehouse {
    project: String,
    change_log: String,
}

impl BigQueryWarehouse {
    pub fn new(project: &str, change_log: &str) -> Self {
        Self {
            project: project.to_owned(),
            change_log: change_log.to_owned(),
        }
    }

    pub fn project(&self) -> &str {
        &self.project
    }
}
impl WarehouseSql for BigQueryWarehouse {
    fn identifier(&self, name: &str) -> SqlString {
        SqlString::from(format!("`{name}`"))
    }

    fn namespace_ref(&self, namespace: &str) -> SqlString {
        SqlString::from(format!(
            "{}.{}",
            self.identifier(&self.project),
            self.identifier(namespace)
        ))
    }

    fn table_ref(&self, table: &TablePath) -> SqlString {
        SqlString::from(format!(
            "{}.{}",
            self.namespace_ref(table.namespace()),
            self.identifier(table.name())
        ))
    }

    fn literal(&self, text: &str) -> SqlString {
        SqlString::from(format!(
            "'{}'",
            text.replace('\\', "\\\\").replace('\'', "\\'")
        ))
    }

    fn column_type(&self, column_type: &ColumnType) -> Result<SqlString, UnsupportedColumn> {
        match column_type {
            ColumnType::Bool => Ok(SqlString::from("BOOL")),
            ColumnType::Int16 | ColumnType::Int32 | ColumnType::Int64 => {
                Ok(SqlString::from("INT64"))
            }
            ColumnType::Float32 | ColumnType::Float64 => Ok(SqlString::from("FLOAT64")),
            ColumnType::Decimal { precision, scale } => {
                Ok(SqlString::from(format!("NUMERIC({precision}, {scale})")))
            }
            ColumnType::Text => Ok(SqlString::from("STRING")),
            ColumnType::Bytes => Ok(SqlString::from("BYTES")),
            ColumnType::Date => Ok(SqlString::from("DATE")),
            ColumnType::Time => Ok(SqlString::from("TIME")),
            ColumnType::Timestamp => Ok(SqlString::from("DATETIME")),
            ColumnType::ZonedTimestamp => Ok(SqlString::from("TIMESTAMP")),
            ColumnType::Json => Ok(SqlString::from("JSON")),
            ColumnType::Array(element) => Ok(SqlString::from(format!(
                "ARRAY<{}>",
                self.column_type(element)?
            ))),
            unsupported @ ColumnType::Unsupported(_) => Err(UnsupportedColumn::new(unsupported)),
        }
    }

    fn unix_millis(&self, expression: &SqlString) -> SqlString {
        SqlString::from(format!("UNIX_MILLIS({expression})"))
    }

    fn select_existence(&self, table: &TablePath) -> SqlString {
        SqlString::from(format!(
            "SELECT COUNT(*) AS {} FROM {}.{}.INFORMATION_SCHEMA.TABLES WHERE table_name = {}",
            self.identifier("count"),
            self.identifier(&self.project),
            self.identifier(table.namespace()),
            self.literal(table.name())
        ))
    }

    fn timestamp_millis(&self, milliseconds: i64) -> SqlString {
        SqlString::from(format!("TIMESTAMP_MILLIS({milliseconds})"))
    }

    fn cast_or_null(
        &self,
        expression: &SqlString,
        column_type: &ColumnType,
    ) -> Result<SqlString, UnsupportedColumn> {
        Ok(SqlString::from(format!(
            "SAFE_CAST({expression} AS {})",
            self.column_type(column_type)?
        )))
    }

    fn read(
        &self,
        expression: &SqlString,
        column_type: &ColumnType,
    ) -> Result<SqlString, UnsupportedColumn> {
        match column_type {
            ColumnType::Json => Ok(SqlString::from(format!("PARSE_JSON({expression})"))),
            other => Ok(SqlString::from(format!(
                "CAST({expression} AS {})",
                self.column_type(other)?
            ))),
        }
    }
}

impl ChangeLogSql for BigQueryWarehouse {
    fn change_log_namespace(&self) -> &str {
        &self.change_log
    }

    fn change_log(&self, topic: &str) -> TablePath {
        TablePath::new(
            self.change_log_namespace(),
            &topic
                .chars()
                .map(|character| match character.is_ascii_alphanumeric() {
                    true => character,
                    false => '_',
                })
                .collect::<String>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::debezium::Debezium;
    use crate::source::debezium::mysql::MySql;

    use chrono::DateTime;
    use lithify_core::build_log::{BuildLog, BuildLogQuery, Latest, Outcome, Run, Status};
    use lithify_core::model::{ColumnType, TableId};
    use lithify_core::position::RecordedAt;
    use lithify_core::sink::{ChangeLogSql, TablePath, UnsupportedColumn};
    use lithify_core::window::Window;
    use ulid::Ulid;

    fn bigquery() -> BigQueryWarehouse {
        BigQueryWarehouse::new("local-project", "mysql_shop_cdc")
    }

    fn read(column_type: ColumnType) -> SqlString {
        bigquery()
            .read(&SqlString::from("`after`.`amount`"), &column_type)
            .unwrap()
    }

    #[test]
    fn a_value_that_cannot_take_the_type_of_its_column_is_cast_into_null() {
        assert_eq!(
            bigquery()
                .cast_or_null(
                    &SqlString::from("'CURRENT_TIMESTAMP'"),
                    &ColumnType::Timestamp
                )
                .unwrap(),
            "SAFE_CAST('CURRENT_TIMESTAMP' AS DATETIME)"
        );
    }

    #[test]
    fn a_build_log_is_created_with_the_types_bigquery_has_for_its_columns() {
        assert_eq!(
            BuildLog::create_table(&bigquery(), "mysql_shop").unwrap(),
            "CREATE TABLE IF NOT EXISTS `local-project`.`mysql_shop`.`_lithify_log` (`id` INT64, `run_id` STRING, `started_at` TIMESTAMP, `artifact` STRING, `schedule` STRING, `source` STRING, `capture` STRING, `table_from` STRING, `table_to` STRING, `lifecycle` STRING, `base` STRING, `target` STRING, `window_from` TIMESTAMP, `window_to` TIMESTAMP, `status` STRING, `finished_at` TIMESTAMP, `error` STRING, `version` STRING)"
        );
    }

    #[test]
    fn a_build_log_is_appended_under_the_next_id_with_its_instants_in_milliseconds() {
        let log = BuildLog::new(
            &Run::new(
                Ulid::nil(),
                DateTime::UNIX_EPOCH,
                "snapshot",
                "daily",
                "primary",
                "{}",
                "0.1.0",
            ),
            Some(TableId::new("shop", "customers")),
            Some(TableId::new("shop", "clients")),
            Some(TablePath::new("mysql_shop", "customers_20260919")),
            Some(TablePath::new("mysql_shop", "customers_20260920")),
            Window::new(
                RecordedAt::from(1789862400000),
                RecordedAt::from(1789948800000),
            ),
            Outcome::new(
                Status::Succeeded,
                chrono::DateTime::from_timestamp_millis(1789949000000).unwrap(),
            ),
        );

        assert_eq!(
            log.insert(&bigquery(), "mysql_shop"),
            "INSERT INTO `local-project`.`mysql_shop`.`_lithify_log` (`id`, `run_id`, `started_at`, `artifact`, `schedule`, `source`, `capture`, `table_from`, `table_to`, `lifecycle`, `base`, `target`, `window_from`, `window_to`, `status`, `finished_at`, `error`, `version`)\nSELECT COALESCE(MAX(`id`), 0) + 1, '00000000000000000000000000', TIMESTAMP_MILLIS(0), 'snapshot', 'daily', 'primary', '{}', 'shop.customers', 'shop.clients', 'renamed', 'mysql_shop.customers_20260919', 'mysql_shop.customers_20260920', TIMESTAMP_MILLIS(1789862400000), TIMESTAMP_MILLIS(1789948800000), 'succeeded', TIMESTAMP_MILLIS(1789949000000), NULL, '0.1.0'\nFROM `local-project`.`mysql_shop`.`_lithify_log`"
        );
    }

    const COLUMNS: &str = "`run_id`, UNIX_MILLIS(`started_at`) AS `started_at`, `artifact`, `schedule`, `source`, `capture`, `table_from`, `table_to`, `lifecycle`, `base`, `target`, UNIX_MILLIS(`window_from`) AS `window_from`, UNIX_MILLIS(`window_to`) AS `window_to`, `status`, UNIX_MILLIS(`finished_at`) AS `finished_at`, `error`, `version`";

    #[test]
    fn a_build_log_is_read_by_the_latest_success_under_each_name_a_table_still_goes_by() {
        assert_eq!(
            Latest::Succeeded.select(&bigquery(), "mysql_shop"),
            format!(
                "SELECT {COLUMNS}\nFROM `local-project`.`mysql_shop`.`_lithify_log` AS `latest`\nWHERE `latest`.`table_to` IS NOT NULL AND `latest`.`status` = 'succeeded' AND NOT EXISTS (SELECT 1 FROM `local-project`.`mysql_shop`.`_lithify_log` AS `departed` WHERE `departed`.`artifact` = `latest`.`artifact` AND `departed`.`schedule` = `latest`.`schedule` AND `departed`.`table_from` = `latest`.`table_to` AND `departed`.`lifecycle` IN ('renamed', 'dropped') AND `departed`.`window_from` >= `latest`.`window_to` AND `departed`.`status` = 'succeeded')\nQUALIFY ROW_NUMBER() OVER (PARTITION BY `artifact`, `schedule`, `table_to` ORDER BY `id` DESC) = 1\nORDER BY `artifact`, `schedule`, `table_to`"
            )
            .as_str()
        );
    }

    #[test]
    fn a_build_log_is_read_whole_from_the_newest_record() {
        assert_eq!(
            BuildLogQuery::All.select(&bigquery(), "mysql_shop"),
            format!(
                "SELECT {COLUMNS}\nFROM `local-project`.`mysql_shop`.`_lithify_log`\nORDER BY `id` DESC"
            )
            .as_str()
        );
    }

    #[test]
    fn a_build_log_is_read_by_the_latest_success_closing_at_an_instant_under_the_name_it_closed_with()
     {
        assert_eq!(
            Latest::SucceededUntil(RecordedAt::from(1789948800000))
                .select(&bigquery(), "mysql_shop"),
            format!(
                "SELECT {COLUMNS}\nFROM `local-project`.`mysql_shop`.`_lithify_log` AS `latest`\nWHERE `latest`.`table_to` IS NOT NULL AND `latest`.`status` = 'succeeded' AND `latest`.`window_to` = TIMESTAMP_MILLIS(1789948800000)\nQUALIFY ROW_NUMBER() OVER (PARTITION BY `artifact`, `schedule`, `table_to` ORDER BY `id` DESC) = 1\nORDER BY `artifact`, `schedule`, `table_to`"
            )
            .as_str()
        );
    }

    #[test]
    fn a_table_is_looked_up_among_the_tables_of_its_dataset() {
        assert_eq!(
            bigquery().select_existence(&TablePath::new("mysql_shop", "people_20260920")),
            "SELECT COUNT(*) AS `count` FROM `local-project`.`mysql_shop`.INFORMATION_SCHEMA.TABLES WHERE table_name = 'people_20260920'"
        );
    }

    #[test]
    fn a_table_is_qualified_by_the_project_it_lives_in() {
        assert_eq!(
            bigquery().table_ref(&TablePath::new("mysql_shop", "customers_20260920")),
            "`local-project`.`mysql_shop`.`customers_20260920`"
        );
    }

    #[test]
    fn whole_numbers_and_text_take_the_one_type_bigquery_has_for_them() {
        assert_eq!(bigquery().column_type(&ColumnType::Int16).unwrap(), "INT64");
        assert_eq!(bigquery().column_type(&ColumnType::Int64).unwrap(), "INT64");
        assert_eq!(bigquery().column_type(&ColumnType::Text).unwrap(), "STRING");
        assert_eq!(bigquery().column_type(&ColumnType::Json).unwrap(), "JSON");
    }

    #[test]
    fn a_decimal_keeps_the_precision_and_scale_the_source_declared() {
        assert_eq!(
            bigquery()
                .column_type(&ColumnType::Decimal {
                    precision: 12,
                    scale: 2
                })
                .unwrap(),
            "NUMERIC(12, 2)"
        );
    }

    #[test]
    fn a_decimal_is_read_from_the_string_debezium_publishes() {
        assert_eq!(
            read(ColumnType::Decimal {
                precision: 12,
                scale: 2
            }),
            "CAST(`after`.`amount` AS NUMERIC(12, 2))"
        );
    }

    #[test]
    fn temporal_columns_are_read_from_the_types_the_sink_connector_lands() {
        assert_eq!(read(ColumnType::Date), "CAST(`after`.`amount` AS DATE)");
        assert_eq!(
            read(ColumnType::Timestamp),
            "CAST(`after`.`amount` AS DATETIME)"
        );
    }

    #[test]
    fn a_zoned_timestamp_is_read_from_the_text_debezium_publishes() {
        assert_eq!(
            read(ColumnType::ZonedTimestamp),
            "CAST(`after`.`amount` AS TIMESTAMP)"
        );
    }

    #[test]
    fn json_is_read_from_the_text_debezium_publishes() {
        assert_eq!(read(ColumnType::Json), "PARSE_JSON(`after`.`amount`)");
    }

    #[test]
    fn bytes_are_read_from_the_bytes_the_sink_connector_lands() {
        assert_eq!(read(ColumnType::Bytes), "CAST(`after`.`amount` AS BYTES)");
    }

    #[test]
    fn an_array_takes_the_type_of_its_elements() {
        assert_eq!(
            bigquery()
                .column_type(&ColumnType::Array(Box::new(ColumnType::Text)))
                .unwrap(),
            "ARRAY<STRING>"
        );
    }

    #[test]
    fn a_type_no_dialect_mapped_has_no_column() {
        let unsupported = ColumnType::Unsupported("GEOMETRY".to_owned());

        assert_eq!(
            bigquery().column_type(&unsupported).unwrap_err(),
            UnsupportedColumn::new(&unsupported)
        );
    }

    #[test]
    fn the_schema_changes_of_a_window_are_read_whole_in_the_order_they_were_recorded_then_logged() {
        let log = Debezium::new("mysql", MySql);
        let window = Window::new(RecordedAt::from(0), RecordedAt::from(10));

        assert_eq!(
            BigQueryWarehouse::new("local-project", "mysql_shop_cdc")
                .select_schema_changes(&log, &window),
            concat!(
                "SELECT *\n",
                "FROM `local-project`.`mysql_shop_cdc`.`mysql`\n",
                "WHERE `source`.`ts_ms` >= 0 AND `source`.`ts_ms` < 10\n",
                "ORDER BY `ts_ms`, `source`.`file`, `source`.`pos`",
            )
        );
    }

    #[test]
    fn a_change_log_is_read_from_the_table_its_topic_was_sanitized_into() {
        assert_eq!(
            bigquery().change_log_ref("mysql.shop.customers"),
            "`local-project`.`mysql_shop_cdc`.`mysql_shop_customers`"
        );
    }

    #[test]
    fn a_change_log_is_read_from_a_table_named_only_with_what_bigquery_allows() {
        assert_eq!(
            bigquery().change_log_ref("mysql.shop.order-lines"),
            "`local-project`.`mysql_shop_cdc`.`mysql_shop_order_lines`"
        );
    }

    #[test]
    fn a_literal_escapes_its_quotes_the_way_bigquery_reads_them() {
        assert_eq!(bigquery().literal("it's"), "'it\\'s'");
    }

    #[test]
    fn a_namespace_is_made_within_the_project_of_the_warehouse() {
        assert_eq!(
            bigquery().create_namespace("mysql_shop"),
            "CREATE SCHEMA IF NOT EXISTS `local-project`.`mysql_shop`"
        );
    }
}
