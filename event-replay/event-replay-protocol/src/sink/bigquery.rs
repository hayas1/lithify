use event_replay_core::model::ColumnType;
use event_replay_core::sink::{TablePath, UnsupportedColumn, Warehouse};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigQuery {
    project: String,
}

impl BigQuery {
    pub fn new(project: &str) -> Self {
        Self {
            project: project.to_owned(),
        }
    }

    pub fn project(&self) -> &str {
        &self.project
    }
}

impl Warehouse for BigQuery {
    fn identifier(&self, name: &str) -> String {
        format!("`{name}`")
    }

    fn table_ref(&self, table: &TablePath) -> String {
        format!(
            "{}.{}.{}",
            self.identifier(&self.project),
            self.identifier(table.namespace()),
            self.identifier(table.name())
        )
    }

    fn column_type(&self, column_type: &ColumnType) -> Result<String, UnsupportedColumn> {
        match column_type {
            ColumnType::Bool => Ok("BOOL".to_owned()),
            ColumnType::Int16 | ColumnType::Int32 | ColumnType::Int64 => Ok("INT64".to_owned()),
            ColumnType::Float32 | ColumnType::Float64 => Ok("FLOAT64".to_owned()),
            ColumnType::Decimal { precision, scale } => {
                Ok(format!("NUMERIC({precision}, {scale})"))
            }
            ColumnType::Text => Ok("STRING".to_owned()),
            ColumnType::Bytes => Ok("BYTES".to_owned()),
            ColumnType::Date => Ok("DATE".to_owned()),
            ColumnType::Time => Ok("TIME".to_owned()),
            ColumnType::Timestamp => Ok("DATETIME".to_owned()),
            ColumnType::ZonedTimestamp => Ok("TIMESTAMP".to_owned()),
            ColumnType::Json => Ok("JSON".to_owned()),
            ColumnType::Array(element) => Ok(format!("ARRAY<{}>", self.column_type(element)?)),
            unsupported @ ColumnType::Unsupported(_) => Err(UnsupportedColumn::new(unsupported)),
        }
    }

    fn read(
        &self,
        expression: &str,
        column_type: &ColumnType,
    ) -> Result<String, UnsupportedColumn> {
        match column_type {
            ColumnType::Json => Ok(format!("PARSE_JSON({expression})")),
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
    use event_replay_core::model::ColumnType;
    use event_replay_core::sink::{TablePath, UnsupportedColumn, Warehouse};

    fn bigquery() -> BigQuery {
        BigQuery::new("local-project")
    }

    fn read(column_type: ColumnType) -> String {
        bigquery().read("`after`.`amount`", &column_type).unwrap()
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
}
