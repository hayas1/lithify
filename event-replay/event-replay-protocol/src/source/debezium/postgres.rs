use serde::{Deserialize, Serialize};
use sqlparser::dialect::PostgreSqlDialect;

use event_replay_core::model::ColumnType;
use event_replay_core::position::{Coordinate, Position};
use event_replay_core::source::ColumnPath;

use super::Dialect;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "bigquery", derive(google_cloud_bigquery::query::FromSql))]
pub struct PostgresSource {
    pub lsn: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PostgresPosition {
    lsn: i64,
}

impl PostgresPosition {
    pub fn lsn(&self) -> i64 {
        self.lsn
    }
}

impl Position for PostgresPosition {}

impl From<&PostgresSource> for PostgresPosition {
    fn from(source: &PostgresSource) -> Self {
        Self { lsn: source.lsn }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Postgres;

impl Dialect for Postgres {
    type Source = PostgresSource;
    type Position = PostgresPosition;
    type Syntax = PostgreSqlDialect;

    fn position(&self, source: &Self::Source) -> Self::Position {
        PostgresPosition::from(source)
    }

    fn snapshot<'a>(&self, source: &'a Self::Source) -> Option<&'a str> {
        source.snapshot.as_deref()
    }

    fn syntax(&self) -> Self::Syntax {
        PostgreSqlDialect {}
    }

    fn ordering(&self) -> Vec<ColumnPath> {
        vec![ColumnPath::from("source.lsn")]
    }

    fn coordinates(&self, position: &Self::Position) -> Vec<Coordinate> {
        vec![Coordinate::Number(position.lsn)]
    }

    fn column_type(&self, type_name: &str, length: Option<u32>, scale: Option<u32>) -> ColumnType {
        if let Some(element) = type_name.strip_prefix('_') {
            return ColumnType::Array(Box::new(self.column_type(element, length, scale)));
        }

        match type_name.to_lowercase().as_str() {
            "bool" | "boolean" => ColumnType::Bool,
            "int2" | "smallint" | "smallserial" => ColumnType::Int16,
            "int4" | "int" | "integer" | "serial" => ColumnType::Int32,
            "int8" | "bigint" | "bigserial" => ColumnType::Int64,
            "float4" | "real" => ColumnType::Float32,
            "float8" | "double precision" => ColumnType::Float64,
            "numeric" | "decimal" | "money" => ColumnType::Decimal {
                precision: length.unwrap_or_default(),
                scale: scale.unwrap_or_default(),
            },
            "text" | "varchar" | "character varying" | "bpchar" | "char" | "uuid" | "name" => {
                ColumnType::Text
            }
            "bytea" => ColumnType::Bytes,
            "date" => ColumnType::Date,
            "time" | "timetz" => ColumnType::Time,
            "timestamp" => ColumnType::Timestamp,
            "timestamptz" => ColumnType::ZonedTimestamp,
            "json" | "jsonb" => ColumnType::Json,
            unsupported => ColumnType::Unsupported(unsupported.to_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::Debezium;
    use super::*;
    use event_replay_core::event::{MalformedEvent, TableChange};
    use event_replay_core::model::{ColumnType, TableId};
    use serde_json::{Value, json};

    fn position(lsn: i64) -> PostgresPosition {
        Postgres.position(&serde_json::from_value(json!({"lsn": lsn})).unwrap())
    }

    #[test]
    fn positions_order_by_log_sequence_number() {
        assert!(position(26738384) < position(26738385));
    }

    #[test]
    fn a_statement_whose_source_carries_no_log_sequence_number_is_rejected() {
        let statement = Debezium::new("postgres", Postgres).schema_change_json(json!({
            "source": {"file": "binlog.000002", "pos": 448},
            "ts_ms": 1789800000000i64,
            "ddl": "",
            "tableChanges": []
        }));

        assert!(matches!(
            statement.unwrap_err(),
            MalformedEvent::Undecodable(_)
        ));
    }

    fn column_type(type_name: &str, length: Option<u32>, scale: Option<u32>) -> ColumnType {
        Postgres.column_type(type_name, length, scale)
    }

    #[test]
    fn whole_number_types_keep_the_width_debezium_publishes() {
        assert_eq!(column_type("int2", None, None), ColumnType::Int16);
        assert_eq!(column_type("int4", None, None), ColumnType::Int32);
        assert_eq!(column_type("int8", None, None), ColumnType::Int64);
        assert_eq!(column_type("serial", None, None), ColumnType::Int32);
    }

    #[test]
    fn a_numeric_keeps_its_precision_and_scale() {
        assert_eq!(
            column_type("numeric", Some(12), Some(2)),
            ColumnType::Decimal {
                precision: 12,
                scale: 2
            }
        );
    }

    #[test]
    fn text_and_json_types_keep_their_shape() {
        assert_eq!(column_type("text", None, None), ColumnType::Text);
        assert_eq!(column_type("varchar", Some(255), None), ColumnType::Text);
        assert_eq!(column_type("jsonb", None, None), ColumnType::Json);
        assert_eq!(column_type("bytea", None, None), ColumnType::Bytes);
        assert_eq!(column_type("bool", None, None), ColumnType::Bool);
    }

    #[test]
    fn temporal_types_distinguish_the_zoned_ones() {
        assert_eq!(column_type("date", None, None), ColumnType::Date);
        assert_eq!(column_type("timestamp", None, None), ColumnType::Timestamp);
        assert_eq!(
            column_type("timestamptz", None, None),
            ColumnType::ZonedTimestamp
        );
    }

    #[test]
    fn an_array_carries_the_type_of_its_elements() {
        assert_eq!(
            column_type("_text", None, None),
            ColumnType::Array(Box::new(ColumnType::Text))
        );
    }

    #[test]
    fn a_type_the_dialect_does_not_map_stays_named() {
        assert_eq!(
            column_type("tsvector", None, None),
            ColumnType::Unsupported("tsvector".to_owned())
        );
    }

    #[test]
    fn a_schema_change_is_positioned_by_its_log_sequence_number() {
        let change = Debezium::new("postgres", Postgres)
            .schema_change_json(json!({
                "source": {"lsn": 26738384i64, "txId": 770},
                "ts_ms": 1789800000000i64,
                "ddl": "",
                "tableChanges": []
            }))
            .unwrap();

        assert_eq!(change.position(), &position(26738384));
    }

    #[test]
    fn a_rename_is_read_from_the_ddl_in_the_syntax_of_postgres() {
        let change = Debezium::new("postgres", Postgres)
            .schema_change_json(json!({
                "source": {"lsn": 26738384i64},
                "ts_ms": 1789800000000i64,
                "ddl": "ALTER TABLE shop.customers RENAME COLUMN \"name\" TO full_name",
                "tableChanges": [{
                    "type": "ALTER",
                    "id": "\"shop\".\"customers\"",
                    "table": {"primaryKeyColumnNames": ["id"], "columns": [
                        {"name": "id", "typeName": "int8", "position": 1, "optional": false,
                         "length": null, "scale": null, "defaultValueExpression": null}
                    ]}
                }]
            }))
            .unwrap();

        assert_eq!(
            change.tables().next(),
            Some(&TableChange::ColumnRenamed {
                table: TableId::new("shop", "customers"),
                from: "name".to_owned(),
                to: "full_name".to_owned(),
            })
        );
    }

    fn captured(source: Value) -> bool {
        Debezium::new("postgres", Postgres)
            .schema_change_json(json!({
                "source": source,
                "ts_ms": 1789800000000i64,
                "ddl": "",
                "tableChanges": []
            }))
            .unwrap()
            .is_captured()
    }

    #[test]
    fn a_statement_the_snapshot_of_the_connector_took_is_captured() {
        assert!(captured(json!({"lsn": 26738384i64, "snapshot": "true"})));
    }

    #[test]
    fn a_statement_the_log_carried_is_not_captured() {
        assert!(!captured(json!({"lsn": 26738384i64, "snapshot": "false"})));
        assert!(!captured(json!({"lsn": 26738384i64})));
    }

    #[test]
    fn a_position_is_placed_by_its_log_sequence_number_as_the_dialect_orders_it() {
        let position = position(26738384);

        assert_eq!(
            Postgres.coordinates(&position),
            [Coordinate::Number(26738384)]
        );
        assert_eq!(
            Postgres.coordinates(&position).len(),
            Postgres.ordering().len()
        );
    }
}
