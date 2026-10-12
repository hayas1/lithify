use serde::{Deserialize, Serialize};

use sqlparser::dialect::MySqlDialect;

use event_replay_core::model::ColumnType;
use event_replay_core::position::Position;
use event_replay_core::source::ColumnPath;

use super::Dialect;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg_attr(feature = "bigquery", derive(google_cloud_bigquery::query::FromSql))]
pub struct MySqlSource {
    pub file: String,
    pub pos: i64,
    pub gtid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MySqlPosition {
    file: String,
    pos: i64,
    transaction: Option<String>,
}

impl MySqlPosition {
    pub fn file(&self) -> &str {
        &self.file
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn transaction(&self) -> Option<&str> {
        self.transaction.as_deref()
    }
}

impl Position for MySqlPosition {}

impl From<&MySqlSource> for MySqlPosition {
    fn from(source: &MySqlSource) -> Self {
        Self {
            file: source.file.clone(),
            pos: source.pos,
            transaction: source.gtid.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MySql;

impl Dialect for MySql {
    type Source = MySqlSource;
    type Position = MySqlPosition;
    type Syntax = MySqlDialect;

    fn position(&self, source: &Self::Source) -> Self::Position {
        MySqlPosition::from(source)
    }

    fn snapshot<'a>(&self, source: &'a Self::Source) -> Option<&'a str> {
        source.snapshot.as_deref()
    }

    fn syntax(&self) -> Self::Syntax {
        MySqlDialect {}
    }

    fn ordering(&self) -> Vec<ColumnPath> {
        vec![
            ColumnPath::from("source.file"),
            ColumnPath::from("source.pos"),
        ]
    }

    fn column_type(&self, type_name: &str, length: Option<u32>, scale: Option<u32>) -> ColumnType {
        match type_name.to_uppercase().as_str() {
            "TINYINT" | "SMALLINT" | "YEAR" => ColumnType::Int16,
            "MEDIUMINT" | "INT" | "INTEGER" => ColumnType::Int32,
            "BIGINT" => ColumnType::Int64,
            "FLOAT" => ColumnType::Float32,
            "DOUBLE" | "REAL" => ColumnType::Float64,
            "DECIMAL" | "NUMERIC" => ColumnType::Decimal {
                precision: length.unwrap_or_default(),
                scale: scale.unwrap_or_default(),
            },
            "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM"
            | "SET" => ColumnType::Text,
            "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BIT" => {
                ColumnType::Bytes
            }
            "DATE" => ColumnType::Date,
            "TIME" => ColumnType::Time,
            "DATETIME" => ColumnType::Timestamp,
            "TIMESTAMP" => ColumnType::ZonedTimestamp,
            "JSON" => ColumnType::Json,
            unsupported => ColumnType::Unsupported(unsupported.to_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::Debezium;
    use super::*;
    use event_replay_core::event::{MalformedEvent, TableChange};
    use serde_json::{Value, json};

    fn source(source: Value) -> MySqlSource {
        serde_json::from_value(source).unwrap()
    }

    fn position(file: &str, pos: i64) -> MySqlPosition {
        MySql.position(&source(json!({"file": file, "pos": pos})))
    }

    #[test]
    fn positions_order_by_binlog_file_then_offset() {
        assert!(position("binlog.000002", 448) < position("binlog.000002", 449));
        assert!(position("binlog.000002", 999) < position("binlog.000003", 4));
    }

    #[test]
    fn a_position_carries_the_transaction_identifier_when_the_server_assigns_one() {
        let position = MySql.position(&source(
            json!({"file": "binlog.000002", "pos": 448, "gtid": "f07c93b3:15"}),
        ));

        assert_eq!(position.transaction(), Some("f07c93b3:15"));
        assert_eq!(self::position("binlog.000002", 448).transaction(), None);
    }

    #[test]
    fn a_statement_whose_source_carries_no_binlog_offset_is_rejected() {
        let statement = Debezium::new(MySql).schema_change_json(json!({
            "source": {"lsn": 26738384i64},
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
        MySql.column_type(type_name, length, scale)
    }

    #[test]
    fn whole_number_types_keep_the_width_debezium_publishes() {
        assert_eq!(column_type("TINYINT", None, None), ColumnType::Int16);
        assert_eq!(column_type("SMALLINT", None, None), ColumnType::Int16);
        assert_eq!(column_type("INT", None, None), ColumnType::Int32);
        assert_eq!(column_type("BIGINT", None, None), ColumnType::Int64);
    }

    #[test]
    fn a_decimal_keeps_its_precision_and_scale() {
        assert_eq!(
            column_type("DECIMAL", Some(12), Some(2)),
            ColumnType::Decimal {
                precision: 12,
                scale: 2
            }
        );
    }

    #[test]
    fn text_types_are_text() {
        assert_eq!(column_type("VARCHAR", Some(255), None), ColumnType::Text);
        assert_eq!(column_type("TEXT", None, None), ColumnType::Text);
        assert_eq!(column_type("ENUM", None, None), ColumnType::Text);
    }

    #[test]
    fn temporal_types_follow_what_the_source_stores() {
        assert_eq!(column_type("DATE", None, None), ColumnType::Date);
        assert_eq!(column_type("TIME", None, None), ColumnType::Time);
        assert_eq!(
            column_type("DATETIME", Some(6), None),
            ColumnType::Timestamp
        );
        assert_eq!(
            column_type("TIMESTAMP", None, None),
            ColumnType::ZonedTimestamp
        );
    }

    #[test]
    fn json_and_binary_types_keep_their_shape() {
        assert_eq!(column_type("JSON", None, None), ColumnType::Json);
        assert_eq!(column_type("BLOB", None, None), ColumnType::Bytes);
        assert_eq!(column_type("DOUBLE", None, None), ColumnType::Float64);
        assert_eq!(column_type("FLOAT", None, None), ColumnType::Float32);
    }

    #[test]
    fn a_type_the_dialect_does_not_map_stays_named() {
        assert_eq!(
            column_type("GEOMETRY", None, None),
            ColumnType::Unsupported("GEOMETRY".to_owned())
        );
    }

    #[test]
    fn a_column_of_a_schema_change_carries_its_type() {
        let change = Debezium::new(MySql)
            .schema_change_json(json!({
                "source": {"file": "binlog.000002", "pos": 448},
                "ts_ms": 1789800000000i64,
                "ddl": "",
                "tableChanges": [{
                    "type": "CREATE",
                    "id": "\"shop\".\"orders\"",
                    "table": {"primaryKeyColumnNames": ["id"], "columns": [
                        {"name": "amount", "typeName": "DECIMAL", "position": 1, "optional": false,
                         "length": 12, "scale": 2, "defaultValueExpression": null}
                    ]}
                }]
            }))
            .unwrap();

        let TableChange::Created(table) = change.tables().next().unwrap() else {
            panic!("expected a created table");
        };
        assert_eq!(
            table.column("amount").unwrap().column_type(),
            &ColumnType::Decimal {
                precision: 12,
                scale: 2
            }
        );
    }

    #[test]
    fn a_schema_change_is_positioned_by_its_binlog_offset() {
        let change = Debezium::new(MySql)
            .schema_change_json(json!({
                "source": {"file": "binlog.000002", "pos": 448},
                "ts_ms": 1789800000000i64,
                "ddl": "ALTER TABLE `customers` ADD COLUMN `nickname` varchar(255)",
                "tableChanges": []
            }))
            .unwrap();

        assert_eq!(change.position(), &position("binlog.000002", 448));
    }

    fn altered(ddl: &str, ids: &[&str]) -> Vec<String> {
        let table_changes: Vec<Value> = ids
            .iter()
            .map(|id| {
                json!({
                    "type": "ALTER",
                    "id": id,
                    "table": {"primaryKeyColumnNames": ["id"], "columns": [
                        {"name": "id", "typeName": "BIGINT", "position": 1, "optional": false,
                         "length": null, "scale": null, "defaultValueExpression": null}
                    ]}
                })
            })
            .collect();
        let change = Debezium::new(MySql)
            .schema_change_json(json!({
                "source": {"file": "binlog.000002", "pos": 448},
                "ts_ms": 1789800000000i64,
                "ddl": ddl,
                "tableChanges": table_changes
            }))
            .unwrap();

        change
            .tables()
            .map(|table| match table {
                TableChange::Renamed { from, to } => format!("renamed {from} to {to}"),
                TableChange::ColumnRenamed { table, from, to } => {
                    format!("renamed {table}.{from} to {to}")
                }
                TableChange::Altered(table) => format!("altered {}", table.id()),
                TableChange::Created(table) => format!("created {}", table.id()),
                TableChange::Dropped(id) => format!("dropped {id}"),
            })
            .collect()
    }

    #[test]
    fn a_column_renamed_by_rename_column_is_renamed_before_the_table_is_altered() {
        assert_eq!(
            altered(
                "ALTER TABLE probe RENAME COLUMN name TO full_name",
                &["\"shop\".\"probe\""]
            ),
            ["renamed shop.probe.name to full_name", "altered shop.probe"]
        );
    }

    #[test]
    fn a_column_renamed_by_change_is_renamed_before_the_table_is_altered() {
        assert_eq!(
            altered(
                "ALTER TABLE probe CHANGE email mail VARCHAR(128)",
                &["\"shop\".\"probe\""]
            ),
            ["renamed shop.probe.email to mail", "altered shop.probe"]
        );
    }

    #[test]
    fn a_change_that_keeps_the_name_of_its_column_renames_nothing() {
        assert_eq!(
            altered(
                "ALTER TABLE probe CHANGE email email VARCHAR(128)",
                &["\"shop\".\"probe\""]
            ),
            ["altered shop.probe"]
        );
    }

    #[test]
    fn a_table_renamed_by_rename_table_is_renamed_within_the_schema_it_landed_in() {
        assert_eq!(
            altered("RENAME TABLE probe TO probe2", &["\"shop\".\"probe2\""]),
            ["renamed shop.probe to shop.probe2", "altered shop.probe2"]
        );
    }

    #[test]
    fn every_table_one_rename_table_renames_is_renamed_in_its_order() {
        assert_eq!(
            altered(
                "RENAME TABLE a TO b, b TO c",
                &["\"shop\".\"b\"", "\"shop\".\"c\""]
            ),
            [
                "renamed shop.a to shop.b",
                "renamed shop.b to shop.c",
                "altered shop.b",
                "altered shop.c"
            ]
        );
    }

    #[test]
    fn a_table_renamed_by_alter_table_is_renamed_whether_to_or_as() {
        assert_eq!(
            altered(
                "ALTER TABLE probe2 RENAME TO probe3",
                &["\"shop\".\"probe3\""]
            ),
            ["renamed shop.probe2 to shop.probe3", "altered shop.probe3"]
        );
        assert_eq!(
            altered(
                "ALTER TABLE probe2 RENAME AS probe3",
                &["\"shop\".\"probe3\""]
            ),
            ["renamed shop.probe2 to shop.probe3", "altered shop.probe3"]
        );
    }

    #[test]
    fn columns_are_renamed_under_the_old_name_of_a_table_renamed_by_the_same_statement() {
        assert_eq!(
            altered(
                "ALTER TABLE `probe` RENAME COLUMN `name` TO `full_name`, RENAME TO `probe2`, ADD COLUMN age INT",
                &["\"shop\".\"probe2\""]
            ),
            [
                "renamed shop.probe.name to full_name",
                "renamed shop.probe to shop.probe2",
                "altered shop.probe2"
            ]
        );
    }

    #[test]
    fn a_qualified_name_keeps_the_schema_it_names() {
        assert_eq!(
            altered(
                "RENAME TABLE archive.probe TO shop.probe",
                &["\"shop\".\"probe\""]
            ),
            ["renamed archive.probe to shop.probe", "altered shop.probe"]
        );
    }

    #[test]
    fn a_statement_the_dialect_cannot_read_renames_nothing() {
        assert_eq!(
            altered(
                "ALTER TABLE probe SOMETHING NO PARSER KNOWS",
                &["\"shop\".\"probe\""]
            ),
            ["altered shop.probe"]
        );
    }

    #[test]
    fn a_statement_that_renames_nothing_only_alters() {
        assert_eq!(
            altered(
                "ALTER TABLE probe ADD COLUMN age INT",
                &["\"shop\".\"probe\""]
            ),
            ["altered shop.probe"]
        );
    }

    #[test]
    fn columns_are_renamed_under_the_old_name_even_when_the_table_is_renamed_first() {
        assert_eq!(
            altered(
                "ALTER TABLE probe RENAME TO probe2, RENAME COLUMN name TO full_name",
                &["\"shop\".\"probe2\""]
            ),
            [
                "renamed shop.probe.name to full_name",
                "renamed shop.probe to shop.probe2",
                "altered shop.probe2"
            ]
        );
    }

    fn captured(source: Value) -> bool {
        Debezium::new(MySql)
            .schema_change_json(json!({
                "source": source,
                "ts_ms": 1789800000000i64,
                "ddl": "CREATE TABLE `customers` (`id` bigint)",
                "tableChanges": []
            }))
            .unwrap()
            .is_captured()
    }

    #[test]
    fn a_statement_the_snapshot_of_the_connector_took_is_captured() {
        assert!(captured(
            json!({"file": "binlog.000002", "pos": 448, "snapshot": "true"})
        ));
        assert!(captured(
            json!({"file": "binlog.000002", "pos": 448, "snapshot": "first"})
        ));
        assert!(captured(
            json!({"file": "binlog.000002", "pos": 448, "snapshot": "last"})
        ));
        assert!(captured(
            json!({"file": "binlog.000002", "pos": 448, "snapshot": "incremental"})
        ));
    }

    #[test]
    fn a_statement_the_binlog_carried_is_not_captured() {
        assert!(!captured(
            json!({"file": "binlog.000002", "pos": 448, "snapshot": "false"})
        ));
        assert!(!captured(json!({"file": "binlog.000002", "pos": 448})));
    }
}
