pub mod mysql;
pub mod postgres;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use event_replay_core::event::{MalformedEvent, SchemaChange, TableChange};
use event_replay_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use event_replay_core::position::{Position, RecordedAt};
use event_replay_core::source::{ChangeLog, ColumnPath};

pub trait Dialect {
    type Source;
    type Position: Position;

    fn position(&self, source: &Self::Source) -> Self::Position;

    fn column_type(&self, type_name: &str, length: Option<u32>, scale: Option<u32>) -> ColumnType;

    fn ordering(&self) -> Vec<ColumnPath>;
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DebeziumDdl<S> {
    pub source: S,
    #[serde(rename = "ts_ms")]
    pub ts_ms: i64,
    pub ddl: String,
    #[serde(default)]
    pub table_changes: Vec<DdlTableChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "bigquery", derive(google_cloud_bigquery::query::FromSql))]
#[serde(rename_all = "camelCase")]
pub struct DdlTableChange {
    #[cfg_attr(feature = "bigquery", bigquery(rename = "type"))]
    pub r#type: String,
    pub id: String,
    pub table: Option<DdlTable>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "bigquery", derive(google_cloud_bigquery::query::FromSql))]
#[serde(rename_all = "camelCase")]
pub struct DdlTable {
    #[serde(default)]
    #[cfg_attr(feature = "bigquery", bigquery(rename = "primaryKeyColumnNames"))]
    pub primary_key_column_names: Vec<String>,
    pub columns: Vec<DdlColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "bigquery", derive(google_cloud_bigquery::query::FromSql))]
#[serde(rename_all = "camelCase")]
pub struct DdlColumn {
    pub name: String,
    #[cfg_attr(feature = "bigquery", bigquery(rename = "typeName"))]
    pub type_name: String,
    pub position: i64,
    pub optional: bool,
    #[cfg_attr(feature = "bigquery", bigquery(rename = "defaultValueExpression"))]
    pub default_value_expression: Option<String>,
    pub length: Option<i64>,
    pub scale: Option<i64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Debezium<D> {
    dialect: D,
}

impl<D: Dialect> Debezium<D> {
    pub fn schema_change(
        &self,
        statement: DebeziumDdl<D::Source>,
    ) -> Result<SchemaChange<D::Position>, MalformedEvent> {
        let tables = statement
            .table_changes
            .into_iter()
            .map(|change| self.table_change(change))
            .collect::<Result<_, _>>()?;

        Ok(SchemaChange::new(
            self.dialect.position(&statement.source),
            RecordedAt::from(statement.ts_ms),
            &statement.ddl,
            tables,
        ))
    }

    pub fn new(dialect: D) -> Self {
        Self { dialect }
    }

    pub fn dialect(&self) -> &D {
        &self.dialect
    }

    fn table_change(&self, change: DdlTableChange) -> Result<TableChange, MalformedEvent> {
        let id = TableId::try_from(change.id.as_str())?;
        let table = |table: Option<DdlTable>| {
            table
                .ok_or_else(|| {
                    MalformedEvent::Undecodable(format!("{} without a table", change.r#type))
                })
                .map(|table| self.table(id.clone(), table))
        };

        match change.r#type.as_str() {
            "CREATE" => Ok(TableChange::Created(table(change.table)?)),
            "ALTER" => Ok(TableChange::Altered(table(change.table)?)),
            "DROP" => Ok(TableChange::Dropped(id)),
            _ => Err(MalformedEvent::UnknownTableChange(change.r#type)),
        }
    }

    fn table(&self, id: TableId, table: DdlTable) -> TableModel {
        let columns = table
            .columns
            .into_iter()
            .map(|column| self.column(column))
            .collect();

        TableModel::new(id, columns, table.primary_key_column_names)
    }

    fn column(&self, column: DdlColumn) -> ColumnModel {
        ColumnModel::new(
            &column.name,
            &column.type_name,
            self.dialect.column_type(
                &column.type_name,
                column.length.map(|length| length as u32),
                column.scale.map(|scale| scale as u32),
            ),
            column.position as u32,
            column.optional,
            column.default_value_expression.as_deref(),
        )
    }
}

impl<D: Dialect> Debezium<D>
where
    D::Source: DeserializeOwned,
{
    pub fn schema_change_json(
        &self,
        row: Value,
    ) -> Result<SchemaChange<D::Position>, MalformedEvent> {
        let statement: DebeziumDdl<D::Source> = serde_json::from_value(row)
            .map_err(|undecodable| MalformedEvent::Undecodable(undecodable.to_string()))?;

        self.schema_change(statement)
    }
}

impl<D: Dialect> ChangeLog for Debezium<D> {
    type Position = D::Position;

    fn key(&self) -> ColumnPath {
        ColumnPath::from("_kafka_key")
    }

    fn operation(&self) -> ColumnPath {
        ColumnPath::from("op")
    }

    fn before(&self) -> ColumnPath {
        ColumnPath::from("before")
    }

    fn after(&self) -> ColumnPath {
        ColumnPath::from("after")
    }

    fn recorded_at(&self) -> ColumnPath {
        ColumnPath::from("ts_ms")
    }

    fn ordering(&self) -> Vec<ColumnPath> {
        self.dialect.ordering()
    }
}

#[cfg(test)]
mod tests {
    use super::mysql::MySql;
    use super::*;
    use serde_json::json;

    #[test]
    fn a_statement_survives_a_round_trip_through_its_payload() {
        let row = json!({
            "source": {"file": "binlog.000002", "pos": 448, "gtid": null},
            "ts_ms": 1789800000000i64,
            "ddl": "ALTER TABLE `customers` ADD COLUMN `nickname` varchar(255)",
            "tableChanges": [{
                "type": "ALTER",
                "id": "\"shop\".\"customers\"",
                "table": {
                    "primaryKeyColumnNames": ["id"],
                    "columns": [{
                        "name": "id", "typeName": "BIGINT", "position": 1, "optional": false,
                        "defaultValueExpression": null, "length": null, "scale": null
                    }]
                }
            }]
        });

        let statement: DebeziumDdl<<MySql as Dialect>::Source> =
            serde_json::from_value(row.clone()).unwrap();

        assert_eq!(serde_json::to_value(statement).unwrap(), row);
    }
}
