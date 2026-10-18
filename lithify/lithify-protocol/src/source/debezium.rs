pub mod config;
pub mod mysql;
pub mod postgres;
pub mod rename;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use lithify_core::event::{Hint, MalformedEvent, SchemaChange, TableChange};
use lithify_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use lithify_core::position::{Coordinate, Position, RecordedAt};
use lithify_core::source::{
    CdcFormat, ChangeLog, ChangeLogs, ColumnPath, RowChangeLog, SchemaChangeLog,
};

use rename::RenameReader;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DebeziumFormat;

impl CdcFormat for DebeziumFormat {
    type Config = config::Config;
}

pub trait Dialect {
    type Source;
    type Position: Position;
    type Syntax: sqlparser::dialect::Dialect;

    fn position(&self, source: &Self::Source) -> Self::Position;

    fn snapshot<'a>(&self, source: &'a Self::Source) -> Option<&'a str>;

    fn column_type(&self, type_name: &str, length: Option<u32>, scale: Option<u32>) -> ColumnType;

    fn ordering(&self) -> Vec<ColumnPath>;

    fn coordinates(&self, position: &Self::Position) -> Vec<Coordinate>;

    fn syntax(&self) -> Self::Syntax;

    fn hints(&self, ddl: &str, schema: &str) -> Vec<Hint> {
        RenameReader::new(self.syntax()).read(ddl, schema)
    }
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
#[serde(rename_all = "camelCase")]
pub struct DdlColumn {
    pub name: String,
    pub type_name: String,
    pub position: i64,
    pub optional: bool,
    pub default_value_expression: Option<String>,
    pub length: Option<i64>,
    pub scale: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Debezium<D> {
    dialect: D,
    envelope: Option<ColumnPath>,
    topic_prefix: String,
    ddl_topic: String,
}

impl<D: Dialect> Debezium<D> {
    pub fn new(topic_prefix: &str, dialect: D) -> Self {
        Self::within(None, topic_prefix, dialect)
    }

    pub fn under(column: &str, topic_prefix: &str, dialect: D) -> Self {
        Self::within(Some(column), topic_prefix, dialect)
    }

    pub fn within(envelope: Option<&str>, topic_prefix: &str, dialect: D) -> Self {
        Self {
            dialect,
            envelope: envelope.map(ColumnPath::from),
            topic_prefix: topic_prefix.to_owned(),
            ddl_topic: topic_prefix.to_owned(),
        }
    }

    pub fn with_ddl_topic(self, ddl_topic: &str) -> Self {
        Self {
            ddl_topic: ddl_topic.to_owned(),
            ..self
        }
    }

    fn path(&self, name: &str) -> ColumnPath {
        match &self.envelope {
            Some(envelope) => ColumnPath::from(format!("{envelope}.{name}").as_str()),
            None => ColumnPath::from(name),
        }
    }

    pub fn dialect(&self) -> &D {
        &self.dialect
    }

    fn captured(&self, source: &D::Source) -> bool {
        self.dialect
            .snapshot(source)
            .is_some_and(|mark| mark != "false")
    }

    fn ordered(&self) -> Vec<ColumnPath> {
        self.dialect
            .ordering()
            .iter()
            .map(|path| self.path(&path.parts().collect::<Vec<_>>().join(".")))
            .collect()
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
        let mut columns = table.columns;
        columns.sort_by_key(|column| column.position);

        let columns = columns
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
            column.optional,
            column.default_value_expression.as_deref(),
        )
        .with_comment(column.comment.as_deref())
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

    fn envelope(&self) -> Option<ColumnPath> {
        self.envelope.clone()
    }

    fn occurred_at(&self) -> ColumnPath {
        self.path("source.ts_ms")
    }

    fn recorded_at(&self) -> ColumnPath {
        self.path("ts_ms")
    }

    fn ordering(&self) -> Vec<ColumnPath> {
        self.ordered()
    }

    fn coordinates(&self, position: &Self::Position) -> Vec<Coordinate> {
        self.dialect.coordinates(position)
    }
}

impl<D: Dialect> ChangeLogs for Debezium<D> {
    type Position = D::Position;
    type Schema = Self;
    type Row = Self;

    fn schema_changes(&self) -> &Self::Schema {
        self
    }

    fn row_changes(&self) -> &Self::Row {
        self
    }
}

impl<D: Dialect> SchemaChangeLog for Debezium<D> {
    type Record = DebeziumDdl<D::Source>;

    fn ddl_topic(&self) -> String {
        self.ddl_topic.clone()
    }

    fn schema_change(
        &self,
        statement: Self::Record,
    ) -> Result<SchemaChange<Self::Position>, MalformedEvent> {
        let tables = statement
            .table_changes
            .into_iter()
            .map(|change| self.table_change(change))
            .collect::<Result<Vec<_>, _>>()?;
        let hints = tables
            .first()
            .map(|change| self.dialect.hints(&statement.ddl, change.id().schema()))
            .unwrap_or_default();

        let change = SchemaChange::new(
            self.dialect.position(&statement.source),
            RecordedAt::from(statement.ts_ms),
            &statement.ddl,
            tables,
            hints,
        );

        match self.captured(&statement.source) {
            true => Ok(change.captured()),
            false => Ok(change),
        }
    }
}

impl<D: Dialect> RowChangeLog for Debezium<D> {
    fn dml_topic(&self, table: &TableId) -> String {
        format!("{}.{}.{}", self.topic_prefix, table.schema(), table.table())
    }

    fn operation(&self) -> ColumnPath {
        self.path("op")
    }

    fn before(&self) -> ColumnPath {
        self.path("before")
    }

    fn after(&self) -> ColumnPath {
        self.path("after")
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::mysql::MySql;
    use super::*;
    use serde_json::json;

    pub(crate) fn described(table: &TableChange) -> String {
        match table {
            TableChange::Created(table) => format!("created {}", table.id()),
            TableChange::Altered(table) => format!("altered {}", table.id()),
            TableChange::Dropped(id) => format!("dropped {id}"),
        }
    }

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

    fn comment_of(column: Value) -> Option<String> {
        let statement = Debezium::new("mysql", MySql)
            .schema_change_json(json!({
                "source": {"file": "binlog.000002", "pos": 448, "gtid": null},
                "ts_ms": 1789800000000i64,
                "ddl": "ALTER TABLE `customers` MODIFY `id` bigint COMMENT 'who they are'",
                "tableChanges": [{
                    "type": "ALTER",
                    "id": "\"shop\".\"customers\"",
                    "table": {"primaryKeyColumnNames": ["id"], "columns": [column]}
                }]
            }))
            .unwrap();
        let Some(TableChange::Altered(table)) = statement.tables().next() else {
            panic!("expected an altered table");
        };

        table.column("id").unwrap().comment().map(str::to_owned)
    }

    #[test]
    fn a_column_carries_the_comment_the_source_described_it_with() {
        assert_eq!(
            comment_of(json!({
                "name": "id", "typeName": "BIGINT", "position": 1, "optional": false,
                "defaultValueExpression": null, "length": null, "scale": null,
                "comment": "who they are"
            })),
            Some("who they are".to_owned())
        );
    }

    #[test]
    fn a_column_logged_without_a_comment_is_undescribed() {
        assert_eq!(
            comment_of(json!({
                "name": "id", "typeName": "BIGINT", "position": 1, "optional": false,
                "defaultValueExpression": null, "length": null, "scale": null
            })),
            None
        );
    }

    #[test]
    fn a_table_holds_its_columns_in_the_order_the_source_positioned_them() {
        let statement = Debezium::new("mysql", MySql)
            .schema_change_json(json!({
                "source": {"file": "binlog.000002", "pos": 448, "gtid": null},
                "ts_ms": 1789800000000i64,
                "ddl": "ALTER TABLE `customers` ADD COLUMN `nickname` varchar(255) AFTER `id`",
                "tableChanges": [{
                    "type": "ALTER",
                    "id": "\"shop\".\"customers\"",
                    "table": {
                        "primaryKeyColumnNames": ["id"],
                        "columns": [
                            {
                                "name": "name", "typeName": "VARCHAR", "position": 3,
                                "optional": true, "defaultValueExpression": null,
                                "length": 255, "scale": null
                            },
                            {
                                "name": "id", "typeName": "BIGINT", "position": 1,
                                "optional": false, "defaultValueExpression": null,
                                "length": null, "scale": null
                            },
                            {
                                "name": "nickname", "typeName": "VARCHAR", "position": 2,
                                "optional": true, "defaultValueExpression": null,
                                "length": 255, "scale": null
                            }
                        ]
                    }
                }]
            }))
            .unwrap();

        let Some(TableChange::Altered(table)) = statement.tables().next() else {
            panic!("expected an altered table");
        };

        assert_eq!(
            table.columns().map(ColumnModel::name).collect::<Vec<_>>(),
            ["id", "nickname", "name"]
        );
    }

    #[test]
    fn the_row_changes_of_a_table_land_on_the_topic_named_after_the_prefix_and_the_table() {
        assert_eq!(
            Debezium::new("mysql", MySql).dml_topic(&TableId::new("shop", "customers")),
            "mysql.shop.customers"
        );
    }

    #[test]
    fn the_schema_changes_land_on_the_topic_named_after_the_prefix_alone() {
        assert_eq!(Debezium::new("mysql", MySql).ddl_topic(), "mysql");
    }

    #[test]
    fn the_schema_changes_land_on_a_topic_of_their_own_when_one_is_named() {
        let log = Debezium::new("mysql", MySql).with_ddl_topic("mysql.schema-changes");

        assert_eq!(log.ddl_topic(), "mysql.schema-changes");
        assert_eq!(
            log.dml_topic(&TableId::new("shop", "customers")),
            "mysql.shop.customers"
        );
    }
}
