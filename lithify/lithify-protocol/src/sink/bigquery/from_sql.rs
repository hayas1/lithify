use google_cloud_bigquery::error::{ConvertError, RowError};
use google_cloud_bigquery::query::{FromSql, Row};

use lithify_core::build_log::BuildLogRow;

use crate::source::debezium::{DdlColumn, DebeziumDdl};

impl FromSql for DdlColumn {
    fn from_sql(value: wkt::Value) -> Result<Self, ConvertError> {
        const FIELDS: [&str; 8] = [
            "name",
            "typeName",
            "position",
            "optional",
            "defaultValueExpression",
            "length",
            "scale",
            "comment",
        ];
        let mut fields: wkt::Struct = match value {
            wkt::Value::Object(fields) => fields,
            wkt::Value::Array(values) => FIELDS
                .iter()
                .map(|name| (*name).to_owned())
                .zip(values)
                .collect(),
            other => {
                return Err(ConvertError::TypeMismatch {
                    expected: "array or object",
                    got: other,
                });
            }
        };
        let mut take = |name: &str| {
            fields
                .remove(name)
                .ok_or_else(|| ConvertError::MissingField(name.to_owned()))
        };

        Ok(Self {
            name: FromSql::from_sql(take("name")?)?,
            type_name: FromSql::from_sql(take("typeName")?)?,
            position: FromSql::from_sql(take("position")?)?,
            optional: FromSql::from_sql(take("optional")?)?,
            default_value_expression: FromSql::from_sql(take("defaultValueExpression")?)?,
            length: FromSql::from_sql(take("length")?)?,
            scale: FromSql::from_sql(take("scale")?)?,
            comment: FromSql::from_sql(take("comment").unwrap_or_default())?,
        })
    }
}

impl<S: FromSql> TryFrom<Row> for DebeziumDdl<S> {
    type Error = RowError;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Self {
            source: row.try_get("source")?,
            ts_ms: row.try_get("ts_ms")?,
            ddl: row.try_get("ddl")?,
            table_changes: row.try_get("tableChanges")?,
        })
    }
}

pub(crate) struct BigQueryCount(pub(crate) i64);

impl TryFrom<Row> for BigQueryCount {
    type Error = RowError;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Self(row.try_get("count")?))
    }
}

pub(crate) struct BigQueryBuildLogRow(pub(crate) BuildLogRow);

impl TryFrom<Row> for BigQueryBuildLogRow {
    type Error = RowError;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Self(BuildLogRow {
            run_id: row.try_get("run_id")?,
            artifact: row.try_get("artifact")?,
            schedule: row.try_get("schedule")?,
            source: row.try_get("source")?,
            capture: row.try_get("capture")?,
            table_from: row.try_get("table_from")?,
            table_to: row.try_get("table_to")?,
            lifecycle: row.try_get("lifecycle")?,
            base: row.try_get("base")?,
            target: row.try_get("target")?,
            window_from: row.try_get("window_from")?,
            window_to: row.try_get("window_to")?,
            status: row.try_get("status")?,
            finished_at: row.try_get("finished_at")?,
            error: row.try_get("error")?,
            version: row.try_get("version")?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::debezium::DdlTableChange;
    use crate::source::debezium::mysql::MySqlSource;
    use crate::source::debezium::postgres::PostgresSource;
    use serde_json::json;

    #[test]
    fn a_source_is_read_from_the_struct_the_sink_wrote() {
        let source = MySqlSource::from_sql(json!({
            "file": "binlog.000002",
            "pos": "448",
            "gtid": null,
            "snapshot": "false"
        }))
        .unwrap();

        assert_eq!(source.file, "binlog.000002");
        assert_eq!(source.pos, 448);
        assert_eq!(source.gtid, None);
    }

    #[test]
    fn a_source_carries_the_snapshot_mark_the_sink_wrote() {
        let source = MySqlSource::from_sql(json!({
            "file": "binlog.000002",
            "pos": "448",
            "gtid": null,
            "snapshot": "last"
        }))
        .unwrap();

        assert_eq!(source.snapshot.as_deref(), Some("last"));
    }

    #[test]
    fn a_position_the_warehouse_wrote_as_a_number_is_read_as_one() {
        let source = PostgresSource::from_sql(json!({"lsn": 26738384, "snapshot": null})).unwrap();

        assert_eq!(source.lsn, 26738384);
    }

    #[test]
    fn a_column_is_read_with_the_widths_the_source_declared() {
        let column = DdlColumn::from_sql(json!({
            "name": "amount",
            "typeName": "DECIMAL",
            "position": "4",
            "optional": false,
            "defaultValueExpression": null,
            "length": "12",
            "scale": "2"
        }))
        .unwrap();

        assert_eq!(column.name, "amount");
        assert_eq!(column.type_name, "DECIMAL");
        assert_eq!(column.position, 4);
        assert!(!column.optional);
        assert_eq!(column.length, Some(12));
        assert_eq!(column.scale, Some(2));
    }

    #[test]
    fn a_column_carries_the_comment_the_sink_wrote() {
        let column = DdlColumn::from_sql(json!({
            "name": "id",
            "typeName": "BIGINT",
            "position": "1",
            "optional": false,
            "defaultValueExpression": null,
            "length": null,
            "scale": null,
            "comment": "who they are"
        }))
        .unwrap();

        assert_eq!(column.comment.as_deref(), Some("who they are"));
    }

    #[test]
    fn a_column_the_sink_wrote_without_a_comment_is_read_without_one() {
        let column = DdlColumn::from_sql(json!({
            "name": "id",
            "typeName": "BIGINT",
            "position": "1",
            "optional": false,
            "defaultValueExpression": null,
            "length": null,
            "scale": null
        }))
        .unwrap();

        assert_eq!(column.comment, None);
    }

    #[test]
    fn a_table_change_carries_the_table_it_left_behind() {
        let change = DdlTableChange::from_sql(json!({
            "type": "CREATE",
            "id": "\"shop\".\"customers\"",
            "table": {
                "primaryKeyColumnNames": ["id"],
                "columns": [{
                    "name": "id",
                    "typeName": "BIGINT",
                    "position": "1",
                    "optional": false,
                    "defaultValueExpression": null,
                    "length": null,
                    "scale": null
                }]
            }
        }))
        .unwrap();

        assert_eq!(change.r#type, "CREATE");
        assert_eq!(change.id, "\"shop\".\"customers\"");
        let table = change.table.unwrap();
        assert_eq!(table.primary_key_column_names, ["id"]);
        assert_eq!(table.columns[0].name, "id");
    }

    #[test]
    fn a_statement_that_changed_no_table_carries_none() {
        let change = DdlTableChange::from_sql(json!({
            "type": "DROP",
            "id": "\"shop\".\"customers\"",
            "table": null
        }))
        .unwrap();

        assert_eq!(change.table, None);
    }
}
