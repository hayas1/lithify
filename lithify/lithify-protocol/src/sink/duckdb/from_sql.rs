use duckdb::arrow::array::{
    Array, ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, LargeListArray,
    LargeStringArray, ListArray, StringArray, StructArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use duckdb::arrow::datatypes::DataType;
use duckdb::types::{FromSql, FromSqlError, FromSqlResult, ListType, ValueRef};

use crate::source::debezium::mysql::MySqlSource;
use crate::source::debezium::postgres::PostgresSource;
use crate::source::debezium::{DdlColumn, DdlTable, DdlTableChange, DebeziumDdl};

// TODO: duckdb reads a nested value with Row::value_ref_internal, which is
// pub(crate), and ValueRef::from(&Value::Struct) is unsupported, so a FromSql
// implementation cannot reach into a STRUCT or a LIST without rebuilding the
// ValueRef here. Delete this in favour of the upstream conversion once duckdb
// exposes it (last checked against duckdb 1.10505.0).
fn element(array: &ArrayRef, row: usize) -> FromSqlResult<ValueRef<'_>> {
    if array.is_null(row) {
        return Ok(ValueRef::Null);
    }

    match array.data_type() {
        DataType::Null => Ok(ValueRef::Null),
        DataType::Boolean => {
            downcast::<BooleanArray>(array).map(|values| ValueRef::Boolean(values.value(row)))
        }
        DataType::Int8 => {
            downcast::<Int8Array>(array).map(|values| ValueRef::TinyInt(values.value(row)))
        }
        DataType::Int16 => {
            downcast::<Int16Array>(array).map(|values| ValueRef::SmallInt(values.value(row)))
        }
        DataType::Int32 => {
            downcast::<Int32Array>(array).map(|values| ValueRef::Int(values.value(row)))
        }
        DataType::Int64 => {
            downcast::<Int64Array>(array).map(|values| ValueRef::BigInt(values.value(row)))
        }
        DataType::UInt8 => {
            downcast::<UInt8Array>(array).map(|values| ValueRef::UTinyInt(values.value(row)))
        }
        DataType::UInt16 => {
            downcast::<UInt16Array>(array).map(|values| ValueRef::USmallInt(values.value(row)))
        }
        DataType::UInt32 => {
            downcast::<UInt32Array>(array).map(|values| ValueRef::UInt(values.value(row)))
        }
        DataType::UInt64 => {
            downcast::<UInt64Array>(array).map(|values| ValueRef::UBigInt(values.value(row)))
        }
        DataType::Utf8 => downcast::<StringArray>(array)
            .map(|values| ValueRef::Text(values.value(row).as_bytes())),
        DataType::LargeUtf8 => downcast::<LargeStringArray>(array)
            .map(|values| ValueRef::Text(values.value(row).as_bytes())),
        DataType::Struct(_) => {
            downcast::<StructArray>(array).map(|values| ValueRef::Struct(values, row))
        }
        DataType::List(_) => downcast::<ListArray>(array)
            .map(|values| ValueRef::List(ListType::Regular(values), row)),
        DataType::LargeList(_) => downcast::<LargeListArray>(array)
            .map(|values| ValueRef::List(ListType::Large(values), row)),
        _ => Err(FromSqlError::InvalidType),
    }
}

fn downcast<A: Array + 'static>(array: &ArrayRef) -> FromSqlResult<&A> {
    array
        .as_any()
        .downcast_ref::<A>()
        .ok_or(FromSqlError::InvalidType)
}

fn member<T: FromSql>(structure: &StructArray, row: usize, name: &str) -> FromSqlResult<T> {
    let column = structure
        .column_by_name(name)
        .ok_or(FromSqlError::InvalidType)?;

    T::column_result(element(column, row)?)
}

fn member_or_none<T: FromSql>(
    structure: &StructArray,
    row: usize,
    name: &str,
) -> FromSqlResult<Option<T>> {
    match structure.column_by_name(name) {
        Some(column) => Option::<T>::column_result(element(column, row)?),
        None => Ok(None),
    }
}

// TODO: members belongs on T rather than here: a list would be read by FromSql
// for Vec<T>, which the orphan rule forbids us to write and duckdb does not
// provide. Delete it once duckdb reads a LIST into a typed collection.
fn members<T: FromSql>(structure: &StructArray, row: usize, name: &str) -> FromSqlResult<Vec<T>> {
    let column = structure
        .column_by_name(name)
        .ok_or(FromSqlError::InvalidType)?;

    let (values, from, length) = match element(column, row)? {
        ValueRef::Null => return Ok(Vec::new()),
        ValueRef::List(ListType::Regular(list), row) => (
            list.values(),
            list.value_offsets()[row] as usize,
            list.value_length(row) as usize,
        ),
        ValueRef::List(ListType::Large(list), row) => (
            list.values(),
            list.value_offsets()[row] as usize,
            list.value_length(row) as usize,
        ),
        _ => return Err(FromSqlError::InvalidType),
    };

    (from..from + length)
        .map(|row| T::column_result(element(values, row)?))
        .collect()
}

fn structure(value: ValueRef<'_>) -> FromSqlResult<(&StructArray, usize)> {
    match value {
        ValueRef::Struct(fields, row) => Ok((fields, row)),
        _ => Err(FromSqlError::InvalidType),
    }
}

impl FromSql for MySqlSource {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let (source, row) = structure(value)?;

        Ok(Self {
            file: member(source, row, "file")?,
            pos: member(source, row, "pos")?,
            gtid: member(source, row, "gtid")?,
            snapshot: member_or_none(source, row, "snapshot")?,
        })
    }
}

impl FromSql for PostgresSource {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let (source, row) = structure(value)?;

        Ok(Self {
            lsn: member(source, row, "lsn")?,
            snapshot: member_or_none(source, row, "snapshot")?,
        })
    }
}

impl FromSql for DdlColumn {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let (column, row) = structure(value)?;

        Ok(Self {
            name: member(column, row, "name")?,
            type_name: member(column, row, "typeName")?,
            position: member(column, row, "position")?,
            optional: member(column, row, "optional")?,
            default_value_expression: member(column, row, "defaultValueExpression")?,
            length: member(column, row, "length")?,
            scale: member(column, row, "scale")?,
            comment: member_or_none(column, row, "comment")?,
        })
    }
}

impl FromSql for DdlTable {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let (table, row) = structure(value)?;

        Ok(Self {
            primary_key_column_names: members(table, row, "primaryKeyColumnNames")?,
            columns: members(table, row, "columns")?,
        })
    }
}

impl FromSql for DdlTableChange {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let (change, row) = structure(value)?;

        Ok(Self {
            r#type: member(change, row, "type")?,
            id: member(change, row, "id")?,
            table: member(change, row, "table")?,
        })
    }
}

impl<S: FromSql> FromSql for DebeziumDdl<S> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let (statement, row) = structure(value)?;

        Ok(Self {
            source: member(statement, row, "source")?,
            ts_ms: member(statement, row, "ts_ms")?,
            ddl: member(statement, row, "ddl")?,
            table_changes: members(statement, row, "tableChanges")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    const CUSTOMERS: &str = "SELECT {
        'source': {'file': 'binlog.000002', 'pos': 448, 'gtid': NULL},
        'ts_ms': 1789800000000::BIGINT,
        'ddl': 'CREATE TABLE `customers` (`id` bigint)',
        'tableChanges': [{
          'type': 'CREATE',
          'id': '\"shop\".\"customers\"',
          'table': {
            'primaryKeyColumnNames': ['id'],
            'columns': [{
              'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
              'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL
            }, {
              'name': 'amount', 'typeName': 'DECIMAL', 'position': 2, 'optional': true,
              'defaultValueExpression': '0.00', 'length': 12, 'scale': 2
            }]
          }
        }]
      }";

    fn statement(sql: &str) -> Result<DebeziumDdl<MySqlSource>, duckdb::Error> {
        let connection = Connection::open_in_memory().unwrap();
        connection.query_row(sql, [], |row| row.get::<_, DebeziumDdl<MySqlSource>>(0))
    }

    #[test]
    fn a_statement_carries_its_source_time_and_text() {
        let statement = statement(CUSTOMERS).unwrap();

        assert_eq!(statement.source.file, "binlog.000002");
        assert_eq!(statement.source.pos, 448);
        assert_eq!(statement.source.gtid, None);
        assert_eq!(statement.ts_ms, 1789800000000);
        assert_eq!(statement.ddl, "CREATE TABLE `customers` (`id` bigint)");
    }

    #[test]
    fn a_created_table_carries_its_columns_and_key() {
        let statement = statement(CUSTOMERS).unwrap();

        let [change] = statement.table_changes.as_slice() else {
            panic!("expected one table change");
        };
        assert_eq!(change.r#type, "CREATE");
        assert_eq!(change.id, "\"shop\".\"customers\"");

        let table = change.table.as_ref().unwrap();
        assert_eq!(table.primary_key_column_names, ["id"]);
        assert_eq!(table.columns[1].type_name, "DECIMAL");
        assert_eq!(table.columns[1].length, Some(12));
        assert_eq!(table.columns[1].scale, Some(2));
        assert_eq!(
            table.columns[1].default_value_expression.as_deref(),
            Some("0.00")
        );
        assert!(table.columns[1].optional);
    }

    #[test]
    fn a_column_carries_the_comment_the_sink_wrote() {
        let statement = statement(
            "SELECT {
               'source': {'file': 'binlog.000002', 'pos': 448, 'gtid': NULL},
               'ts_ms': 1789800000000::BIGINT,
               'ddl': 'CREATE TABLE `customers` (`id` bigint COMMENT ''who they are'')',
               'tableChanges': [{
                 'type': 'CREATE',
                 'id': '\"shop\".\"customers\"',
                 'table': {
                   'primaryKeyColumnNames': ['id'],
                   'columns': [{
                     'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                     'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL,
                     'comment': 'who they are'
                   }]
                 }
               }]
             }",
        )
        .unwrap();

        let table = statement.table_changes[0].table.as_ref().unwrap();
        assert_eq!(table.columns[0].comment.as_deref(), Some("who they are"));
    }

    #[test]
    fn a_column_the_sink_wrote_without_a_comment_is_read_without_one() {
        let statement = statement(CUSTOMERS).unwrap();

        let table = statement.table_changes[0].table.as_ref().unwrap();
        assert_eq!(table.columns[0].comment, None);
    }

    #[test]
    fn a_statement_carries_the_snapshot_mark_of_its_source() {
        let statement = statement(
            "SELECT {
               'source': {'file': 'binlog.000002', 'pos': 198, 'gtid': NULL, 'snapshot': 'true'},
               'ts_ms': 1789800000000::BIGINT,
               'ddl': 'CREATE TABLE `customers` (`id` bigint)',
               'tableChanges': []::STRUCT(type VARCHAR)[]
             }",
        )
        .unwrap();

        assert_eq!(statement.source.snapshot.as_deref(), Some("true"));
    }

    #[test]
    fn a_statement_whose_source_carries_no_snapshot_mark_is_read_without_one() {
        assert_eq!(statement(CUSTOMERS).unwrap().source.snapshot, None);
    }

    #[test]
    fn a_dropped_table_carries_no_table() {
        let statement = statement(
            "SELECT {
               'source': {'file': 'binlog.000002', 'pos': 198, 'gtid': NULL},
               'ts_ms': 1789800000000::BIGINT,
               'ddl': 'DROP TABLE `customers`',
               'tableChanges': [{'type': 'DROP', 'id': '\"shop\".\"customers\"', 'table': NULL}]
             }",
        )
        .unwrap();

        assert_eq!(statement.table_changes[0].r#type, "DROP");
        assert!(statement.table_changes[0].table.is_none());
    }

    #[test]
    fn a_postgres_statement_is_positioned_by_its_log_sequence_number() {
        let connection = Connection::open_in_memory().unwrap();
        let statement: DebeziumDdl<PostgresSource> = connection
            .query_row(
                "SELECT {
                   'source': {'lsn': 26738384::BIGINT},
                   'ts_ms': 1789800000000::BIGINT,
                   'ddl': '',
                   'tableChanges': []::STRUCT(type VARCHAR)[]
                 }",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(statement.source.lsn, 26738384);
    }

    #[test]
    fn a_column_the_change_log_does_not_shape_like_a_statement_is_rejected() {
        assert!(statement("SELECT 'not a statement'").is_err());
    }
}
