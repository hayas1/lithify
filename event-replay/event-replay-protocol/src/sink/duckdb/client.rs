use duckdb::types::{FromSql, Type};
use duckdb::{Connection, Error, Statement};

use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::source::{
    RowChangeLog, SchemaChangeLog, SchemaChangeStream, SchemaChanges, Streamed,
};
use event_replay_core::window::Window;

use crate::sink::duckdb::DuckDbWarehouse;

pub trait DuckDbLogFn {
    type Output;

    fn call_duckdb<L>(self, log: L) -> Self::Output
    where
        L: SchemaChangeLog + RowChangeLog,
        L::Record: FromSql;
}

pub trait DuckDbCapture {
    fn with_duckdb_log<F: DuckDbLogFn>(&self, f: F) -> F::Output;
}

pub struct DuckDbClient {
    connection: Connection,
}

impl DuckDbClient {
    pub fn new(connection: Connection) -> Self {
        Self { connection }
    }
}

pub struct DuckDbSchemaChanges<'a, L> {
    statement: Statement<'a>,
    log: &'a L,
}

impl<L> SchemaChanges<L> for DuckDbSchemaChanges<'_, L>
where
    L: SchemaChangeLog,
    L::Record: FromSql,
{
    type Error = Error;

    fn stream(
        &mut self,
    ) -> Result<impl Iterator<Item = Streamed<L::Position, Self::Error>>, Self::Error> {
        let log = self.log;

        Ok(self
            .statement
            .query_map([], |row| row.get::<_, L::Record>(0))?
            .map(move |statement| {
                log.schema_change(statement?).map_err(|malformed| {
                    Error::FromSqlConversionFailure(0, Type::Any, Box::new(malformed))
                })
            }))
    }
}

impl<L> SchemaChangeStream<L> for DuckDbClient
where
    L: SchemaChangeLog,
    L::Record: FromSql,
{
    type Error = Error;
    type Changes<'a>
        = DuckDbSchemaChanges<'a, L>
    where
        L: 'a;

    fn schema_changes<'a>(
        &'a self,
        log: &'a L,
        table: &TablePath,
        window: &Window,
    ) -> Result<Self::Changes<'a>, Self::Error> {
        Ok(DuckDbSchemaChanges {
            statement: self
                .connection
                .prepare(&DuckDbWarehouse.select_schema_changes(log, table, window))?,
            log,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::debezium::Debezium;
    use crate::source::debezium::mysql::MySql;
    use event_replay_core::event::{MalformedEvent, SchemaChange, TableChange};
    use event_replay_core::position::Position;
    use event_replay_core::position::RecordedAt;
    use event_replay_core::source::ColumnPath;

    const CHANGE_LOG: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS
        SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000002', 'pos': 198, 'gtid': NULL},
            'ts_ms': 1789800000000::BIGINT,
            'ddl': 'DROP TABLE `customers`',
            'tableChanges': [{'type': 'DROP', 'id': '\"shop\".\"customers\"', 'table': NULL}]}),
          ({'source': {'file': 'binlog.000002', 'pos': 448, 'gtid': NULL},
            'ts_ms': 1789800000001::BIGINT,
            'ddl': 'CREATE TABLE `customers` (`id` bigint)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"customers\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000003', 'pos': 4, 'gtid': NULL},
            'ts_ms': 1789900000000::BIGINT,
            'ddl': 'DROP TABLE `orders`',
            'tableChanges': [{'type': 'DROP', 'id': '\"shop\".\"orders\"', 'table': NULL}]})
        ) AS t(value)";

    fn table() -> TablePath {
        TablePath::new("mysql_shop_cdc", "mysql")
    }

    fn window(from: i64, to: i64) -> Window {
        Window::new(RecordedAt::from(from), RecordedAt::from(to))
    }

    fn read(window: &Window) -> Vec<String> {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(CHANGE_LOG).unwrap();
        let client = DuckDbClient::new(connection);
        let log = Debezium::under("value", MySql);
        let mut changes = client.schema_changes(&log, &table(), window).unwrap();

        changes
            .stream()
            .unwrap()
            .map(|change| {
                let change = change.unwrap();

                format!(
                    "{} {:?}",
                    change.statement(),
                    change
                        .tables()
                        .map(|table| match table {
                            TableChange::Created(table) => format!("created {}", table.id()),
                            TableChange::Altered(table) => format!("altered {}", table.id()),
                            TableChange::Dropped(id) => format!("dropped {id}"),
                        })
                        .collect::<Vec<_>>()
                )
            })
            .collect()
    }

    #[test]
    fn the_schema_changes_of_a_window_arrive_in_the_order_the_source_logged_them() {
        let statements = read(&window(0, 1789900000001));

        assert_eq!(
            statements,
            [
                "DROP TABLE `customers` [\"dropped shop.customers\"]",
                "CREATE TABLE `customers` (`id` bigint) [\"created shop.customers\"]",
                "DROP TABLE `orders` [\"dropped shop.orders\"]",
            ]
        );
    }

    #[test]
    fn a_window_holds_the_statements_recorded_within_it() {
        let statements = read(&window(1789800000001, 1789900000000));

        assert_eq!(
            statements,
            ["CREATE TABLE `customers` (`id` bigint) [\"created shop.customers\"]"]
        );
    }

    #[test]
    fn a_window_without_a_statement_reads_nothing() {
        assert!(read(&window(0, 1789800000000)).is_empty());
    }

    #[test]
    fn a_table_that_does_not_hold_a_change_log_is_rejected() {
        let connection = Connection::open_in_memory().unwrap();
        let client = DuckDbClient::new(connection);
        let log = Debezium::under("value", MySql);

        let changes = client.schema_changes(&log, &table(), &window(0, 1));

        assert!(changes.is_err());
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd)]
    struct Line(i64);

    impl Position for Line {}

    struct Ledger;

    impl SchemaChangeLog for Ledger {
        type Position = Line;
        type Record = String;

        fn envelope(&self) -> Option<ColumnPath> {
            None
        }

        fn recorded_at(&self) -> ColumnPath {
            ColumnPath::from("written")
        }

        fn ordering(&self) -> Vec<ColumnPath> {
            vec![ColumnPath::from("line")]
        }

        fn schema_change(
            &self,
            statement: Self::Record,
        ) -> Result<SchemaChange<Self::Position>, MalformedEvent> {
            Ok(SchemaChange::new(
                Line(0),
                RecordedAt::from(0),
                &statement,
                Vec::new(),
            ))
        }
    }

    #[test]
    fn a_change_log_of_any_format_is_streamed_by_the_record_it_reads() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE SCHEMA ledger;
                 CREATE TABLE ledger.statements AS SELECT * FROM (VALUES
                   ('DROP TABLE b', 2, 2),
                   ('CREATE TABLE a (id bigint)', 1, 1)
                 ) AS t(statement, line, written)",
            )
            .unwrap();
        let client = DuckDbClient::new(connection);
        let mut changes = client
            .schema_changes(
                &Ledger,
                &TablePath::new("ledger", "statements"),
                &window(0, 3),
            )
            .unwrap();

        let statements: Vec<String> = changes
            .stream()
            .unwrap()
            .map(|change| change.unwrap().statement().to_owned())
            .collect();

        assert_eq!(statements, ["CREATE TABLE a (id bigint)", "DROP TABLE b"]);
    }
}
