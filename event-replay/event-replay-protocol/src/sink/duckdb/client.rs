use duckdb::types::{FromSql, Type};
use duckdb::{Connection, Error, Statement};

use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::source::{SchemaChangeStream, SchemaChanges, Streamed};
use event_replay_core::window::Window;

use crate::sink::duckdb::DuckDbWarehouse;
use crate::source::debezium::{Debezium, DebeziumDdl, Dialect};

pub struct DuckDbClient {
    connection: Connection,
}

impl DuckDbClient {
    pub fn new(connection: Connection) -> Self {
        Self { connection }
    }
}

pub struct DuckDbSchemaChanges<'a, D> {
    statement: Statement<'a>,
    log: &'a Debezium<D>,
}

impl<D: Dialect> SchemaChanges<Debezium<D>> for DuckDbSchemaChanges<'_, D>
where
    DebeziumDdl<D::Source>: FromSql,
{
    type Error = Error;

    fn stream(
        &mut self,
    ) -> Result<impl Iterator<Item = Streamed<D::Position, Self::Error>>, Self::Error> {
        let log = self.log;

        Ok(self
            .statement
            .query_map([], |row| row.get::<_, DebeziumDdl<D::Source>>(0))?
            .map(move |statement| {
                log.schema_change(statement?).map_err(|malformed| {
                    Error::FromSqlConversionFailure(0, Type::Any, Box::new(malformed))
                })
            }))
    }
}

impl<D: Dialect> SchemaChangeStream<Debezium<D>> for DuckDbClient
where
    DebeziumDdl<D::Source>: FromSql,
{
    type Error = Error;
    type Changes<'a>
        = DuckDbSchemaChanges<'a, D>
    where
        D: 'a;

    fn schema_changes<'a>(
        &'a self,
        log: &'a Debezium<D>,
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
    use crate::source::debezium::mysql::MySql;
    use event_replay_core::event::TableChange;
    use event_replay_core::position::RecordedAt;

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
}
