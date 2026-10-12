use duckdb::types::{FromSql, Type};
use duckdb::{Connection, Error, Statement};

use lithify_core::sink::{Warehouse, WarehouseClient};
use lithify_core::source::{
    ChangeLogs, SchemaChangeLog, SchemaChangeStream, SchemaChanges, Streamed,
};
use lithify_core::sql::SqlString;
use lithify_core::window::Window;

use crate::sink::duckdb::DuckDbWarehouse;

pub trait DuckDbChangeLogsFn {
    type Output;

    fn call_duckdb<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: FromSql;
}

pub trait DuckDbCapture {
    fn with_duckdb_change_logs<F: DuckDbChangeLogsFn>(&self, topic_prefix: &str, f: F)
    -> F::Output;
}

pub struct DuckDbClient {
    connection: Connection,
    warehouse: DuckDbWarehouse,
}

impl DuckDbClient {
    pub fn new(connection: Connection, warehouse: DuckDbWarehouse) -> Self {
        Self {
            connection,
            warehouse,
        }
    }

    pub fn connection(&self) -> &Connection {
        &self.connection
    }
}

impl WarehouseClient for DuckDbClient {
    type Warehouse = DuckDbWarehouse;
    type Error = Error;

    fn warehouse(&self) -> &Self::Warehouse {
        &self.warehouse
    }

    fn execute(&self, statement: &SqlString) -> Result<(), Self::Error> {
        self.connection.execute_batch(statement.as_str())
    }

    fn next_run_id(&self, namespace: &str) -> Result<i64, Self::Error> {
        self.connection.query_row(
            self.warehouse.select_next_run_id(namespace).as_str(),
            [],
            |row| row.get(0),
        )
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
        window: &Window,
    ) -> Result<Self::Changes<'a>, Self::Error> {
        Ok(DuckDbSchemaChanges {
            statement: self
                .connection
                .prepare(self.warehouse.select_schema_changes(log, window).as_str())?,
            log,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::debezium::Debezium;
    use crate::source::debezium::mysql::MySql;
    use chrono::DateTime;
    use lithify_core::artifact::UnbuildableArtifact;
    use lithify_core::build_log::{BuildLog, Outcome, Run, Status};
    use lithify_core::event::{MalformedEvent, SchemaChange, TableChange};
    use lithify_core::listener::OutOfOrder;
    use lithify_core::model::TableId;
    use lithify_core::position::RecordedAt;
    use lithify_core::position::{Coordinate, Position};
    use lithify_core::sink::TablePath;
    use lithify_core::source::{ChangeLog, ColumnPath};

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

    fn window(from: i64, to: i64) -> Window {
        Window::new(RecordedAt::from(from), RecordedAt::from(to))
    }

    fn read(window: &Window) -> Vec<String> {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(CHANGE_LOG).unwrap();
        let client = DuckDbClient::new(connection, DuckDbWarehouse::new("mysql_shop_cdc"));
        let log = Debezium::under("value", "mysql", MySql);
        let mut changes = client.schema_changes(&log, window).unwrap();

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
                            TableChange::Renamed { from, to } => {
                                format!("renamed {from} to {to}")
                            }
                            TableChange::ColumnRenamed { table, from, to } => {
                                format!("renamed {table}.{from} to {to}")
                            }
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
    fn a_table_is_modelled_as_the_schema_changes_of_a_window_left_it() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(CHANGE_LOG).unwrap();
        let client = DuckDbClient::new(connection, DuckDbWarehouse::new("mysql_shop_cdc"));
        let log = Debezium::under("value", "mysql", MySql);

        let customers = client
            .table::<Unread>(
                &log,
                &TableId::new("shop", "customers"),
                RecordedAt::from(1789900000001),
            )
            .unwrap()
            .unwrap();

        assert_eq!(
            customers
                .columns()
                .map(|column| column.name())
                .collect::<Vec<_>>(),
            ["id"]
        );
    }

    #[test]
    fn a_catalog_at_an_instant_holds_the_tables_the_schema_changes_before_it_left() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(CHANGE_LOG).unwrap();
        let client = DuckDbClient::new(connection, DuckDbWarehouse::new("mysql_shop_cdc"));
        let log = Debezium::under("value", "mysql", MySql);
        let tables = |at: i64| {
            client
                .catalog_at::<Unread>(&log, RecordedAt::from(at))
                .unwrap()
                .tables()
                .map(|table| table.id().to_string())
                .collect::<Vec<_>>()
        };

        assert!(tables(1789800000001).is_empty());
        assert_eq!(tables(1789800000002), ["shop.customers"]);
    }

    #[derive(Debug)]
    struct Unread;

    impl From<Error> for Unread {
        fn from(_: Error) -> Self {
            Self
        }
    }

    impl From<OutOfOrder> for Unread {
        fn from(_: OutOfOrder) -> Self {
            Self
        }
    }

    #[test]
    fn a_client_runs_statements_on_its_connection() {
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new("mysql_shop_cdc"),
        );

        client
            .execute(&SqlString::from("CREATE TABLE people AS SELECT 1 AS id"))
            .unwrap();

        let id: i64 = client
            .connection()
            .query_row("SELECT id FROM people", [], |row| row.get(0))
            .unwrap();
        assert_eq!(id, 1);
    }

    fn logged(table: &str, run_id: i64, status: Status, finished_at: i64) -> BuildLog {
        BuildLog::new(
            &Run::new(
                run_id,
                "snapshot",
                "daily",
                "primary",
                r#"{"debezium":{"dialect":"mysql"}}"#,
                "0.1.0",
            ),
            TableId::new("shop", table),
            None,
            TablePath::new("mysql_shop", &format!("{table}_20260920")),
            window(1789862400000, 1789948800000),
            Outcome::new(
                status,
                DateTime::from_timestamp_millis(finished_at).unwrap(),
            ),
        )
    }

    fn logging(logs: &[BuildLog]) -> DuckDbClient {
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new("mysql_shop_cdc"),
        );
        client.create_build_log::<Logged>("mysql_shop").unwrap();
        for log in logs {
            client.append_build_log(log).unwrap();
        }

        client
    }

    fn latest(client: &DuckDbClient) -> Vec<(String, String, Option<String>)> {
        client
            .connection()
            .prepare(
                client
                    .warehouse()
                    .select_latest_build_logs("mysql_shop")
                    .as_str(),
            )
            .unwrap()
            .query_map([], |row| {
                Ok((row.get("table_id")?, row.get("status")?, row.get("error")?))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    #[derive(Debug)]
    struct Logged;

    impl From<Error> for Logged {
        fn from(_: Error) -> Self {
            Self
        }
    }

    impl From<UnbuildableArtifact> for Logged {
        fn from(_: UnbuildableArtifact) -> Self {
            Self
        }
    }

    fn next_run_id(client: &DuckDbClient) -> i64 {
        client.next_run_id("mysql_shop").unwrap()
    }

    #[test]
    fn a_build_log_is_created_once_however_often_it_is_asked_for() {
        let client = logging(&[logged("customers", 1, Status::Succeeded, 1789949000000)]);

        client.create_build_log::<Logged>("mysql_shop").unwrap();

        assert_eq!(latest(&client).len(), 1);
    }

    #[test]
    fn a_build_log_is_read_by_the_latest_record_of_each_table() {
        let client = logging(&[
            logged(
                "customers",
                1,
                Status::Failed("no era".to_owned()),
                1789949000000,
            ),
            logged("orders", 1, Status::Succeeded, 1789949100000),
            logged("customers", 2, Status::Succeeded, 1789949200000),
            logged(
                "orders",
                2,
                Status::Failed("it's gone".to_owned()),
                1789949300000,
            ),
        ]);

        assert_eq!(
            latest(&client),
            [
                ("shop.customers".to_owned(), "succeeded".to_owned(), None),
                (
                    "shop.orders".to_owned(),
                    "failed".to_owned(),
                    Some("it's gone".to_owned())
                ),
            ]
        );
    }

    #[test]
    fn a_build_log_keeps_the_instants_it_was_given() {
        let client = logging(&[logged("customers", 1, Status::Succeeded, 1789949000123)]);

        let instants: (i64, i64, i64) = client
            .connection()
            .query_row(
                "SELECT epoch_ms(window_from), epoch_ms(window_to), epoch_ms(finished_at) FROM mysql_shop._lithify_log",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(instants, (1789862400000, 1789948800000, 1789949000123));
    }

    #[test]
    fn a_run_is_numbered_after_the_last_one_the_build_log_holds() {
        assert_eq!(next_run_id(&logging(&[])), 1);
        assert_eq!(
            next_run_id(&logging(&[logged(
                "customers",
                3,
                Status::Succeeded,
                1789949000000
            )])),
            4
        );
    }

    #[test]
    fn a_window_without_a_statement_reads_nothing() {
        assert!(read(&window(0, 1789800000000)).is_empty());
    }

    #[test]
    fn a_table_that_does_not_hold_a_change_log_is_rejected() {
        let connection = Connection::open_in_memory().unwrap();
        let client = DuckDbClient::new(connection, DuckDbWarehouse::new("mysql_shop_cdc"));
        let log = Debezium::under("value", "mysql", MySql);

        let changes = client.schema_changes(&log, &window(0, 1));

        assert!(changes.is_err());
    }

    #[derive(Debug, Clone, PartialEq, PartialOrd)]
    struct Line(i64);

    impl Position for Line {}

    struct Ledger;

    impl ChangeLog for Ledger {
        type Position = Line;

        fn envelope(&self) -> Option<ColumnPath> {
            None
        }

        fn recorded_at(&self) -> ColumnPath {
            ColumnPath::from("written")
        }

        fn ordering(&self) -> Vec<ColumnPath> {
            vec![ColumnPath::from("line")]
        }

        fn coordinates(&self, position: &Line) -> Vec<Coordinate> {
            vec![Coordinate::Number(position.0)]
        }
    }

    impl SchemaChangeLog for Ledger {
        type Record = String;

        fn ddl_topic(&self) -> String {
            "statements".to_owned()
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
        let client = DuckDbClient::new(connection, DuckDbWarehouse::new("ledger"));
        let mut changes = client.schema_changes(&Ledger, &window(0, 3)).unwrap();

        let statements: Vec<String> = changes
            .stream()
            .unwrap()
            .map(|change| change.unwrap().statement().to_owned())
            .collect();

        assert_eq!(statements, ["CREATE TABLE a (id bigint)", "DROP TABLE b"]);
    }
}
