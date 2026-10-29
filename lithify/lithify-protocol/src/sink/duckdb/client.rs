use duckdb::types::{FromSql, Type};
use duckdb::{Connection, Error, Statement};
use lithify_pipeline::warehouse::WarehouseControl;

use lithify_core::sink::{ChangeLogSql, Namespace, TablePath, WarehouseClient, WarehouseSql};
use lithify_core::source::{
    ChangeLogs, SchemaChangeLog, SchemaChangeStream, SchemaChanges, Streamed,
};
use lithify_core::sql::SqlString;
use lithify_core::window::Window;
use lithify_pipeline::build_log::{BuildLogQuery, BuildLogRow};

use crate::sink::duckdb::DuckDbWarehouse;
use crate::sink::execution::{Apply, DryRun};

pub trait DuckDbChangeLogsFn {
    type Output;

    async fn call_duckdb<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: FromSql;
}

pub trait DuckDbCapture {
    async fn with_duckdb_change_logs<F: DuckDbChangeLogsFn>(
        &self,
        topic_prefix: &str,
        f: F,
    ) -> F::Output;
}

// DuckDB answers on the calling thread, so executing a statement waits on nothing.
pub trait DuckDbExecution {
    fn execute(&self, connection: &Connection, statement: &SqlString) -> Result<(), Error>;
}

impl DuckDbExecution for Apply {
    fn execute(&self, connection: &Connection, statement: &SqlString) -> Result<(), Error> {
        connection.execute_batch(statement.as_str())
    }
}

impl DuckDbExecution for DryRun {
    fn execute(&self, _: &Connection, statement: &SqlString) -> Result<(), Error> {
        self.record(statement);

        Ok(())
    }
}

pub struct DuckDbClient<R = Apply> {
    connection: Connection,
    warehouse: DuckDbWarehouse,
    execution: R,
}

impl DuckDbClient {
    pub fn new(connection: Connection, warehouse: DuckDbWarehouse) -> Self {
        Self {
            connection,
            warehouse,
            execution: Apply,
        }
    }
}

impl<R> DuckDbClient<R> {
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    pub fn execution(&self) -> &R {
        &self.execution
    }

    pub fn executing<S>(self, execution: S) -> DuckDbClient<S> {
        DuckDbClient {
            connection: self.connection,
            warehouse: self.warehouse,
            execution,
        }
    }
}

impl<R: DuckDbExecution> WarehouseClient for DuckDbClient<R> {
    type Warehouse = DuckDbWarehouse;
    type Error = Error;

    fn warehouse(&self) -> &Self::Warehouse {
        &self.warehouse
    }

    async fn execute(&self, statement: &SqlString) -> Result<(), Self::Error> {
        self.execution.execute(&self.connection, statement)
    }

    async fn exists(&self, table: &TablePath) -> Result<bool, Self::Error> {
        let count: i64 = self.connection.query_row(
            self.warehouse.select_existence(table).as_str(),
            [],
            |row| row.get(0),
        )?;

        Ok(count > 0)
    }
}

impl<R: DuckDbExecution> WarehouseControl for DuckDbClient<R> {
    async fn build_log_rows(
        &self,
        namespace: &Namespace,
        query: &BuildLogQuery,
    ) -> Result<Vec<BuildLogRow>, Self::Error> {
        self.connection
            .prepare(query.select(&self.warehouse, namespace).as_str())?
            .query_map([], |row| {
                Ok(BuildLogRow {
                    run_id: row.get("run_id")?,
                    started_at: row.get("started_at")?,
                    artifact: row.get("artifact")?,
                    schedule: row.get("schedule")?,
                    source: row.get("source")?,
                    capture: row.get("capture")?,
                    table_from: row.get("table_from")?,
                    table_to: row.get("table_to")?,
                    lifecycle: row.get("lifecycle")?,
                    base: row.get("base")?,
                    target: row.get("target")?,
                    window_from: row.get("window_from")?,
                    window_to: row.get("window_to")?,
                    status: row.get("status")?,
                    finished_at: row.get("finished_at")?,
                    error: row.get("error")?,
                    version: row.get("version")?,
                })
            })?
            .collect()
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

impl<L, R> SchemaChangeStream<L> for DuckDbClient<R>
where
    L: SchemaChangeLog,
    L::Record: FromSql,
{
    type Error = Error;
    type Changes<'a>
        = DuckDbSchemaChanges<'a, L>
    where
        L: 'a,
        R: 'a;

    async fn schema_changes<'a>(
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
    use crate::source::debezium::tests::described;
    use chrono::DateTime;
    use lithify_core::artifact::UnbuildableArtifact;
    use lithify_core::event::{MalformedEvent, SchemaChange};
    use lithify_core::listener::OutOfOrder;
    use lithify_core::model::TableId;
    use lithify_core::position::{Coordinate, Position};
    use lithify_core::sink::TablePath;
    use lithify_core::source::{ChangeLog, ColumnPath};
    use lithify_core::ts::Ts;
    use lithify_pipeline::build_log::Latest;
    use lithify_pipeline::build_log::{
        BuildLog, Run, Series, Status, TableBuilt, UnreadableBuildLog,
    };
    use ulid::Ulid;

    const CHANGE_LOG: &str = include_str!("../../../testdata/sink/duckdb/client/change_log.sql");

    fn window(from: i64, to: i64) -> Window {
        Window::new(Ts::try_from(from).unwrap(), Ts::try_from(to).unwrap())
    }

    async fn read(window: &Window) -> Vec<String> {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(CHANGE_LOG).unwrap();
        let client = DuckDbClient::new(connection, DuckDbWarehouse::new("mysql_shop_cdc"));
        let log = Debezium::under("value", "mysql", MySql);
        let mut changes = client.schema_changes(&log, window).await.unwrap();

        changes
            .stream()
            .unwrap()
            .map(|change| {
                let change = change.unwrap();

                format!(
                    "{} {:?}",
                    change.statement(),
                    change.tables().map(described).collect::<Vec<_>>()
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn the_schema_changes_of_a_window_arrive_in_the_order_the_source_logged_them() {
        let statements = read(&window(0, 1789900000001)).await;

        assert_eq!(
            statements,
            [
                "DROP TABLE `customers` [\"dropped shop.customers\"]",
                "CREATE TABLE `customers` (`id` bigint) [\"created shop.customers\"]",
                "DROP TABLE `orders` [\"dropped shop.orders\"]",
            ]
        );
    }

    #[tokio::test]
    async fn a_window_holds_the_statements_whose_event_time_falls_within_it() {
        let statements = read(&window(1789800000001, 1789900000000)).await;

        assert_eq!(
            statements,
            ["CREATE TABLE `customers` (`id` bigint) [\"created shop.customers\"]"]
        );
    }

    #[tokio::test]
    async fn a_table_is_modelled_as_the_schema_changes_of_a_window_left_it() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(CHANGE_LOG).unwrap();
        let client = DuckDbClient::new(connection, DuckDbWarehouse::new("mysql_shop_cdc"));
        let log = Debezium::under("value", "mysql", MySql);

        let customers = client
            .table::<Unread>(
                &log,
                &TableId::new("shop", "customers"),
                Ts::try_from(1789900000001).unwrap(),
            )
            .await
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

    #[tokio::test]
    async fn a_catalog_at_an_instant_holds_the_tables_the_schema_changes_before_it_left() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(CHANGE_LOG).unwrap();
        let client = DuckDbClient::new(connection, DuckDbWarehouse::new("mysql_shop_cdc"));
        let log = Debezium::under("value", "mysql", MySql);
        let tables = async |at: i64| {
            client
                .catalog_at::<Unread>(&log, Ts::try_from(at).unwrap())
                .await
                .unwrap()
                .tables()
                .map(|table| table.id().to_string())
                .collect::<Vec<_>>()
        };

        assert!(tables(1789800000001).await.is_empty());
        assert_eq!(tables(1789800000002).await, ["shop.customers"]);
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

    #[tokio::test]
    async fn a_client_runs_statements_on_its_connection() {
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new("mysql_shop_cdc"),
        );

        client
            .execute(&SqlString::from("CREATE TABLE people AS SELECT 1 AS id"))
            .await
            .unwrap();

        let id: i64 = client
            .connection()
            .query_row("SELECT id FROM people", [], |row| row.get(0))
            .unwrap();
        assert_eq!(id, 1);
    }

    const W1: (i64, i64) = (1789862400000, 1789948800000);
    const W2: (i64, i64) = (1789948800000, 1790035200000);
    const W3: (i64, i64) = (1790035200000, 1790121600000);

    fn logged(table: &str, window: (i64, i64), status: Status, finished_at: u64) -> BuildLog {
        logged_across(Some(table), Some(table), window, status, finished_at)
    }

    fn logged_across(
        table_from: Option<&str>,
        table_to: Option<&str>,
        (from, to): (i64, i64),
        status: Status,
        finished_at: u64,
    ) -> BuildLog {
        let table = table_to.or(table_from).unwrap();

        TableBuilt::new(
            table_from.map(|table| TableId::new("shop", table)),
            table_to.map(|table| TableId::new("shop", table)),
            Some(TablePath::new("mysql_shop", &format!("{table}_base"))),
            table_to.map(|_| TablePath::new("mysql_shop", &format!("{table}_{from}"))),
            window(from, to),
            status,
            Ts::from(DateTime::from_timestamp_millis(finished_at as i64).unwrap()),
        )
        .logged(&Run::new(
            Ulid::from_parts(finished_at, 0),
            Ts::from(DateTime::UNIX_EPOCH),
            &"snapshot".into(),
            "daily",
            "primary",
            r#"{"debezium":{"dialect":"mysql"}}"#,
            "0.1.0",
        ))
    }

    async fn logging(logs: &[BuildLog]) -> DuckDbClient {
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new("mysql_shop_cdc"),
        );
        client
            .create_build_log::<Logged>(&Namespace::from("mysql_shop"))
            .await
            .unwrap();
        client
            .append_build_logs(&Namespace::from("mysql_shop"), logs)
            .await
            .unwrap();

        client
    }

    async fn latest(client: &DuckDbClient, latest: Latest) -> Vec<BuildLog> {
        client
            .build_logs::<Logged>(&Namespace::from("mysql_shop"), &daily(), &latest.into())
            .await
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

    impl From<UnreadableBuildLog> for Logged {
        fn from(_: UnreadableBuildLog) -> Self {
            Self
        }
    }

    fn daily() -> Series {
        Series::new("snapshot".into(), "daily")
    }

    fn history() -> Vec<BuildLog> {
        vec![
            logged(
                "customers",
                W1,
                Status::Failed("no era".to_owned()),
                1789949000000,
            ),
            logged("orders", W1, Status::Succeeded, 1789949100000),
            logged("customers", W1, Status::Succeeded, 1789949200000),
            logged("customers", W2, Status::Succeeded, 1790035300000),
            logged(
                "orders",
                W2,
                Status::Failed("it's gone".to_owned()),
                1790035400000,
            ),
        ]
    }

    #[tokio::test]
    async fn a_build_log_is_created_once_however_often_it_is_asked_for() {
        let client = logging(&history()[..1]).await;

        client
            .create_build_log::<Logged>(&Namespace::from("mysql_shop"))
            .await
            .unwrap();

        assert_eq!(latest(&client, Latest::Attempted).await.len(), 1);
    }

    #[tokio::test]
    async fn a_build_log_reads_back_the_latest_record_of_each_table_as_it_was_written() {
        let history = history();

        assert_eq!(
            latest(&logging(&history).await, Latest::Attempted).await,
            [history[3].clone(), history[4].clone()]
        );
    }

    #[tokio::test]
    async fn a_build_log_reads_back_every_record_from_the_newest() {
        let history = history();

        assert_eq!(
            logging(&history)
                .await
                .build_logs::<Logged>(
                    &Namespace::from("mysql_shop"),
                    &daily(),
                    &BuildLogQuery::All
                )
                .await
                .unwrap(),
            history.into_iter().rev().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn a_build_log_reads_the_latest_success_of_each_table() {
        let history = history();

        assert_eq!(
            latest(&logging(&history).await, Latest::Succeeded).await,
            [history[3].clone(), history[1].clone()]
        );
    }

    #[tokio::test]
    async fn a_build_log_reads_the_latest_success_of_each_table_closing_at_an_instant() {
        let history = history();

        assert_eq!(
            latest(
                &logging(&history).await,
                Latest::SucceededUntil(Ts::try_from(W1.1).unwrap())
            )
            .await,
            [history[2].clone(), history[1].clone()]
        );
    }

    #[tokio::test]
    async fn a_build_log_reads_no_more_the_name_a_table_was_renamed_away_from() {
        let renamed = logged_across(
            Some("customers"),
            Some("clients"),
            W2,
            Status::Succeeded,
            1790035300000,
        );
        let client = logging(&[
            logged("customers", W1, Status::Succeeded, 1789949000000),
            renamed.clone(),
        ])
        .await;

        assert_eq!(
            latest(&client, Latest::Attempted).await,
            std::slice::from_ref(&renamed)
        );
        assert_eq!(latest(&client, Latest::Succeeded).await, [renamed]);
    }

    #[tokio::test]
    async fn a_build_log_reads_a_name_again_once_a_table_is_created_under_it() {
        let renamed = logged_across(
            Some("customers"),
            Some("clients"),
            W2,
            Status::Succeeded,
            1790035300000,
        );
        let created = logged_across(
            None,
            Some("customers"),
            W3,
            Status::Succeeded,
            1790121700000,
        );
        let client = logging(&[
            logged("customers", W1, Status::Succeeded, 1789949000000),
            renamed.clone(),
            created.clone(),
        ])
        .await;

        assert_eq!(latest(&client, Latest::Succeeded).await, [renamed, created]);
    }

    #[tokio::test]
    async fn a_build_log_reads_the_base_of_a_window_under_the_name_it_closed_with_even_once_renamed_away()
     {
        let before = logged("customers", W1, Status::Succeeded, 1789949000000);
        let client = logging(&[
            before.clone(),
            logged_across(
                Some("customers"),
                Some("clients"),
                W2,
                Status::Succeeded,
                1790035300000,
            ),
        ])
        .await;

        assert_eq!(
            latest(&client, Latest::SucceededUntil(Ts::try_from(W1.1).unwrap())).await,
            [before]
        );
    }

    #[tokio::test]
    async fn a_build_log_still_reads_the_last_success_of_a_name_a_failed_build_renamed_away_from() {
        let before = logged("customers", W1, Status::Succeeded, 1789949000000);
        let renamed = logged_across(
            Some("customers"),
            Some("clients"),
            W2,
            Status::Failed("no era".to_owned()),
            1790035300000,
        );
        let client = logging(&[before.clone(), renamed.clone()]).await;

        assert_eq!(latest(&client, Latest::Attempted).await, [renamed]);
        assert_eq!(latest(&client, Latest::Succeeded).await, [before]);
    }

    #[tokio::test]
    async fn a_build_log_reads_no_more_a_name_rebuilt_in_a_window_before_it_was_renamed_away_from()
    {
        let renamed = logged_across(
            Some("customers"),
            Some("clients"),
            W2,
            Status::Succeeded,
            1790035300000,
        );
        let client = logging(&[
            logged("customers", W1, Status::Succeeded, 1789949000000),
            renamed.clone(),
            logged("customers", W1, Status::Succeeded, 1790121700000),
        ])
        .await;

        assert_eq!(latest(&client, Latest::Succeeded).await, [renamed]);
    }

    #[tokio::test]
    async fn a_build_log_reads_nothing_of_a_table_once_dropped() {
        let client = logging(&[
            logged("customers", W1, Status::Succeeded, 1789949000000),
            logged_across(
                Some("customers"),
                None,
                W2,
                Status::Succeeded,
                1790035300000,
            ),
        ])
        .await;

        assert_eq!(latest(&client, Latest::Attempted).await, []);
        assert_eq!(latest(&client, Latest::Succeeded).await, []);
    }

    #[tokio::test]
    async fn a_client_tells_whether_a_table_stands_in_its_warehouse() {
        let client = logging(&[]).await;

        assert!(
            client
                .exists(&TablePath::new("mysql_shop", "_lithify_log"))
                .await
                .unwrap()
        );
        assert!(
            !client
                .exists(&TablePath::new("mysql_shop", "people_20260920"))
                .await
                .unwrap()
        );
        assert!(
            !client
                .exists(&TablePath::new("elsewhere", "_lithify_log"))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn a_window_without_a_statement_reads_nothing() {
        assert!(read(&window(0, 1789800000000)).await.is_empty());
    }

    #[tokio::test]
    async fn a_table_that_does_not_hold_a_change_log_is_rejected() {
        let connection = Connection::open_in_memory().unwrap();
        let client = DuckDbClient::new(connection, DuckDbWarehouse::new("mysql_shop_cdc"));
        let log = Debezium::under("value", "mysql", MySql);

        let changes = client.schema_changes(&log, &window(0, 1)).await;

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

        fn event_time(&self) -> ColumnPath {
            ColumnPath::from("event_time")
        }

        fn processing_time(&self) -> ColumnPath {
            ColumnPath::from("processing_time")
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
                Ts::try_from(0).unwrap(),
                &statement,
                Vec::new(),
                Vec::new(),
            ))
        }
    }

    #[tokio::test]
    async fn a_change_log_of_any_format_is_streamed_by_the_record_it_reads_in_the_order_of_its_processing_time()
     {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE SCHEMA ledger;
                 CREATE TABLE ledger.statements AS SELECT * FROM (VALUES
                   ('DROP TABLE b', 2, 1, 2),
                   ('CREATE TABLE a (id bigint)', 1, 2, 1),
                   ('DROP TABLE c', 3, 3, 0)
                 ) AS t(statement, line, event_time, processing_time)",
            )
            .unwrap();
        let client = DuckDbClient::new(connection, DuckDbWarehouse::new("ledger"));
        let mut changes = client.schema_changes(&Ledger, &window(0, 3)).await.unwrap();

        let statements: Vec<String> = changes
            .stream()
            .unwrap()
            .map(|change| change.unwrap().statement().to_owned())
            .collect();

        assert_eq!(statements, ["CREATE TABLE a (id bigint)", "DROP TABLE b"]);
    }
}
