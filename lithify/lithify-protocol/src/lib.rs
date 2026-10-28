// No future here is asked to be Send: a DuckDB connection cannot be shared across threads.
#![allow(async_fn_in_trait)]

pub mod sink;
pub mod source;

pub use sink::bigquery::BigQueryWarehouse;
pub use sink::duckdb::DuckDbWarehouse;
pub use source::debezium::mysql::{MySql, MySqlPosition, MySqlSource};
pub use source::debezium::postgres::{Postgres, PostgresPosition, PostgresSource};
pub use source::debezium::{Debezium, DebeziumFormat, Dialect};
