// No future here is asked to be Send: a DuckDB connection cannot be shared across threads.
#![allow(async_fn_in_trait)]

pub mod landing;
pub mod sink;
pub mod source;

pub use landing::{Landed, Landing};
pub use sink::bigquery::{BigQuerySink, BigQueryWarehouse};
pub use sink::duckdb::{DuckDbWarehouse, S3Sink};
pub use source::CdcFormat;
pub use source::debezium::mysql::{MySql, MySqlPosition, MySqlSource};
pub use source::debezium::postgres::{Postgres, PostgresPosition, PostgresSource};
pub use source::debezium::{Debezium, Dialect};
