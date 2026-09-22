pub mod bigquery;
pub mod duckdb;
#[cfg(all(feature = "bigquery", feature = "duckdb"))]
pub mod reader;
