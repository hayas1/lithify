use serde::{Deserialize, Serialize};

#[cfg(feature = "bigquery")]
use crate::sink::bigquery::client::BigQueryLogFn;
#[cfg(feature = "duckdb")]
use crate::sink::duckdb::client::DuckDbLogFn;
#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use crate::source::debezium::Debezium;
#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use crate::source::debezium::mysql::MySql;
#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use crate::source::debezium::postgres::Postgres;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    dialect: Dialect,
}

impl Config {
    pub fn of(dialect: Dialect) -> Self {
        Self { dialect }
    }

    pub fn dialect(&self) -> Dialect {
        self.dialect
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    MySql,
    Postgres,
}

impl Dialect {
    #[cfg(feature = "duckdb")]
    pub fn call_duckdb<F: DuckDbLogFn>(self, f: F) -> F::Output {
        match self {
            Self::MySql => f.call(Debezium::under("value", MySql)),
            Self::Postgres => f.call(Debezium::under("value", Postgres)),
        }
    }

    #[cfg(feature = "bigquery")]
    pub fn call_bigquery<F: BigQueryLogFn>(self, f: F) -> F::Output {
        match self {
            Self::MySql => f.call(Debezium::new(MySql)),
            Self::Postgres => f.call(Debezium::new(Postgres)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    use crate::source::debezium;
    #[cfg(feature = "duckdb")]
    use crate::source::debezium::DebeziumDdl;
    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    use event_replay_core::source::{ColumnPath, SchemaChangeLog};

    #[test]
    fn a_source_says_which_dialect_debezium_reads_it_as() {
        let config: Config =
            serde_json::from_value(serde_json::json!({"dialect": "postgres"})).unwrap();

        assert_eq!(config.dialect(), Dialect::Postgres);
    }

    #[cfg(feature = "duckdb")]
    #[test]
    fn a_duckdb_log_fn_is_called_with_the_log_of_the_dialect_under_the_value_of_each_record() {
        struct Probe;

        impl DuckDbLogFn for Probe {
            type Output = (&'static str, Option<ColumnPath>);

            fn call<D>(self, log: Debezium<D>) -> Self::Output
            where
                D: debezium::Dialect,
                DebeziumDdl<D::Source>: duckdb::types::FromSql,
            {
                (std::any::type_name::<D>(), log.envelope())
            }
        }

        assert_eq!(
            Dialect::MySql.call_duckdb(Probe),
            (
                std::any::type_name::<MySql>(),
                Some(ColumnPath::from("value"))
            )
        );
        assert_eq!(
            Dialect::Postgres.call_duckdb(Probe),
            (
                std::any::type_name::<Postgres>(),
                Some(ColumnPath::from("value"))
            )
        );
    }

    #[cfg(feature = "bigquery")]
    #[test]
    fn a_bigquery_log_fn_is_called_with_the_log_of_the_dialect_at_the_top_of_each_row() {
        struct Probe;

        impl BigQueryLogFn for Probe {
            type Output = (&'static str, Option<ColumnPath>);

            fn call<D>(self, log: Debezium<D>) -> Self::Output
            where
                D: debezium::Dialect,
                D::Source: google_cloud_bigquery::query::FromSql,
            {
                (std::any::type_name::<D>(), log.envelope())
            }
        }

        assert_eq!(
            Dialect::MySql.call_bigquery(Probe),
            (std::any::type_name::<MySql>(), None)
        );
        assert_eq!(
            Dialect::Postgres.call_bigquery(Probe),
            (std::any::type_name::<Postgres>(), None)
        );
    }

    #[test]
    fn a_dialect_debezium_does_not_read_is_rejected() {
        let unknown = serde_json::from_value::<Config>(serde_json::json!({"dialect": "oracle"}));

        assert!(unknown.is_err());
    }
}
