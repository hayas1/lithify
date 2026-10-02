use serde::{Deserialize, Serialize};

#[cfg(feature = "bigquery")]
use crate::sink::bigquery::client::{BigQueryCapture, BigQueryLogFn};
#[cfg(feature = "duckdb")]
use crate::sink::duckdb::client::{DuckDbCapture, DuckDbLogFn};
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

#[cfg(feature = "duckdb")]
impl DuckDbCapture for Config {
    fn with_duckdb_log<F: DuckDbLogFn>(&self, f: F) -> F::Output {
        self.dialect.with_duckdb_log(f)
    }
}

#[cfg(feature = "bigquery")]
impl BigQueryCapture for Config {
    fn with_bigquery_log<F: BigQueryLogFn>(&self, f: F) -> F::Output {
        self.dialect.with_bigquery_log(f)
    }
}

impl Dialect {
    #[cfg(feature = "duckdb")]
    fn with_duckdb_log<F: DuckDbLogFn>(self, f: F) -> F::Output {
        match self {
            Self::MySql => f.call_duckdb(Debezium::under("value", MySql)),
            Self::Postgres => f.call_duckdb(Debezium::under("value", Postgres)),
        }
    }

    #[cfg(feature = "bigquery")]
    fn with_bigquery_log<F: BigQueryLogFn>(self, f: F) -> F::Output {
        match self {
            Self::MySql => f.call_bigquery(Debezium::new(MySql)),
            Self::Postgres => f.call_bigquery(Debezium::new(Postgres)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    use event_replay_core::source::{ColumnPath, RowChangeLog, SchemaChangeLog};

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

            fn call_duckdb<L>(self, log: L) -> Self::Output
            where
                L: SchemaChangeLog + RowChangeLog,
                L::Record: duckdb::types::FromSql,
            {
                (std::any::type_name::<L>(), log.envelope())
            }
        }

        assert_eq!(
            Config::of(Dialect::MySql).with_duckdb_log(Probe),
            (
                std::any::type_name::<Debezium<MySql>>(),
                Some(ColumnPath::from("value"))
            )
        );
        assert_eq!(
            Config::of(Dialect::Postgres).with_duckdb_log(Probe),
            (
                std::any::type_name::<Debezium<Postgres>>(),
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

            fn call_bigquery<L>(self, log: L) -> Self::Output
            where
                L: SchemaChangeLog + RowChangeLog,
                L::Record: TryFrom<
                        google_cloud_bigquery::query::Row,
                        Error = google_cloud_bigquery::error::RowError,
                    >,
            {
                (std::any::type_name::<L>(), log.envelope())
            }
        }

        assert_eq!(
            Config::of(Dialect::MySql).with_bigquery_log(Probe),
            (std::any::type_name::<Debezium<MySql>>(), None)
        );
        assert_eq!(
            Config::of(Dialect::Postgres).with_bigquery_log(Probe),
            (std::any::type_name::<Debezium<Postgres>>(), None)
        );
    }

    #[test]
    fn a_dialect_debezium_does_not_read_is_rejected() {
        let unknown = serde_json::from_value::<Config>(serde_json::json!({"dialect": "oracle"}));

        assert!(unknown.is_err());
    }
}
