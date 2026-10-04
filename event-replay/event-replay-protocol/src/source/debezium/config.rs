use serde::{Deserialize, Serialize};

#[cfg(feature = "bigquery")]
use crate::sink::bigquery::client::{BigQueryCapture, BigQueryChangeLogsFn};
#[cfg(feature = "duckdb")]
use crate::sink::duckdb::client::{DuckDbCapture, DuckDbChangeLogsFn};
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
    #[cfg(feature = "duckdb")]
    const DUCKDB_ENVELOPE: Option<&str> = Some("value");
    #[cfg(feature = "bigquery")]
    const BIGQUERY_ENVELOPE: Option<&str> = None;

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
    fn with_duckdb_change_logs<F: DuckDbChangeLogsFn>(&self, f: F) -> F::Output {
        self.dialect
            .with_duckdb_change_logs(Self::DUCKDB_ENVELOPE, f)
    }
}

#[cfg(feature = "bigquery")]
impl BigQueryCapture for Config {
    fn with_bigquery_change_logs<F: BigQueryChangeLogsFn>(&self, f: F) -> F::Output {
        self.dialect
            .with_bigquery_change_logs(Self::BIGQUERY_ENVELOPE, f)
    }
}

impl Dialect {
    #[cfg(feature = "duckdb")]
    fn with_duckdb_change_logs<F: DuckDbChangeLogsFn>(
        self,
        envelope: Option<&str>,
        f: F,
    ) -> F::Output {
        match self {
            Self::MySql => f.call_duckdb(Debezium::within(envelope, MySql)),
            Self::Postgres => f.call_duckdb(Debezium::within(envelope, Postgres)),
        }
    }

    #[cfg(feature = "bigquery")]
    fn with_bigquery_change_logs<F: BigQueryChangeLogsFn>(
        self,
        envelope: Option<&str>,
        f: F,
    ) -> F::Output {
        match self {
            Self::MySql => f.call_bigquery(Debezium::within(envelope, MySql)),
            Self::Postgres => f.call_bigquery(Debezium::within(envelope, Postgres)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    use event_replay_core::source::{ChangeLog, ChangeLogs, ColumnPath, SchemaChangeLog};

    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    type Probed = (&'static str, Option<ColumnPath>, Option<ColumnPath>);

    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    fn probe<C: ChangeLogs>(logs: C) -> Probed {
        (
            std::any::type_name::<C>(),
            logs.schema_changes().envelope(),
            logs.row_changes().envelope(),
        )
    }

    #[test]
    fn a_source_says_which_dialect_debezium_reads_it_as() {
        let config: Config =
            serde_json::from_value(serde_json::json!({"dialect": "postgres"})).unwrap();

        assert_eq!(config.dialect(), Dialect::Postgres);
    }

    #[cfg(feature = "duckdb")]
    #[test]
    fn a_duckdb_fn_is_called_with_the_logs_of_the_dialect_under_the_value_of_each_record() {
        struct Probe;

        impl DuckDbChangeLogsFn for Probe {
            type Output = Probed;

            fn call_duckdb<C>(self, logs: C) -> Self::Output
            where
                C: ChangeLogs,
                <C::Schema as SchemaChangeLog>::Record: duckdb::types::FromSql,
            {
                probe(logs)
            }
        }

        let value = Some(ColumnPath::from("value"));

        assert_eq!(
            Config::of(Dialect::MySql).with_duckdb_change_logs(Probe),
            (
                std::any::type_name::<Debezium<MySql>>(),
                value.clone(),
                value.clone()
            )
        );
        assert_eq!(
            Config::of(Dialect::Postgres).with_duckdb_change_logs(Probe),
            (
                std::any::type_name::<Debezium<Postgres>>(),
                value.clone(),
                value
            )
        );
    }

    #[cfg(feature = "bigquery")]
    #[test]
    fn a_bigquery_fn_is_called_with_the_logs_of_the_dialect_at_the_top_of_each_row() {
        struct Probe;

        impl BigQueryChangeLogsFn for Probe {
            type Output = Probed;

            fn call_bigquery<C>(self, logs: C) -> Self::Output
            where
                C: ChangeLogs,
                <C::Schema as SchemaChangeLog>::Record: TryFrom<
                        google_cloud_bigquery::query::Row,
                        Error = google_cloud_bigquery::error::RowError,
                    >,
            {
                probe(logs)
            }
        }

        assert_eq!(
            Config::of(Dialect::MySql).with_bigquery_change_logs(Probe),
            (std::any::type_name::<Debezium<MySql>>(), None, None)
        );
        assert_eq!(
            Config::of(Dialect::Postgres).with_bigquery_change_logs(Probe),
            (std::any::type_name::<Debezium<Postgres>>(), None, None)
        );
    }

    #[test]
    fn a_dialect_debezium_does_not_read_is_rejected() {
        let unknown = serde_json::from_value::<Config>(serde_json::json!({"dialect": "oracle"}));

        assert!(unknown.is_err());
    }
}
