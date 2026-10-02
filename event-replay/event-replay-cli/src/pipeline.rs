use serde::Deserialize;

use event_replay_core::sink;
use event_replay_core::source::CdcFormat;
use event_replay_protocol::sink::bigquery::client::BigQueryLogFn;
use event_replay_protocol::sink::duckdb::client::DuckDbLogFn;
use event_replay_protocol::{BigQueryWarehouse, DebeziumFormat, DuckDbWarehouse};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum Capture {
    Debezium(<DebeziumFormat as CdcFormat>::Config),
}

impl Capture {
    pub fn call_duckdb<F: DuckDbLogFn>(&self, f: F) -> F::Output {
        match self {
            Self::Debezium(debezium) => debezium.dialect().call_duckdb(f),
        }
    }

    pub fn call_bigquery<F: BigQueryLogFn>(&self, f: F) -> F::Output {
        match self {
            Self::Debezium(debezium) => debezium.dialect().call_bigquery(f),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum Warehouse {
    DuckDb(<DuckDbWarehouse as sink::Warehouse>::Config),
    BigQuery(<BigQueryWarehouse as sink::Warehouse>::Config),
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_replay_protocol::Postgres;
    use event_replay_protocol::source::debezium::config;
    use event_replay_protocol::source::debezium::{Debezium, DebeziumDdl, Dialect};

    #[test]
    fn a_source_is_captured_by_a_format_this_program_reads() {
        let capture: Capture = toml::from_str("[debezium]\ndialect = \"mysql\"").unwrap();

        let Capture::Debezium(debezium) = capture;
        assert_eq!(debezium.dialect(), config::Dialect::MySql);
    }

    #[test]
    fn a_capture_calls_a_log_fn_with_the_log_of_the_dialect_it_captures_in() {
        struct Probe;

        impl DuckDbLogFn for Probe {
            type Output = &'static str;

            fn call<D>(self, _: Debezium<D>) -> Self::Output
            where
                D: Dialect,
                DebeziumDdl<D::Source>: duckdb::types::FromSql,
            {
                std::any::type_name::<D>()
            }
        }

        let capture: Capture = toml::from_str("[debezium]\ndialect = \"postgres\"").unwrap();

        assert_eq!(
            capture.call_duckdb(Probe),
            std::any::type_name::<Postgres>()
        );
    }

    #[test]
    fn a_format_this_program_does_not_read_is_rejected() {
        let unknown = toml::from_str::<Capture>("[maxwell]\ndialect = \"mysql\"");

        assert!(unknown.is_err());
    }

    #[test]
    fn a_pipeline_is_sunk_into_a_warehouse_this_program_reads() {
        let warehouse: Warehouse =
            toml::from_str("[bigquery]\nproject = \"local-project\"").unwrap();

        let Warehouse::BigQuery(bigquery) = warehouse else {
            panic!("expected a bigquery warehouse");
        };
        assert_eq!(bigquery.project(), "local-project");
    }

    #[test]
    fn a_warehouse_this_program_does_not_read_is_rejected() {
        let unknown = toml::from_str::<Warehouse>("[redshift]\ncluster = \"shop\"");

        assert!(unknown.is_err());
    }
}
