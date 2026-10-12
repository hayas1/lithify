use serde::{Deserialize, Serialize};

use lithify_core::sink;
use lithify_core::source::CdcFormat;
use lithify_protocol::sink::bigquery::client::{BigQueryCapture, BigQueryChangeLogsFn};
use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbChangeLogsFn};
use lithify_protocol::{BigQueryWarehouse, DebeziumFormat, DuckDbWarehouse};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum Capture {
    Debezium(<DebeziumFormat as CdcFormat>::Config),
}

impl Capture {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

impl DuckDbCapture for Capture {
    fn with_duckdb_change_logs<F: DuckDbChangeLogsFn>(
        &self,
        topic_prefix: &str,
        f: F,
    ) -> F::Output {
        match self {
            Self::Debezium(debezium) => debezium.with_duckdb_change_logs(topic_prefix, f),
        }
    }
}

impl BigQueryCapture for Capture {
    fn with_bigquery_change_logs<F: BigQueryChangeLogsFn>(
        &self,
        topic_prefix: &str,
        f: F,
    ) -> F::Output {
        match self {
            Self::Debezium(debezium) => debezium.with_bigquery_change_logs(topic_prefix, f),
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
    use lithify_core::source::{ChangeLogs, SchemaChangeLog};
    use lithify_protocol::Postgres;
    use lithify_protocol::source::debezium::Debezium;
    use lithify_protocol::source::debezium::config;

    #[test]
    fn a_source_is_captured_by_a_format_this_program_reads() {
        let capture: Capture = toml::from_str("[debezium]\ndialect = \"mysql\"").unwrap();

        let Capture::Debezium(debezium) = capture;
        assert_eq!(debezium.dialect(), config::Dialect::MySql);
    }

    #[test]
    fn a_capture_calls_a_fn_with_the_logs_of_the_dialect_it_captures_in() {
        struct Probe;

        impl DuckDbChangeLogsFn for Probe {
            type Output = &'static str;

            fn call_duckdb<C>(self, _: C) -> Self::Output
            where
                C: ChangeLogs,
                <C::Schema as SchemaChangeLog>::Record: duckdb::types::FromSql,
            {
                std::any::type_name::<C>()
            }
        }

        let capture: Capture = toml::from_str("[debezium]\ndialect = \"postgres\"").unwrap();

        assert_eq!(
            capture.with_duckdb_change_logs("postgres", Probe),
            std::any::type_name::<Debezium<Postgres>>()
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
