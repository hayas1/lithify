use serde::Deserialize;

use event_replay_core::sink;
use event_replay_core::source::CdcFormat;
use event_replay_protocol::{BigQueryWarehouse, DebeziumFormat, DuckDbWarehouse};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum Capture {
    Debezium(<DebeziumFormat as CdcFormat>::Config),
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
    use event_replay_protocol::source::debezium::config::Dialect;

    #[test]
    fn a_source_is_captured_by_a_format_this_program_reads() {
        let capture: Capture = toml::from_str("[debezium]\ndialect = \"mysql\"").unwrap();

        let Capture::Debezium(debezium) = capture;
        assert_eq!(debezium.dialect(), Dialect::MySql);
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
