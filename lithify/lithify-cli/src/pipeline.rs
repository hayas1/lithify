use serde::{Deserialize, Serialize};

use lithify_core::sink::{Namespace, OpenWarehouse, SinkWarehouse, WarehouseFn};
use lithify_core::source::CdcFormat;
use lithify_protocol::sink::bigquery::client::{BigQueryCapture, BigQueryChangeLogsFn};
use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbChangeLogsFn};
use lithify_protocol::{BigQueryWarehouse, DebeziumFormat, DuckDbWarehouse};

use crate::credential::Credentials;
use crate::unrunnable::Unrunnable;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum CaptureConfig {
    Debezium(<DebeziumFormat as CdcFormat>::Config),
}

impl CaptureConfig {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

impl DuckDbCapture for CaptureConfig {
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

impl BigQueryCapture for CaptureConfig {
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
pub enum WarehouseConfig {
    DuckDb(<DuckDbWarehouse as OpenWarehouse>::Config),
    BigQuery(<BigQueryWarehouse as OpenWarehouse>::Config),
}

impl WarehouseConfig {
    pub fn reach<'a>(&'a self, location: &'a LocationConfig) -> Option<Reached<'a>> {
        match (self, location) {
            (Self::DuckDb(config), LocationConfig::DuckDb(location)) => {
                Some(Reached::DuckDb(config, location))
            }
            (Self::BigQuery(config), LocationConfig::BigQuery(location)) => {
                Some(Reached::BigQuery(config, location))
            }
            _ => None,
        }
    }
}

impl SinkWarehouse for WarehouseConfig {
    type Location = LocationConfig;
    type Credential = Credentials;
    type Error = Unrunnable;

    fn reaches(&self, location: &LocationConfig) -> bool {
        self.reach(location).is_some()
    }

    fn with_client<F: WarehouseFn>(
        &self,
        change_log: &LocationConfig,
        credentials: &Credentials,
        f: F,
    ) -> Result<F::Output, Unrunnable> {
        match self.reach(change_log).ok_or(Unrunnable::Unreached)? {
            Reached::DuckDb(config, change_log) => Ok(f.call(&DuckDbWarehouse::open(
                config,
                credentials.duckdb()?,
                change_log,
            )?)),
            Reached::BigQuery(config, change_log) => {
                Ok(f.call(&BigQueryWarehouse::open(config, &(), change_log)?))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum LocationConfig {
    DuckDb(<DuckDbWarehouse as OpenWarehouse>::Location),
    BigQuery(<BigQueryWarehouse as OpenWarehouse>::Location),
}

impl From<&LocationConfig> for Namespace {
    fn from(location: &LocationConfig) -> Self {
        match location {
            LocationConfig::DuckDb(location) => Namespace::from(location),
            LocationConfig::BigQuery(location) => Namespace::from(location),
        }
    }
}

pub enum Reached<'a> {
    DuckDb(
        &'a <DuckDbWarehouse as OpenWarehouse>::Config,
        &'a <DuckDbWarehouse as OpenWarehouse>::Location,
    ),
    BigQuery(
        &'a <BigQueryWarehouse as OpenWarehouse>::Config,
        &'a <BigQueryWarehouse as OpenWarehouse>::Location,
    ),
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
        let capture: CaptureConfig = toml::from_str("[debezium]\ndialect = \"mysql\"").unwrap();

        let CaptureConfig::Debezium(debezium) = capture;
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

        let capture: CaptureConfig = toml::from_str("[debezium]\ndialect = \"postgres\"").unwrap();

        assert_eq!(
            capture.with_duckdb_change_logs("postgres", Probe),
            std::any::type_name::<Debezium<Postgres>>()
        );
    }

    #[test]
    fn a_format_this_program_does_not_read_is_rejected() {
        let unknown = toml::from_str::<CaptureConfig>("[maxwell]\ndialect = \"mysql\"");

        assert!(unknown.is_err());
    }

    #[test]
    fn a_pipeline_is_sunk_into_a_warehouse_this_program_reads() {
        let warehouse: WarehouseConfig =
            toml::from_str("[bigquery]\nproject = \"local-project\"").unwrap();

        let WarehouseConfig::BigQuery(bigquery) = warehouse else {
            panic!("expected a bigquery warehouse");
        };
        assert_eq!(bigquery.project(), "local-project");
    }

    #[test]
    fn a_warehouse_reaches_a_location_written_the_way_it_is() {
        let bigquery: WarehouseConfig =
            toml::from_str("[bigquery]\nproject = \"local-project\"").unwrap();
        let dataset: LocationConfig = toml::from_str("bigquery.dataset = \"mysql_shop\"").unwrap();
        let prefix: LocationConfig = toml::from_str("duckdb.prefix = \"mysql_shop\"").unwrap();

        assert!(bigquery.reaches(&dataset));
        assert!(!bigquery.reaches(&prefix));
    }

    #[test]
    fn a_warehouse_this_program_does_not_read_is_rejected() {
        let unknown = toml::from_str::<WarehouseConfig>("[redshift]\ncluster = \"shop\"");

        assert!(unknown.is_err());
    }
}
