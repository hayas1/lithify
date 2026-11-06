use serde::{Deserialize, Serialize};

use lithify_core::sink::{Namespace, OpenWarehouse, WarehouseClient};
use lithify_core::warehouse_source::SchemaChangeStream;

use lithify_pipeline::capture::{Capture, ChangeLogsFn};
use lithify_pipeline::warehouse::{SinkWarehouse, WarehouseFn};
use lithify_pipeline::{Placed, Places, Placing};
use lithify_protocol::sink::bigquery::client::{BigQueryClient, BigQueryExecution};
use lithify_protocol::sink::duckdb::client::{DuckDbClient, DuckDbExecution};
use lithify_protocol::source::debezium::Debezium;
use lithify_protocol::source::debezium::config as debezium;
use lithify_protocol::{
    BigQuerySink, BigQueryWarehouse, DuckDbWarehouse, Landed, Landing, MySql, Postgres, S3Sink,
};

use crate::credential::Credentials;
use crate::unrunnable::Unrunnable;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum CaptureConfig {
    Debezium(DebeziumConfig),
}

impl<X: DuckDbExecution> Capture<DuckDbClient<X>> for CaptureConfig {
    async fn with_change_logs<F: ChangeLogsFn<DuckDbClient<X>>>(
        &self,
        warehouse: &DuckDbWarehouse,
        f: F,
    ) -> F::Output {
        match self {
            Self::Debezium(debezium) => debezium.with_change_logs(S3Sink, warehouse, f).await,
        }
    }
}

impl<X: BigQueryExecution> Capture<BigQueryClient<X>> for CaptureConfig {
    async fn with_change_logs<F: ChangeLogsFn<BigQueryClient<X>>>(
        &self,
        warehouse: &BigQueryWarehouse,
        f: F,
    ) -> F::Output {
        match self {
            Self::Debezium(debezium) => debezium.with_change_logs(BigQuerySink, warehouse, f).await,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum DebeziumConfig {
    MySql(debezium::Config),
    Postgres(debezium::Config),
}

impl DebeziumConfig {
    async fn with_change_logs<K, L, F>(
        &self,
        landing: L,
        warehouse: &K::Warehouse,
        f: F,
    ) -> F::Output
    where
        K: WarehouseClient
            + SchemaChangeStream<Landed<Debezium<MySql>, L>, Error = <K as WarehouseClient>::Error>
            + SchemaChangeStream<Landed<Debezium<Postgres>, L>, Error = <K as WarehouseClient>::Error>,
        L: Landing<Debezium<MySql>, Warehouse = K::Warehouse>
            + Landing<Debezium<Postgres>, Warehouse = K::Warehouse>,
        F: ChangeLogsFn<K>,
    {
        match self {
            Self::MySql(config) => {
                f.call(Landed::new(config.debezium(MySql), landing, warehouse))
                    .await
            }
            Self::Postgres(config) => {
                f.call(Landed::new(config.debezium(Postgres), landing, warehouse))
                    .await
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum WarehouseConfig {
    DuckDb(
        Placed<
            <DuckDbWarehouse as OpenWarehouse>::Config,
            <DuckDbWarehouse as OpenWarehouse>::Location,
        >,
    ),
    BigQuery(
        Placed<
            <BigQueryWarehouse as OpenWarehouse>::Config,
            <BigQueryWarehouse as OpenWarehouse>::Location,
        >,
    ),
}

impl WarehouseConfig {
    pub fn reach<'a>(&'a self, location: &'a LocationConfig) -> Option<Reached<'a>> {
        match (self, location) {
            (Self::DuckDb(placed), LocationConfig::DuckDb(location)) => {
                Some(Reached::DuckDb(placed.warehouse(), location))
            }
            (Self::BigQuery(placed), LocationConfig::BigQuery(location)) => {
                Some(Reached::BigQuery(placed.warehouse(), location))
            }
            _ => None,
        }
    }
}

impl Placing for WarehouseConfig {
    fn change_log(&self) -> LocationConfig {
        match self {
            Self::DuckDb(placed) => LocationConfig::DuckDb(placed.change_log().clone()),
            Self::BigQuery(placed) => LocationConfig::BigQuery(placed.change_log().clone()),
        }
    }

    fn places(&self) -> Places<LocationConfig> {
        match self {
            Self::DuckDb(placed) => placed.places().clone().map(LocationConfig::DuckDb),
            Self::BigQuery(placed) => placed.places().clone().map(LocationConfig::BigQuery),
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

    async fn with_client<F: WarehouseFn>(
        &self,
        change_log: &LocationConfig,
        credentials: &Credentials,
        f: F,
    ) -> Result<F::Output, Unrunnable> {
        match self.reach(change_log).ok_or(Unrunnable::Unreached)? {
            Reached::DuckDb(config, change_log) => Ok(f
                .call(&DuckDbWarehouse::open(config, credentials.duckdb()?, change_log).await?)
                .await),
            Reached::BigQuery(config, change_log) => Ok(f
                .call(&BigQueryWarehouse::open(config, &(), change_log).await?)
                .await),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocationConfig {
    DuckDb(<DuckDbWarehouse as OpenWarehouse>::Location),
    BigQuery(<BigQueryWarehouse as OpenWarehouse>::Location),
}

impl From<LocationConfig> for Namespace {
    fn from(location: LocationConfig) -> Self {
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
    use std::path::Path;

    use lithify_core::sink::TablePath;
    use lithify_core::source::{ChangeLog, ColumnPath};
    use lithify_core::warehouse_source::{ChangeLogs, SchemaChangeLog};
    use lithify_pipeline::Pipeline;

    use crate::artifacts::ArtifactConfig;

    type Probed = (&'static str, TablePath, Option<ColumnPath>);

    struct Probe;

    impl<K: WarehouseClient> ChangeLogsFn<K> for Probe {
        type Output = Probed;

        async fn call<C>(self, logs: C) -> Self::Output
        where
            C: ChangeLogs,
            K: SchemaChangeStream<C::SchemaChangeLog, Error = <K as WarehouseClient>::Error>,
        {
            (
                std::any::type_name::<C>(),
                logs.schema_changes().ddl_landed_table(),
                logs.row_changes().envelope(),
            )
        }
    }

    fn capture(dialect: &str) -> CaptureConfig {
        toml::from_str(&format!(
            "[debezium.{dialect}]\ntopic_prefix = \"{dialect}\""
        ))
        .unwrap()
    }

    fn example(name: &str) -> CaptureConfig {
        let pipeline: Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> = Pipeline::open(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../compose/lithify")
                .join(name),
        )
        .unwrap();

        pipeline.sources().next().unwrap().capture().clone()
    }

    #[test]
    fn each_example_pipeline_is_captured_in_the_dialect_of_its_source() {
        assert_eq!(example("mysql"), capture("mysql"));
        assert_eq!(example("postgres"), capture("postgres"));
    }

    #[test]
    fn a_source_is_captured_by_a_dialect_of_a_format_this_program_reads() {
        assert_eq!(
            capture("mysql"),
            CaptureConfig::Debezium(DebeziumConfig::MySql(debezium::Config::new("mysql")))
        );
        assert_eq!(
            capture("postgres"),
            CaptureConfig::Debezium(DebeziumConfig::Postgres(debezium::Config::new("postgres")))
        );
    }

    #[test]
    fn a_format_this_program_does_not_read_is_rejected() {
        let unknown = toml::from_str::<CaptureConfig>("[maxwell.mysql]\ntopic_prefix = \"mysql\"");

        assert!(unknown.is_err());
    }

    #[test]
    fn a_dialect_this_program_does_not_read_debezium_in_is_rejected() {
        let unknown =
            toml::from_str::<CaptureConfig>("[debezium.oracle]\ntopic_prefix = \"oracle\"");

        assert!(unknown.is_err());
    }

    #[tokio::test]
    async fn a_duckdb_fn_is_called_with_the_logs_of_the_dialect_landed_in_object_storage() {
        let lake = DuckDbWarehouse::new("shop_cdc");
        let value = Some(ColumnPath::from("value"));

        assert_eq!(
            Capture::<DuckDbClient>::with_change_logs(&capture("mysql"), &lake, Probe).await,
            (
                std::any::type_name::<Landed<Debezium<MySql>, S3Sink>>(),
                TablePath::new("shop_cdc", "mysql"),
                value.clone()
            )
        );
        assert_eq!(
            Capture::<DuckDbClient>::with_change_logs(&capture("postgres"), &lake, Probe).await,
            (
                std::any::type_name::<Landed<Debezium<Postgres>, S3Sink>>(),
                TablePath::new("shop_cdc", "postgres"),
                value
            )
        );
    }

    #[tokio::test]
    async fn a_bigquery_fn_is_called_with_the_logs_of_the_dialect_landed_in_bigquery() {
        let dataset = BigQueryWarehouse::new("local-project", "shop_cdc");

        assert_eq!(
            Capture::<BigQueryClient>::with_change_logs(&capture("mysql"), &dataset, Probe).await,
            (
                std::any::type_name::<Landed<Debezium<MySql>, BigQuerySink>>(),
                TablePath::new("shop_cdc", "mysql"),
                None
            )
        );
        assert_eq!(
            Capture::<BigQueryClient>::with_change_logs(&capture("postgres"), &dataset, Probe)
                .await,
            (
                std::any::type_name::<Landed<Debezium<Postgres>, BigQuerySink>>(),
                TablePath::new("shop_cdc", "postgres"),
                None
            )
        );
    }

    const BIGQUERY: &str = include_str!("../testdata/pipeline/bigquery.toml");

    #[test]
    fn a_pipeline_is_sunk_into_a_warehouse_this_program_reads() {
        let warehouse: WarehouseConfig = toml::from_str(BIGQUERY).unwrap();

        let WarehouseConfig::BigQuery(bigquery) = warehouse else {
            panic!("expected a bigquery warehouse");
        };
        assert_eq!(bigquery.warehouse().project(), "local-project");
    }

    #[test]
    fn a_warehouse_reaches_the_change_log_and_the_places_written_in_it() {
        let bigquery: WarehouseConfig = toml::from_str(BIGQUERY).unwrap();
        let places = bigquery.places();
        let snapshot = places.get(&"snapshot".into()).unwrap();

        assert!(bigquery.reaches(&bigquery.change_log()));
        assert!(bigquery.reaches(snapshot));
        assert_eq!(
            Namespace::from(snapshot.clone()),
            Namespace::from("mysql_shop")
        );
    }

    #[test]
    fn a_warehouse_this_program_does_not_read_is_rejected() {
        let unknown = toml::from_str::<WarehouseConfig>(
            "[redshift]\ncluster = \"shop\"\nchange_log = { schema = \"mysql_shop_cdc\" }",
        );

        assert!(unknown.is_err());
    }
}
