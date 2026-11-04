use serde::{Deserialize, Serialize};

#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use lithify_core::sink::WarehouseClient;
#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use lithify_core::source::SchemaChangeStream;
#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use lithify_pipeline::capture::{Capture, ChangeLogsFn};

#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use crate::landing::{Landed, Landing};
#[cfg(feature = "bigquery")]
use crate::sink::bigquery::BigQuerySink;
#[cfg(feature = "bigquery")]
use crate::sink::bigquery::client::{BigQueryClient, BigQueryExecution};
#[cfg(feature = "duckdb")]
use crate::sink::duckdb::S3Sink;
#[cfg(feature = "duckdb")]
use crate::sink::duckdb::client::{DuckDbClient, DuckDbExecution};
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
    topic_prefix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ddl_topic: Option<String>,
}

impl Config {
    pub fn of(dialect: Dialect, topic_prefix: &str) -> Self {
        Self {
            dialect,
            topic_prefix: topic_prefix.to_owned(),
            ddl_topic: None,
        }
    }

    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    fn debezium<D: super::Dialect>(&self, dialect: D) -> Debezium<D> {
        let log = Debezium::new(&self.topic_prefix, dialect);

        match &self.ddl_topic {
            Some(ddl_topic) => log.with_ddl_topic(ddl_topic),
            None => log,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    MySql,
    Postgres,
}

#[cfg(feature = "duckdb")]
impl<X: DuckDbExecution> Capture<DuckDbClient<X>> for Config {
    async fn with_change_logs<F: ChangeLogsFn<DuckDbClient<X>>>(
        &self,
        warehouse: &<DuckDbClient<X> as WarehouseClient>::Warehouse,
        f: F,
    ) -> F::Output {
        self.dialect
            .with_change_logs(self, S3Sink, warehouse, f)
            .await
    }
}

#[cfg(feature = "bigquery")]
impl<X: BigQueryExecution> Capture<BigQueryClient<X>> for Config {
    async fn with_change_logs<F: ChangeLogsFn<BigQueryClient<X>>>(
        &self,
        warehouse: &<BigQueryClient<X> as WarehouseClient>::Warehouse,
        f: F,
    ) -> F::Output {
        self.dialect
            .with_change_logs(self, BigQuerySink, warehouse, f)
            .await
    }
}

impl Dialect {
    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    async fn with_change_logs<K, L, F>(
        self,
        config: &Config,
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
            Self::MySql => {
                f.call(Landed::new(config.debezium(MySql), landing, warehouse))
                    .await
            }
            Self::Postgres => {
                f.call(Landed::new(config.debezium(Postgres), landing, warehouse))
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    use lithify_core::sink::TablePath;
    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    use lithify_core::source::{ChangeLog, ChangeLogs, ColumnPath, SchemaChangeLog};

    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    type Probed = (
        &'static str,
        TablePath,
        Option<ColumnPath>,
        Option<ColumnPath>,
    );

    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    struct Probe;

    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    impl<K: WarehouseClient> ChangeLogsFn<K> for Probe {
        type Output = Probed;

        async fn call<C>(self, logs: C) -> Self::Output
        where
            C: ChangeLogs,
            K: SchemaChangeStream<C::Schema, Error = <K as WarehouseClient>::Error>,
        {
            (
                std::any::type_name::<C>(),
                logs.schema_changes().ddl_landed_table(),
                logs.schema_changes().envelope(),
                logs.row_changes().envelope(),
            )
        }
    }

    #[cfg(feature = "duckdb")]
    fn lake() -> crate::DuckDbWarehouse {
        crate::DuckDbWarehouse::new("shop_cdc")
    }

    #[cfg(feature = "bigquery")]
    fn dataset() -> crate::BigQueryWarehouse {
        crate::BigQueryWarehouse::new("local-project", "shop_cdc")
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn a_source_names_the_topic_its_schema_changes_land_on_apart_from_its_prefix() {
        let config: Config = serde_json::from_value(serde_json::json!({
            "dialect": "mysql",
            "topic_prefix": "mysql",
            "ddl_topic": "mysql.schema-changes"
        }))
        .unwrap();

        assert_eq!(
            Capture::<DuckDbClient>::with_change_logs(&config, &lake(), Probe)
                .await
                .1,
            TablePath::new("shop_cdc", "mysql.schema-changes")
        );
    }

    #[test]
    fn a_source_says_which_dialect_debezium_reads_it_as_under_the_topic_prefix_of_its_connector() {
        let config: Config = serde_json::from_value(
            serde_json::json!({"dialect": "postgres", "topic_prefix": "postgres"}),
        )
        .unwrap();

        assert_eq!(config, Config::of(Dialect::Postgres, "postgres"));
        assert_eq!(config.dialect(), Dialect::Postgres);
    }

    #[test]
    fn a_source_naming_no_topic_prefix_of_its_connector_is_rejected() {
        let unprefixed = serde_json::from_value::<Config>(serde_json::json!({"dialect": "mysql"}));

        assert!(unprefixed.is_err());
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn a_duckdb_fn_is_called_with_the_logs_of_the_dialect_landed_in_object_storage() {
        let value = Some(ColumnPath::from("value"));

        assert_eq!(
            Capture::<DuckDbClient>::with_change_logs(
                &Config::of(Dialect::MySql, "mysql"),
                &lake(),
                Probe
            )
            .await,
            (
                std::any::type_name::<Landed<Debezium<MySql>, S3Sink>>(),
                TablePath::new("shop_cdc", "mysql"),
                value.clone(),
                value.clone()
            )
        );
        assert_eq!(
            Capture::<DuckDbClient>::with_change_logs(
                &Config::of(Dialect::Postgres, "postgres"),
                &lake(),
                Probe
            )
            .await,
            (
                std::any::type_name::<Landed<Debezium<Postgres>, S3Sink>>(),
                TablePath::new("shop_cdc", "postgres"),
                value.clone(),
                value
            )
        );
    }

    #[cfg(feature = "bigquery")]
    #[tokio::test]
    async fn a_bigquery_fn_is_called_with_the_logs_of_the_dialect_landed_in_bigquery() {
        assert_eq!(
            Capture::<BigQueryClient>::with_change_logs(
                &Config::of(Dialect::MySql, "mysql"),
                &dataset(),
                Probe
            )
            .await,
            (
                std::any::type_name::<Landed<Debezium<MySql>, BigQuerySink>>(),
                TablePath::new("shop_cdc", "mysql"),
                None,
                None
            )
        );
        assert_eq!(
            Capture::<BigQueryClient>::with_change_logs(
                &Config::of(Dialect::Postgres, "postgres"),
                &dataset(),
                Probe
            )
            .await,
            (
                std::any::type_name::<Landed<Debezium<Postgres>, BigQuerySink>>(),
                TablePath::new("shop_cdc", "postgres"),
                None,
                None
            )
        );
    }

    #[test]
    fn a_dialect_debezium_does_not_read_is_rejected() {
        let unknown = serde_json::from_value::<Config>(
            serde_json::json!({"dialect": "oracle", "topic_prefix": "oracle"}),
        );

        assert!(unknown.is_err());
    }
}
