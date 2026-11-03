use serde::{Deserialize, Serialize};

#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use lithify_core::sink::{Namespace, WarehouseClient};
#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use lithify_core::source::SchemaChangeStream;
#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use lithify_pipeline::capture::{Capture, ChangeLogsFn};

#[cfg(feature = "bigquery")]
use crate::sink::bigquery::client::{BigQueryClient, BigQueryExecution};
#[cfg(feature = "duckdb")]
use crate::sink::duckdb::client::{DuckDbClient, DuckDbExecution};
#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use crate::source::debezium::mysql::MySql;
#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use crate::source::debezium::postgres::Postgres;
#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use crate::source::debezium::{Debezium, SinkConnector};

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
    fn debezium<D: super::Dialect>(
        &self,
        connector: SinkConnector,
        change_log: &Namespace,
        dialect: D,
    ) -> Debezium<D> {
        let log = Debezium::new(connector, change_log.clone(), &self.topic_prefix, dialect);

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
        change_log: &Namespace,
        f: F,
    ) -> F::Output {
        self.dialect
            .with_change_logs(self, SinkConnector::ObjectStore, change_log, f)
            .await
    }
}

#[cfg(feature = "bigquery")]
impl<X: BigQueryExecution> Capture<BigQueryClient<X>> for Config {
    async fn with_change_logs<F: ChangeLogsFn<BigQueryClient<X>>>(
        &self,
        change_log: &Namespace,
        f: F,
    ) -> F::Output {
        self.dialect
            .with_change_logs(self, SinkConnector::BigQuery, change_log, f)
            .await
    }
}

impl Dialect {
    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    async fn with_change_logs<K, F>(
        self,
        config: &Config,
        connector: SinkConnector,
        change_log: &Namespace,
        f: F,
    ) -> F::Output
    where
        K: WarehouseClient
            + SchemaChangeStream<Debezium<MySql>, Error = <K as WarehouseClient>::Error>
            + SchemaChangeStream<Debezium<Postgres>, Error = <K as WarehouseClient>::Error>,
        F: ChangeLogsFn<K>,
    {
        match self {
            Self::MySql => f.call(config.debezium(connector, change_log, MySql)).await,
            Self::Postgres => {
                f.call(config.debezium(connector, change_log, Postgres))
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
                logs.schema_changes().schema_change_table(),
                logs.schema_changes().envelope(),
                logs.row_changes().envelope(),
            )
        }
    }

    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    fn cdc() -> Namespace {
        Namespace::from("shop_cdc")
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
            Capture::<DuckDbClient>::with_change_logs(&config, &cdc(), Probe)
                .await
                .1,
            TablePath::new(cdc(), "mysql.schema-changes")
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
                &cdc(),
                Probe
            )
            .await,
            (
                std::any::type_name::<Debezium<MySql>>(),
                TablePath::new(cdc(), "mysql"),
                value.clone(),
                value.clone()
            )
        );
        assert_eq!(
            Capture::<DuckDbClient>::with_change_logs(
                &Config::of(Dialect::Postgres, "postgres"),
                &cdc(),
                Probe
            )
            .await,
            (
                std::any::type_name::<Debezium<Postgres>>(),
                TablePath::new(cdc(), "postgres"),
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
                &cdc(),
                Probe
            )
            .await,
            (
                std::any::type_name::<Debezium<MySql>>(),
                TablePath::new(cdc(), "mysql"),
                None,
                None
            )
        );
        assert_eq!(
            Capture::<BigQueryClient>::with_change_logs(
                &Config::of(Dialect::Postgres, "postgres"),
                &cdc(),
                Probe
            )
            .await,
            (
                std::any::type_name::<Debezium<Postgres>>(),
                TablePath::new(cdc(), "postgres"),
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
