use serde::{Deserialize, Serialize};

#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use lithify_core::sink::WarehouseClient;
#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use lithify_core::source::SchemaChangeStream;
#[cfg(any(feature = "bigquery", feature = "duckdb"))]
use lithify_pipeline::capture::{Capture, ChangeLogsFn};

#[cfg(feature = "bigquery")]
use crate::sink::bigquery::client::{BigQueryClient, BigQueryExecution};
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ddl_topic: Option<String>,
}

impl Config {
    #[cfg(feature = "duckdb")]
    const DUCKDB_ENVELOPE: Option<&str> = Some("value");
    #[cfg(feature = "bigquery")]
    const BIGQUERY_ENVELOPE: Option<&str> = None;

    pub fn of(dialect: Dialect) -> Self {
        Self {
            dialect,
            ddl_topic: None,
        }
    }

    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    fn debezium<D: super::Dialect>(
        &self,
        envelope: Option<&str>,
        topic_prefix: &str,
        dialect: D,
    ) -> Debezium<D> {
        let log = Debezium::within(envelope, topic_prefix, dialect);

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
        topic_prefix: &str,
        f: F,
    ) -> F::Output {
        self.dialect
            .with_change_logs(self, Self::DUCKDB_ENVELOPE, topic_prefix, f)
            .await
    }
}

#[cfg(feature = "bigquery")]
impl<X: BigQueryExecution> Capture<BigQueryClient<X>> for Config {
    async fn with_change_logs<F: ChangeLogsFn<BigQueryClient<X>>>(
        &self,
        topic_prefix: &str,
        f: F,
    ) -> F::Output {
        self.dialect
            .with_change_logs(self, Self::BIGQUERY_ENVELOPE, topic_prefix, f)
            .await
    }
}

impl Dialect {
    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    async fn with_change_logs<K, F>(
        self,
        config: &Config,
        envelope: Option<&str>,
        topic_prefix: &str,
        f: F,
    ) -> F::Output
    where
        K: WarehouseClient
            + SchemaChangeStream<Debezium<MySql>, Error = <K as WarehouseClient>::Error>
            + SchemaChangeStream<Debezium<Postgres>, Error = <K as WarehouseClient>::Error>,
        F: ChangeLogsFn<K>,
    {
        match self {
            Self::MySql => f.call(config.debezium(envelope, topic_prefix, MySql)).await,
            Self::Postgres => {
                f.call(config.debezium(envelope, topic_prefix, Postgres))
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    use lithify_core::source::{ChangeLog, ChangeLogs, ColumnPath, SchemaChangeLog};

    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    type Probed = (&'static str, String, Option<ColumnPath>, Option<ColumnPath>);

    #[cfg(any(feature = "bigquery", feature = "duckdb"))]
    fn probe<C: ChangeLogs>(logs: C) -> Probed {
        (
            std::any::type_name::<C>(),
            logs.schema_changes().ddl_topic(),
            logs.schema_changes().envelope(),
            logs.row_changes().envelope(),
        )
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn a_source_names_the_topic_its_schema_changes_land_on_apart_from_its_prefix() {
        struct DdlTopic;

        impl<K: WarehouseClient> ChangeLogsFn<K> for DdlTopic {
            type Output = String;

            async fn call<C>(self, logs: C) -> Self::Output
            where
                C: ChangeLogs,
                K: SchemaChangeStream<C::Schema, Error = <K as WarehouseClient>::Error>,
            {
                logs.schema_changes().ddl_topic()
            }
        }

        let config: Config = serde_json::from_value(serde_json::json!({
            "dialect": "mysql",
            "ddl_topic": "mysql.schema-changes"
        }))
        .unwrap();

        assert_eq!(
            Capture::<DuckDbClient>::with_change_logs(&config, "mysql", DdlTopic).await,
            "mysql.schema-changes"
        );
    }

    #[test]
    fn a_source_says_which_dialect_debezium_reads_it_as() {
        let config: Config =
            serde_json::from_value(serde_json::json!({"dialect": "postgres"})).unwrap();

        assert_eq!(config.dialect(), Dialect::Postgres);
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn a_duckdb_fn_is_called_with_the_logs_of_the_dialect_on_the_prefix_under_the_value_of_each_record()
     {
        struct Probe;

        impl<K: WarehouseClient> ChangeLogsFn<K> for Probe {
            type Output = Probed;

            async fn call<C>(self, logs: C) -> Self::Output
            where
                C: ChangeLogs,
                K: SchemaChangeStream<C::Schema, Error = <K as WarehouseClient>::Error>,
            {
                probe(logs)
            }
        }

        let value = Some(ColumnPath::from("value"));

        assert_eq!(
            Capture::<DuckDbClient>::with_change_logs(&Config::of(Dialect::MySql), "mysql", Probe)
                .await,
            (
                std::any::type_name::<Debezium<MySql>>(),
                "mysql".to_owned(),
                value.clone(),
                value.clone()
            )
        );
        assert_eq!(
            Capture::<DuckDbClient>::with_change_logs(
                &Config::of(Dialect::Postgres),
                "postgres",
                Probe
            )
            .await,
            (
                std::any::type_name::<Debezium<Postgres>>(),
                "postgres".to_owned(),
                value.clone(),
                value
            )
        );
    }

    #[cfg(feature = "bigquery")]
    #[tokio::test]
    async fn a_bigquery_fn_is_called_with_the_logs_of_the_dialect_on_the_prefix_at_the_top_of_each_row()
     {
        struct Probe;

        impl<K: WarehouseClient> ChangeLogsFn<K> for Probe {
            type Output = Probed;

            async fn call<C>(self, logs: C) -> Self::Output
            where
                C: ChangeLogs,
                K: SchemaChangeStream<C::Schema, Error = <K as WarehouseClient>::Error>,
            {
                probe(logs)
            }
        }

        assert_eq!(
            Capture::<BigQueryClient>::with_change_logs(
                &Config::of(Dialect::MySql),
                "mysql",
                Probe
            )
            .await,
            (
                std::any::type_name::<Debezium<MySql>>(),
                "mysql".to_owned(),
                None,
                None
            )
        );
        assert_eq!(
            Capture::<BigQueryClient>::with_change_logs(
                &Config::of(Dialect::Postgres),
                "postgres",
                Probe
            )
            .await,
            (
                std::any::type_name::<Debezium<Postgres>>(),
                "postgres".to_owned(),
                None,
                None
            )
        );
    }

    #[test]
    fn a_dialect_debezium_does_not_read_is_rejected() {
        let unknown = serde_json::from_value::<Config>(serde_json::json!({"dialect": "oracle"}));

        assert!(unknown.is_err());
    }
}
