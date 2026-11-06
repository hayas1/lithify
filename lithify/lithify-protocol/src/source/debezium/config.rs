use serde::{Deserialize, Serialize};

use crate::source::debezium::{Debezium, Dialect};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    topic_prefix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ddl_topic: Option<String>,
}

impl Config {
    pub fn new(topic_prefix: &str) -> Self {
        Self {
            topic_prefix: topic_prefix.to_owned(),
            ddl_topic: None,
        }
    }

    pub fn debezium<D: Dialect>(&self, dialect: D) -> Debezium<D> {
        let log = Debezium::new(&self.topic_prefix, dialect);

        match &self.ddl_topic {
            Some(ddl_topic) => log.with_ddl_topic(ddl_topic),
            None => log,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::CdcFormat;
    use crate::source::debezium::mysql::MySql;

    #[test]
    fn a_source_is_read_under_the_topic_prefix_of_its_connector() {
        let config: Config =
            serde_json::from_value(serde_json::json!({"topic_prefix": "postgres"})).unwrap();

        assert_eq!(config, Config::new("postgres"));
    }

    #[test]
    fn a_source_naming_no_topic_prefix_of_its_connector_is_rejected() {
        let unprefixed = serde_json::from_value::<Config>(serde_json::json!({}));

        assert!(unprefixed.is_err());
    }

    #[test]
    fn a_source_lands_its_schema_changes_on_the_topic_named_after_its_prefix() {
        assert_eq!(Config::new("mysql").debezium(MySql).ddl_topic(), "mysql");
    }

    #[test]
    fn a_source_names_the_topic_its_schema_changes_land_on_apart_from_its_prefix() {
        let config: Config = serde_json::from_value(serde_json::json!({
            "topic_prefix": "mysql",
            "ddl_topic": "mysql.schema-changes"
        }))
        .unwrap();

        assert_eq!(config.debezium(MySql).ddl_topic(), "mysql.schema-changes");
    }
}
