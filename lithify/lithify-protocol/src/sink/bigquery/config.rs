use serde::{Deserialize, Serialize};

use lithify_core::sink::Namespace;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    project: String,
    endpoint: Option<String>,
    #[serde(default)]
    credentials: Credentials,
}

impl Config {
    pub fn project(&self) -> &str {
        &self.project
    }

    pub fn endpoint(&self) -> &str {
        self.endpoint.as_deref().unwrap_or(Self::GOOGLE)
    }

    pub fn credentials(&self) -> Credentials {
        self.credentials
    }

    const GOOGLE: &'static str = "https://bigquery.googleapis.com";
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Location {
    project: Option<String>,
    dataset: String,
}

impl From<&Location> for Namespace {
    fn from(location: &Location) -> Self {
        Namespace::from(
            location
                .project
                .iter()
                .chain([&location.dataset])
                .cloned()
                .collect::<Vec<_>>(),
        )
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Credentials {
    #[default]
    Adc,
    Anonymous,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dataset_in_the_project_of_the_warehouse_is_named_alone() {
        let location: Location =
            serde_json::from_value(serde_json::json!({"dataset": "mysql_shop_cdc"})).unwrap();

        assert_eq!(
            Namespace::from(&location),
            Namespace::from("mysql_shop_cdc")
        );
    }

    #[test]
    fn a_dataset_in_another_project_is_named_after_that_project() {
        let location: Location = serde_json::from_value(serde_json::json!({
            "project": "raw-project",
            "dataset": "mysql_shop_cdc"
        }))
        .unwrap();

        assert_eq!(
            Namespace::from(&location),
            Namespace::from("raw-project.mysql_shop_cdc")
        );
    }

    #[test]
    fn a_location_naming_what_bigquery_does_not_know_is_rejected() {
        assert!(
            serde_json::from_value::<Location>(serde_json::json!({"prefix": "mysql_shop_cdc"}))
                .is_err()
        );
    }
}
