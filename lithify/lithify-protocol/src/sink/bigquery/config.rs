use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Credentials {
    #[default]
    Adc,
    Anonymous,
}
