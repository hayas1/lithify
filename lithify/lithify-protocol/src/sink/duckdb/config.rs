use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use lithify_core::sink::{Namespace, TablePath};
use lithify_core::sql::SqlString;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "WrittenConfig", into = "WrittenConfig")]
pub struct Config {
    database: Option<PathBuf>,
    store: Store,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WrittenConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    database: Option<PathBuf>,
    s3: Option<S3>,
}

impl TryFrom<WrittenConfig> for Config {
    type Error = &'static str;

    fn try_from(written: WrittenConfig) -> Result<Self, Self::Error> {
        match written.s3 {
            Some(s3) => Ok(Self::new(written.database, Store::S3(s3))),
            None => Err("a duckdb warehouse names no store its change log is read from"),
        }
    }
}

impl From<Config> for WrittenConfig {
    fn from(config: Config) -> Self {
        match config.store {
            Store::S3(s3) => Self {
                database: config.database,
                s3: Some(s3),
            },
        }
    }
}

impl Config {
    pub fn new(database: Option<PathBuf>, store: Store) -> Self {
        Self { database, store }
    }

    pub fn database(&self) -> Option<&Path> {
        self.database.as_deref()
    }

    pub fn store(&self) -> &Store {
        &self.store
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum Store {
    S3(S3),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct S3 {
    endpoint: String,
    region: String,
    bucket: String,
    format: FileFormat,
}

impl Store {
    pub fn scan(&self, table: &TablePath) -> SqlString {
        match self {
            Self::S3(s3) => s3.scan(table),
        }
    }

    pub fn existence(&self, table: &TablePath) -> SqlString {
        match self {
            Self::S3(s3) => s3.existence(table),
        }
    }

    pub fn replace(&self, table: &TablePath, select: &SqlString) -> SqlString {
        match self {
            Self::S3(s3) => s3.replace(table, select),
        }
    }

    pub fn append(&self, table: &TablePath, select: &SqlString) -> SqlString {
        match self {
            Self::S3(s3) => s3.append(table, select),
        }
    }

    pub fn secret(&self, key_id: &str, secret: &str) -> SqlString {
        match self {
            Self::S3(s3) => s3.secret(key_id, secret),
        }
    }
}

impl S3 {
    fn secret(&self, key_id: &str, secret: &str) -> SqlString {
        let (endpoint, over_tls) = self.host();

        SqlString::from(format!(
            "INSTALL httpfs;\n\
             LOAD httpfs;\n\
             CREATE OR REPLACE SECRET (TYPE s3, PROVIDER config, KEY_ID '{key_id}', SECRET '{secret}', \
             REGION '{}', ENDPOINT '{endpoint}', USE_SSL {over_tls}, URL_STYLE 'path')",
            self.region,
        ))
    }

    fn host(&self) -> (&str, bool) {
        match self.endpoint.strip_prefix("https://") {
            Some(host) => (host, true),
            None => (
                self.endpoint
                    .strip_prefix("http://")
                    .unwrap_or(&self.endpoint),
                false,
            ),
        }
    }

    fn scan(&self, table: &TablePath) -> SqlString {
        self.format.scan(&self.files(table))
    }

    fn existence(&self, table: &TablePath) -> SqlString {
        SqlString::from(format!(
            "SELECT COUNT(*) AS \"count\" FROM glob('{}')",
            self.files(table)
        ))
    }

    fn replace(&self, table: &TablePath, select: &SqlString) -> SqlString {
        SqlString::from(format!(
            "COPY (\n{select}\n) TO '{}/{}.{}' ({})",
            self.directory(table),
            table.name(),
            self.format.extension(),
            self.format.copy()
        ))
    }

    fn append(&self, table: &TablePath, select: &SqlString) -> SqlString {
        SqlString::from(format!(
            "COPY (\n{select}\n) TO '{}' ({}, FILENAME_PATTERN '{}_{{uuid}}', PER_THREAD_OUTPUT true, APPEND true)",
            self.directory(table),
            self.format.copy(),
            table.name()
        ))
    }

    fn files(&self, table: &TablePath) -> String {
        format!("{}/*.{}", self.directory(table), self.format.extension())
    }

    fn directory(&self, table: &TablePath) -> String {
        format!(
            "s3://{}/{}/{}",
            self.bucket,
            table.namespace().parts().collect::<Vec<_>>().join("/"),
            table.name()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Location {
    prefix: String,
}

impl From<Location> for Namespace {
    fn from(location: Location) -> Self {
        Namespace::from(vec![location.prefix])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Parquet,
}

impl FileFormat {
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
        }
    }

    fn copy(&self) -> &'static str {
        match self {
            Self::Parquet => "FORMAT parquet",
        }
    }

    fn scan(&self, files: &str) -> SqlString {
        match self {
            Self::Parquet => {
                SqlString::from(format!("read_parquet('{files}', union_by_name = true)"))
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn s3(endpoint: &str) -> Store {
        serde_json::from_value(serde_json::json!({
            "s3": {
                "endpoint": endpoint,
                "region": "us-east-1",
                "bucket": "lithify",
                "format": "parquet"
            }
        }))
        .unwrap()
    }

    pub(crate) fn kept_in(database: Option<&str>) -> Config {
        let mut config = serde_json::json!({
            "s3": {
                "endpoint": "http://rustfs:9000",
                "region": "us-east-1",
                "bucket": "lithify",
                "format": "parquet"
            }
        });
        if let Some(database) = database {
            config["database"] = serde_json::json!(database);
        }

        serde_json::from_value(config).unwrap()
    }

    #[test]
    fn a_warehouse_keeps_what_it_builds_in_the_database_it_names() {
        assert_eq!(
            kept_in(Some("lithify.duckdb")).database(),
            Some(std::path::Path::new("lithify.duckdb"))
        );
        assert_eq!(kept_in(None).database(), None);
        assert_eq!(kept_in(None).store(), &s3("http://rustfs:9000"));
    }

    #[test]
    fn a_warehouse_naming_what_it_does_not_know_is_rejected() {
        let unknown = serde_json::from_value::<Config>(serde_json::json!({
            "database": "lithify.duckdb",
            "s3": {
                "endpoint": "http://rustfs:9000",
                "region": "us-east-1",
                "bucket": "lithify",
                "format": "parquet"
            },
            "catalog": "lithify"
        }));

        assert!(unknown.is_err());
    }

    #[test]
    fn a_warehouse_naming_no_store_is_rejected() {
        let storeless =
            serde_json::from_value::<Config>(serde_json::json!({"database": "lithify.duckdb"}));

        assert!(storeless.is_err());
    }

    #[test]
    fn a_table_is_scanned_from_the_files_the_sink_wrote_in_the_format_they_were_written_in() {
        assert_eq!(
            s3("http://rustfs:9000").scan(&TablePath::new("mysql_shop_cdc", "mysql")),
            "read_parquet('s3://lithify/mysql_shop_cdc/mysql/*.parquet', union_by_name = true)"
        );
    }

    #[test]
    fn a_location_lies_under_the_prefix_it_names() {
        let location: Location =
            serde_json::from_value(serde_json::json!({"prefix": "mysql_shop_cdc"})).unwrap();

        assert_eq!(Namespace::from(location), Namespace::from("mysql_shop_cdc"));
    }

    #[test]
    fn a_table_lies_under_each_name_of_its_namespace_as_a_directory() {
        assert_eq!(
            s3("http://rustfs:9000").scan(&TablePath::new("lithify.mysql_shop", "people")),
            "read_parquet('s3://lithify/lithify/mysql_shop/people/*.parquet', union_by_name = true)"
        );
    }
}
