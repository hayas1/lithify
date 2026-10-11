use serde::{Deserialize, Serialize};

use event_replay_core::sink::TablePath;

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
    pub fn scan(&self, table: &TablePath) -> String {
        match self {
            Self::S3(s3) => s3.scan(table),
        }
    }

    pub fn secret(&self, key_id: &str, secret: &str) -> String {
        match self {
            Self::S3(s3) => s3.secret(key_id, secret),
        }
    }
}

impl S3 {
    fn secret(&self, key_id: &str, secret: &str) -> String {
        let (endpoint, over_tls) = self.host();

        format!(
            "INSTALL httpfs;\n\
             LOAD httpfs;\n\
             CREATE OR REPLACE SECRET (TYPE s3, PROVIDER config, KEY_ID '{key_id}', SECRET '{secret}', \
             REGION '{}', ENDPOINT '{endpoint}', USE_SSL {over_tls}, URL_STYLE 'path')",
            self.region,
        )
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

    fn scan(&self, table: &TablePath) -> String {
        self.format.scan(&format!(
            "s3://{}/{}/{}/*.{}",
            self.bucket,
            table.namespace(),
            table.name(),
            self.format.extension()
        ))
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

    fn scan(&self, files: &str) -> String {
        match self {
            Self::Parquet => format!("read_parquet('{files}', union_by_name = true)"),
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
                "bucket": "event-replay",
                "format": "parquet"
            }
        }))
        .unwrap()
    }

    #[test]
    fn a_table_is_scanned_from_the_files_the_sink_wrote_in_the_format_they_were_written_in() {
        assert_eq!(
            s3("http://minio:9000").scan(&TablePath::new("mysql_shop_cdc", "mysql")),
            "read_parquet('s3://event-replay/mysql_shop_cdc/mysql/*.parquet', union_by_name = true)"
        );
    }
}
