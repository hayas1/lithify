use duckdb::Connection;

use event_replay_core::sink::{TablePath, Warehouse};

use crate::sink::duckdb::DuckDbWarehouse;
use crate::sink::duckdb::client::DuckDbClient;

pub struct ObjectStore {
    endpoint: String,
    region: String,
    bucket: String,
    format: FileFormat,
    key_id: String,
    secret: String,
}

impl ObjectStore {
    pub fn new(
        endpoint: &str,
        region: &str,
        bucket: &str,
        format: FileFormat,
        key_id: &str,
        secret: &str,
    ) -> Self {
        Self {
            endpoint: endpoint.to_owned(),
            region: region.to_owned(),
            bucket: bucket.to_owned(),
            format,
            key_id: key_id.to_owned(),
            secret: secret.to_owned(),
        }
    }

    pub fn client(&self, table: &TablePath) -> Result<DuckDbClient, duckdb::Error> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(&self.secret())?;
        connection.execute_batch(&self.view(table))?;

        Ok(DuckDbClient::new(connection))
    }

    fn secret(&self) -> String {
        let (endpoint, over_tls) = self.endpoint();

        format!(
            "INSTALL httpfs;\n\
             LOAD httpfs;\n\
             CREATE SECRET (TYPE s3, PROVIDER config, KEY_ID '{}', SECRET '{}', \
             REGION '{}', ENDPOINT '{endpoint}', USE_SSL {over_tls}, URL_STYLE 'path')",
            self.key_id, self.secret, self.region,
        )
    }

    fn endpoint(&self) -> (&str, bool) {
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

    fn view(&self, table: &TablePath) -> String {
        format!(
            "CREATE SCHEMA IF NOT EXISTS {};\nCREATE VIEW {} AS SELECT * FROM {}",
            DuckDbWarehouse.identifier(table.namespace()),
            DuckDbWarehouse.table_ref(table),
            self.scan(table),
        )
    }

    fn scan(&self, table: &TablePath) -> String {
        match self.format {
            FileFormat::Parquet => format!("read_parquet('{}')", self.files(table)),
        }
    }

    fn files(&self, table: &TablePath) -> String {
        format!(
            "s3://{}/{}/{}/*.{}",
            self.bucket,
            table.namespace(),
            table.name(),
            self.format.extension()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Parquet,
}

impl FileFormat {
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> ObjectStore {
        ObjectStore::new(
            "http://minio:9000",
            "us-east-1",
            "event-replay",
            FileFormat::Parquet,
            "minioadmin",
            "minioadmin",
        )
    }

    fn table() -> TablePath {
        TablePath::new("mysql_shop_cdc", "mysql")
    }

    #[test]
    fn the_change_log_of_a_table_is_read_from_the_files_the_sink_wrote() {
        assert_eq!(
            store().files(&table()),
            "s3://event-replay/mysql_shop_cdc/mysql/*.parquet"
        );
    }

    #[test]
    fn the_files_of_a_table_stand_for_it_in_the_format_they_were_written_in() {
        assert_eq!(
            store().view(&table()),
            concat!(
                "CREATE SCHEMA IF NOT EXISTS \"mysql_shop_cdc\";\n",
                "CREATE VIEW \"mysql_shop_cdc\".\"mysql\" AS SELECT * FROM ",
                "read_parquet('s3://event-replay/mysql_shop_cdc/mysql/*.parquet')"
            )
        );
    }

    #[test]
    fn object_storage_reached_without_tls_is_read_as_such() {
        assert_eq!(store().endpoint(), ("minio:9000", false));
    }

    #[test]
    fn object_storage_reached_over_tls_is_read_as_such() {
        let google = ObjectStore::new(
            "https://storage.googleapis.com",
            "auto",
            "event-replay",
            FileFormat::Parquet,
            "key",
            "secret",
        );

        assert_eq!(google.endpoint(), ("storage.googleapis.com", true));
    }

    #[test]
    fn object_storage_is_reached_with_the_credential_it_was_given() {
        let secret = store().secret();

        assert!(secret.contains("KEY_ID 'minioadmin', SECRET 'minioadmin'"));
        assert!(secret.contains("REGION 'us-east-1', ENDPOINT 'minio:9000', USE_SSL false"));
    }
}
