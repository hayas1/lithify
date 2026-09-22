use duckdb::Connection;

use event_replay_core::sink::{TablePath, Warehouse};

use crate::sink::duckdb::DuckDbWarehouse;
use crate::sink::duckdb::client::DuckDbClient;
use crate::sink::duckdb::config::{FileFormat, S3, Store};

pub struct ObjectStore {
    store: S3,
    key_id: String,
    secret: String,
}

impl ObjectStore {
    pub fn open(store: &Store, key_id: &str, secret: &str) -> Self {
        let Store::S3(store) = store;

        Self {
            store: store.clone(),
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
            self.key_id,
            self.secret,
            self.store.region(),
        )
    }

    fn endpoint(&self) -> (&str, bool) {
        let endpoint = self.store.endpoint();

        match endpoint.strip_prefix("https://") {
            Some(host) => (host, true),
            None => (endpoint.strip_prefix("http://").unwrap_or(endpoint), false),
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
        match self.store.format() {
            FileFormat::Parquet => format!("read_parquet('{}')", self.files(table)),
        }
    }

    fn files(&self, table: &TablePath) -> String {
        format!(
            "s3://{}/{}/{}/*.{}",
            self.store.bucket(),
            table.namespace(),
            table.name(),
            self.store.format().extension()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3(endpoint: &str) -> Store {
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

    fn store() -> ObjectStore {
        ObjectStore::open(&s3("http://minio:9000"), "minioadmin", "minioadmin")
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
        let google = ObjectStore::open(&s3("https://storage.googleapis.com"), "key", "secret");

        assert_eq!(google.endpoint(), ("storage.googleapis.com", true));
    }

    #[test]
    fn object_storage_is_reached_with_the_credential_it_was_given() {
        let secret = store().secret();

        assert!(secret.contains("KEY_ID 'minioadmin', SECRET 'minioadmin'"));
        assert!(secret.contains("REGION 'us-east-1', ENDPOINT 'minio:9000', USE_SSL false"));
    }
}
