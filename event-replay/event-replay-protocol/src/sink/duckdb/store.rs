use duckdb::Connection;

use crate::sink::duckdb::DuckDbWarehouse;
use crate::sink::duckdb::client::DuckDbClient;
use crate::sink::duckdb::config::Store;

pub struct ObjectStore {
    store: Store,
    key_id: String,
    secret: String,
}

impl ObjectStore {
    pub fn open(store: &Store, key_id: &str, secret: &str) -> Self {
        Self {
            store: store.clone(),
            key_id: key_id.to_owned(),
            secret: secret.to_owned(),
        }
    }

    pub fn client(&self) -> Result<DuckDbClient, duckdb::Error> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(&self.secret())?;

        Ok(DuckDbClient::new(
            connection,
            DuckDbWarehouse::over(self.store.clone()),
        ))
    }

    fn secret(&self) -> String {
        self.store.secret(&self.key_id, &self.secret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::duckdb::config::tests::s3;

    fn store() -> ObjectStore {
        ObjectStore::open(&s3("http://minio:9000"), "minioadmin", "minioadmin")
    }

    #[test]
    fn object_storage_reached_without_tls_is_read_as_such() {
        assert!(
            store()
                .secret()
                .contains("ENDPOINT 'minio:9000', USE_SSL false")
        );
    }

    #[test]
    fn object_storage_reached_over_tls_is_read_as_such() {
        let google = ObjectStore::open(&s3("https://storage.googleapis.com"), "key", "secret");

        assert!(
            google
                .secret()
                .contains("ENDPOINT 'storage.googleapis.com', USE_SSL true")
        );
    }

    #[test]
    fn object_storage_is_reached_with_the_credential_it_was_given() {
        let secret = store().secret();

        assert!(secret.contains("KEY_ID 'minioadmin', SECRET 'minioadmin'"));
        assert!(secret.contains("REGION 'us-east-1', ENDPOINT 'minio:9000', USE_SSL false"));
    }
}
