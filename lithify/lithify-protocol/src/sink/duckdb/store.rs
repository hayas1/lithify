use duckdb::Connection;

use lithify_core::sql::SqlString;

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

    pub fn client(&self, change_log: &str) -> Result<DuckDbClient, duckdb::Error> {
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(self.secret().as_str())?;

        Ok(DuckDbClient::new(
            connection,
            DuckDbWarehouse::over(change_log, self.store.clone()),
        ))
    }

    fn secret(&self) -> SqlString {
        self.store.secret(&self.key_id, &self.secret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::duckdb::config::tests::s3;

    fn store() -> ObjectStore {
        ObjectStore::open(&s3("http://rustfs:9000"), "rustfsadmin", "rustfsadmin")
    }

    #[test]
    fn object_storage_reached_without_tls_is_read_as_such() {
        assert!(
            store()
                .secret()
                .as_str()
                .contains("ENDPOINT 'rustfs:9000', USE_SSL false")
        );
    }

    #[test]
    fn object_storage_reached_over_tls_is_read_as_such() {
        let google = ObjectStore::open(&s3("https://storage.googleapis.com"), "key", "secret");

        assert!(
            google
                .secret()
                .as_str()
                .contains("ENDPOINT 'storage.googleapis.com', USE_SSL true")
        );
    }

    #[test]
    fn object_storage_is_reached_with_the_credential_it_was_given() {
        let secret = store().secret().to_string();

        assert!(secret.contains("KEY_ID 'rustfsadmin', SECRET 'rustfsadmin'"));
        assert!(secret.contains("REGION 'us-east-1', ENDPOINT 'rustfs:9000', USE_SSL false"));
    }
}
