use duckdb::Connection;

use lithify_core::sql::SqlString;

use crate::sink::duckdb::DuckDbWarehouse;
use crate::sink::duckdb::client::DuckDbClient;
use crate::sink::duckdb::config::Config;

pub struct ObjectStore {
    config: Config,
    key_id: String,
    secret: String,
}

impl ObjectStore {
    pub fn open(config: &Config, key_id: &str, secret: &str) -> Self {
        Self {
            config: config.clone(),
            key_id: key_id.to_owned(),
            secret: secret.to_owned(),
        }
    }

    pub fn client(&self, change_log: &str) -> Result<DuckDbClient, duckdb::Error> {
        let connection = self.connect()?;
        connection.execute_batch(self.secret().as_str())?;

        Ok(DuckDbClient::new(
            connection,
            DuckDbWarehouse::over(change_log, self.config.store().clone()),
        ))
    }

    fn connect(&self) -> Result<Connection, duckdb::Error> {
        match self.config.database() {
            Some(database) => Connection::open(database),
            None => Connection::open_in_memory(),
        }
    }

    fn secret(&self) -> SqlString {
        self.config.store().secret(&self.key_id, &self.secret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::duckdb::config::Config;
    use crate::sink::duckdb::config::tests::{kept_in, s3};

    fn store() -> ObjectStore {
        ObjectStore::open(&kept_in(None), "rustfsadmin", "rustfsadmin")
    }

    #[test]
    fn what_a_store_builds_outlives_its_connection_in_the_database_it_names() {
        let directory = std::env::temp_dir().join(format!("lithify-store-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let database = directory.join("lithify.duckdb");
        let _ = std::fs::remove_file(&database);
        let store = ObjectStore::open(
            &kept_in(Some(database.to_str().unwrap())),
            "rustfsadmin",
            "rustfsadmin",
        );

        store
            .connect()
            .unwrap()
            .execute_batch("CREATE TABLE people AS SELECT 1 AS id")
            .unwrap();
        let id: i64 = store
            .connect()
            .unwrap()
            .query_row("SELECT id FROM people", [], |row| row.get(0))
            .unwrap();

        assert_eq!(id, 1);
    }

    #[test]
    fn a_store_naming_no_database_builds_in_memory() {
        store()
            .connect()
            .unwrap()
            .execute_batch("CREATE TABLE people AS SELECT 1 AS id")
            .unwrap();

        assert!(
            store()
                .connect()
                .unwrap()
                .execute_batch("SELECT id FROM people")
                .is_err()
        );
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
        let google = ObjectStore::open(
            &Config::new(None, s3("https://storage.googleapis.com")),
            "key",
            "secret",
        );

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
