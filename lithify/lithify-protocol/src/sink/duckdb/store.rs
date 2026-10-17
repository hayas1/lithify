use std::convert::Infallible;
use std::fmt::{self, Debug, Formatter};
use std::str::FromStr;

use duckdb::Connection;

use lithify_core::sink::OpenWarehouse;
use lithify_core::sql::SqlString;

use crate::sink::duckdb::DuckDbWarehouse;
use crate::sink::duckdb::client::DuckDbClient;
use crate::sink::duckdb::config::Config;

pub struct S3Credential {
    key_id: String,
    secret: Secret,
}

impl S3Credential {
    pub fn new(key_id: String, secret: Secret) -> Self {
        Self { key_id, secret }
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn secret(&self) -> &str {
        self.secret.read()
    }
}

#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    pub fn read(&self) -> &str {
        &self.0
    }
}

impl Debug for Secret {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("***")
    }
}

impl FromStr for Secret {
    type Err = Infallible;

    fn from_str(secret: &str) -> Result<Self, Self::Err> {
        Ok(Self(secret.to_owned()))
    }
}

impl OpenWarehouse for DuckDbWarehouse {
    type Config = Config;
    type Client = DuckDbClient;
    type Credential = S3Credential;
    type Error = duckdb::Error;

    fn open(
        config: &Config,
        credential: &S3Credential,
        change_log: &str,
    ) -> Result<DuckDbClient, duckdb::Error> {
        ObjectStore::open(config, credential.key_id(), credential.secret()).client(change_log)
    }
}

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

    #[test]
    fn a_secret_is_not_printed_where_it_is_read() {
        let secret: Secret = "rustfsadmin".parse().unwrap();

        assert_eq!(format!("{secret:?}"), "***");
        assert_eq!(
            S3Credential::new("key".to_owned(), secret).secret(),
            "rustfsadmin"
        );
    }
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
