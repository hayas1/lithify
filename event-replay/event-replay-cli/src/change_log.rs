use duckdb::Connection;
use duckdb::types::FromSql;

use event_replay_config::Host;
use event_replay_config::infra::{Dialect as CapturedAs, Format, S3, Sink, Source, Store};
use event_replay_core::listener::CatalogModel;
use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::source::{SchemaChangeStream, SchemaChanges};
use event_replay_core::window::Window;
use event_replay_protocol::sink::duckdb::client::DuckDbClient;
use event_replay_protocol::source::debezium::{Debezium, DebeziumDdl, Dialect};
use event_replay_protocol::{DuckDb, MySql, Postgres};

use crate::credential::Credential;
use crate::journal::Journal;
use crate::unreplayable::Unreplayable;

pub struct ChangeLogFiles<'a> {
    host: &'a Host,
    store: &'a S3,
    sink: &'a Sink,
    credential: &'a Credential,
}

impl<'a> ChangeLogFiles<'a> {
    pub fn of(host: &'a Host, credential: &'a Credential) -> Option<Self> {
        host.sinks()
            .find_map(|sink| match sink.warehouse().as_duck_db()? {
                Store::S3(store) => Some(Self {
                    host,
                    store,
                    sink,
                    credential,
                }),
            })
    }

    pub fn report(&self, window: &Window) -> Result<Journal, Unreplayable> {
        let mut sources = self.host.sources();
        let source = sources.next().expect("a captured source");

        if sources.next().is_some() {
            todo!("order the change log across the boundary between connection eras");
        }

        match source.capture().dialect() {
            CapturedAs::MySql => self.catalog(source, &Debezium::new(MySql), window),
            CapturedAs::Postgres => self.catalog(source, &Debezium::new(Postgres), window),
        }
    }

    fn catalog<D: Dialect>(
        &self,
        source: &Source,
        log: &Debezium<D>,
        window: &Window,
    ) -> Result<Journal, Unreplayable>
    where
        DebeziumDdl<D::Source>: FromSql,
    {
        let mut journal = Journal::default();

        let Some(table) = self.sink.schema_changes(source) else {
            journal.note(&format!("{} lands no schema changes", source.name()));

            return Ok(journal);
        };

        let client = self.client(&table)?;
        let mut catalog = CatalogModel::default();
        let mut changes = client.schema_changes(log, &table, window)?;

        for change in changes.stream()? {
            catalog.accept(&change?, &mut journal)?;
        }

        Ok(journal)
    }

    fn client(&self, table: &TablePath) -> Result<DuckDbClient, Unreplayable> {
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
            self.credential.key_id(),
            self.credential.secret(),
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
            DuckDb.identifier(table.namespace()),
            DuckDb.table_ref(table),
            self.scan(table),
        )
    }

    fn scan(&self, table: &TablePath) -> String {
        match self.store.format() {
            Format::Parquet => format!("read_parquet('{}')", self.files(table)),
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

    fn files() -> ChangeLogFiles<'static> {
        static HOST: std::sync::OnceLock<Host> = std::sync::OnceLock::new();
        static CREDENTIAL: std::sync::OnceLock<Credential> = std::sync::OnceLock::new();

        let host = HOST.get_or_init(|| {
            Host::open(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../compose/event-replay/mysql"),
            )
            .unwrap()
        });

        ChangeLogFiles::of(
            host,
            CREDENTIAL.get_or_init(|| {
                Credential::new("minioadmin".to_owned(), "minioadmin".parse().unwrap())
            }),
        )
        .unwrap()
    }

    fn table() -> TablePath {
        TablePath::new("mysql_shop_cdc", "mysql")
    }

    #[test]
    fn a_host_sunk_into_object_storage_is_read_from_the_files_the_sink_wrote() {
        assert_eq!(
            files().files(&table()),
            "s3://event-replay/mysql_shop_cdc/mysql/*.parquet"
        );
    }

    #[test]
    fn the_change_log_of_a_table_is_scanned_in_the_format_it_was_written_in() {
        assert_eq!(
            files().view(&table()),
            concat!(
                "CREATE SCHEMA IF NOT EXISTS \"mysql_shop_cdc\";\n",
                "CREATE VIEW \"mysql_shop_cdc\".\"mysql\" AS SELECT * FROM ",
                "read_parquet('s3://event-replay/mysql_shop_cdc/mysql/*.parquet')"
            )
        );
    }

    #[test]
    fn object_storage_reached_without_tls_is_read_as_such() {
        assert_eq!(files().endpoint(), ("minio:9000", false));
    }

    #[test]
    fn object_storage_is_reached_with_the_credential_the_run_carries() {
        let secret = files().secret();

        assert!(secret.contains("KEY_ID 'minioadmin', SECRET 'minioadmin'"));
        assert!(secret.contains("REGION 'us-east-1', ENDPOINT 'minio:9000', USE_SSL false"));
    }
}
