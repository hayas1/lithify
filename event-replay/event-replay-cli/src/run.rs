use std::path::PathBuf;

use event_replay_config::Host;
use event_replay_config::infra::{
    BigQuery, Credentials, Dialect, Format, S3, Sink, Source, Store, Warehouse,
};
use event_replay_core::listener::CatalogModel;
use event_replay_core::sink::TablePath;
use event_replay_core::source::SchemaChangeStream;
use event_replay_core::window::Window;
use event_replay_protocol::sink::bigquery::client::BigQueryClient;
use event_replay_protocol::sink::duckdb::store::{FileFormat, ObjectStore};
use event_replay_protocol::source::debezium::Debezium;
use event_replay_protocol::{MySql, Postgres};

use crate::credential::Credential;
use crate::journal::Journal;
use crate::unreplayable::Unreplayable;

pub struct Run {
    directory: PathBuf,
    window: Window,
    credential: Credential,
    sink: Option<String>,
}

impl Run {
    pub fn new(
        directory: PathBuf,
        window: Window,
        credential: Credential,
        sink: Option<String>,
    ) -> Self {
        Self {
            directory,
            window,
            credential,
            sink,
        }
    }

    pub fn report(&self) -> Result<Journal, Unreplayable> {
        let host = Host::open(&self.directory)?;
        let mut sources = host.sources();
        let source = sources.next().ok_or(Unreplayable::WithoutSource)?;

        if sources.next().is_some() {
            todo!("order the change log across the boundary between connection eras");
        }

        let mut journal = Journal::default();

        for sink in self.sinks(&host)? {
            journal.sink(sink.name());

            if let Err(unreplayable) = self.read(sink, source, &mut journal) {
                journal.failed(sink.name(), &unreplayable);
            }
        }

        Ok(journal)
    }

    fn read(
        &self,
        sink: &Sink,
        source: &Source,
        journal: &mut Journal,
    ) -> Result<(), Unreplayable> {
        let Some(table) = sink.schema_changes(source) else {
            journal.note(&format!("{} lands no schema changes", source.name()));

            return Ok(());
        };

        match sink.warehouse() {
            Warehouse::DuckDb(Store::S3(store)) => {
                self.read_duckdb(&self.store(store), &table, source, journal)
            }
            Warehouse::BigQuery(warehouse) => {
                self.read_bigquery(&self.client(warehouse)?, &table, source, journal)
            }
        }
    }

    fn read_duckdb(
        &self,
        store: &ObjectStore,
        table: &TablePath,
        source: &Source,
        journal: &mut Journal,
    ) -> Result<(), Unreplayable> {
        let client = store.client(table)?;

        match source.capture().dialect() {
            Dialect::MySql => client.catalog(
                &Debezium::new(MySql),
                table,
                &self.window,
                &mut CatalogModel::default(),
                journal,
            ),
            Dialect::Postgres => client.catalog(
                &Debezium::new(Postgres),
                table,
                &self.window,
                &mut CatalogModel::default(),
                journal,
            ),
        }
    }

    fn read_bigquery(
        &self,
        client: &BigQueryClient,
        table: &TablePath,
        source: &Source,
        journal: &mut Journal,
    ) -> Result<(), Unreplayable> {
        match source.capture().dialect() {
            Dialect::MySql => client.catalog(
                &Debezium::new(MySql),
                table,
                &self.window,
                &mut CatalogModel::default(),
                journal,
            ),
            Dialect::Postgres => client.catalog(
                &Debezium::new(Postgres),
                table,
                &self.window,
                &mut CatalogModel::default(),
                journal,
            ),
        }
    }

    fn store(&self, store: &S3) -> ObjectStore {
        ObjectStore::new(
            store.endpoint(),
            store.region(),
            store.bucket(),
            match store.format() {
                Format::Parquet => FileFormat::Parquet,
            },
            self.credential.key_id(),
            self.credential.secret(),
        )
    }

    fn client(&self, warehouse: &BigQuery) -> Result<BigQueryClient, Unreplayable> {
        Ok(BigQueryClient::new(
            warehouse.project(),
            warehouse.endpoint(),
            warehouse.credentials() == Credentials::Anonymous,
        )?)
    }

    fn sinks<'a>(&self, host: &'a Host) -> Result<Vec<&'a Sink>, Unreplayable> {
        let sinks: Vec<_> = host
            .sinks()
            .filter(|sink| match &self.sink {
                Some(named) => named == sink.name(),
                None => true,
            })
            .collect();

        match (sinks.is_empty(), &self.sink) {
            (true, Some(named)) => Err(Unreplayable::UnknownSink(named.clone())),
            _ => Ok(sinks),
        }
    }
}
