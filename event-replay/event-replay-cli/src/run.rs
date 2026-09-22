use std::path::PathBuf;

use event_replay_config::Pipeline;
use event_replay_config::infra::{Sink, Source};
use event_replay_core::listener::CatalogModel;
use event_replay_core::sink::TablePath;
use event_replay_core::source::SchemaChangeStream;
use event_replay_core::window::Window;
use event_replay_protocol::sink::bigquery::client::BigQueryClient;
use event_replay_protocol::sink::duckdb::store::ObjectStore;
use event_replay_protocol::source::debezium::Debezium;
use event_replay_protocol::source::debezium::config::Dialect;
use event_replay_protocol::{MySql, Postgres};

use crate::credential::Credential;
use crate::journal::Journal;
use crate::pipeline::{Capture, Warehouse};
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
        let pipeline: Pipeline<Capture, Warehouse> = Pipeline::open(&self.directory)?;
        let mut sources = pipeline.sources();
        let source = sources.next().ok_or(Unreplayable::WithoutSource)?;

        if sources.next().is_some() {
            todo!("order the change log across the boundary between connection eras");
        }

        let mut journal = Journal::default();

        for sink in self.sinks(&pipeline)? {
            journal.sink(sink.name());

            if let Err(unreplayable) = self.read(sink, source, &mut journal) {
                journal.failed(sink.name(), &unreplayable);
            }
        }

        Ok(journal)
    }

    fn read(
        &self,
        sink: &Sink<Warehouse>,
        source: &Source<Capture>,
        journal: &mut Journal,
    ) -> Result<(), Unreplayable> {
        let Some(table) = sink.schema_changes(source) else {
            journal.note(&format!("{} lands no schema changes", source.name()));

            return Ok(());
        };

        match sink.warehouse() {
            Warehouse::DuckDb(store) => {
                let store =
                    ObjectStore::open(store, self.credential.key_id(), self.credential.secret());

                self.read_duckdb(&store, &table, source, journal)
            }
            Warehouse::BigQuery(config) => {
                self.read_bigquery(&BigQueryClient::open(config)?, &table, source, journal)
            }
        }
    }

    // TODO: the dialect is matched inside each of the two readers below, because
    // the conversion a client requires is settled per warehouse and dialect pair,
    // and a method generic over the dialect would ask every caller for both
    // warehouses' conversions. Double dispatch removes the repetition: a visitor
    // with one method per dialect, matched where the dialects live, in
    // event-replay-protocol.
    fn read_duckdb(
        &self,
        store: &ObjectStore,
        table: &TablePath,
        source: &Source<Capture>,
        journal: &mut Journal,
    ) -> Result<(), Unreplayable> {
        let client = store.client(table)?;

        let Capture::Debezium(debezium) = source.capture();

        match debezium.dialect() {
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
        source: &Source<Capture>,
        journal: &mut Journal,
    ) -> Result<(), Unreplayable> {
        let Capture::Debezium(debezium) = source.capture();

        match debezium.dialect() {
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

    fn sinks<'a>(
        &self,
        pipeline: &'a Pipeline<Capture, Warehouse>,
    ) -> Result<Vec<&'a Sink<Warehouse>>, Unreplayable> {
        let sinks: Vec<_> = pipeline
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
