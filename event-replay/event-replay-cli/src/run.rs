use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use event_replay_config::Pipeline;
use event_replay_config::infra::{Sink, Source};
use event_replay_core::listener::CatalogModel;
use event_replay_core::source::{ChangeLogs, SchemaChangeLog, SchemaChangeStream};
use event_replay_core::sql::SqlString;
use event_replay_core::window::Window;
use event_replay_protocol::sink::bigquery;
use event_replay_protocol::sink::bigquery::client::{
    BigQueryCapture, BigQueryChangeLogsFn, BigQueryClient,
};
use event_replay_protocol::sink::duckdb::client::{
    DuckDbCapture, DuckDbChangeLogsFn, DuckDbClient,
};
use event_replay_protocol::sink::duckdb::config::Store;
use event_replay_protocol::sink::duckdb::store::ObjectStore;
use google_cloud_bigquery::error::RowError;
use google_cloud_bigquery::query::Row;

use crate::credential::Credential;
use crate::journal::Journal;
use crate::pipeline::{Capture, Warehouse};
use crate::plan::SnapshotPlan;
use crate::unreplayable::Unreplayable;

pub struct Run {
    directory: PathBuf,
    window: Window,
    credential: Credential,
    sink: Option<String>,
    onto: Option<DateTime<Utc>>,
}

impl Run {
    pub fn new(
        directory: PathBuf,
        window: Window,
        credential: Credential,
        sink: Option<String>,
        onto: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            directory,
            window,
            credential,
            sink,
            onto,
        }
    }

    pub fn report(&self) -> Result<Journal, Unreplayable> {
        self.each_sink(|sink, source, topic_prefix, journal| {
            self.read(sink, source, topic_prefix, journal)
        })
    }

    pub fn plan(&self, into: &Path) -> Result<Journal, Unreplayable> {
        self.each_sink(|sink, source, topic_prefix, journal| {
            let statements = self.plan_statements(sink, source, topic_prefix)?;

            Self::write_plan(into, sink, &statements, journal)
        })
    }

    fn each_sink(
        &self,
        mut act: impl FnMut(
            &Sink<Warehouse>,
            &Source<Capture>,
            &str,
            &mut Journal,
        ) -> Result<(), Unreplayable>,
    ) -> Result<Journal, Unreplayable> {
        let pipeline: Pipeline<Capture, Warehouse> = Pipeline::open(&self.directory)?;
        let mut sources = pipeline.sources();
        let source = sources.next().ok_or(Unreplayable::WithoutSource)?;

        if sources.next().is_some() {
            todo!("order the change log across the boundary between connection eras");
        }

        let mut journal = Journal::default();

        for sink in self.sinks(&pipeline)? {
            journal.sink(sink.name());

            let Some(topic_prefix) = source.topic_prefix() else {
                journal.note(&format!(
                    "{} names no topic prefix to read its change logs by",
                    source.name()
                ));

                continue;
            };

            if let Err(unreplayable) = act(sink, source, topic_prefix, &mut journal) {
                journal.failed(sink.name(), &unreplayable);
            }
        }

        Ok(journal)
    }

    fn plan_statements(
        &self,
        sink: &Sink<Warehouse>,
        source: &Source<Capture>,
        topic_prefix: &str,
    ) -> Result<Vec<SqlString>, Unreplayable> {
        match sink.warehouse() {
            Warehouse::DuckDb(store) => source.capture().with_duckdb_change_logs(
                topic_prefix,
                SnapshotPlan::new(&self.duckdb(sink, store)?, sink, &self.window, self.onto),
            ),
            Warehouse::BigQuery(config) => source.capture().with_bigquery_change_logs(
                topic_prefix,
                SnapshotPlan::new(&self.bigquery(sink, config)?, sink, &self.window, self.onto),
            ),
        }
    }

    fn write_plan(
        into: &Path,
        sink: &Sink<Warehouse>,
        statements: &[SqlString],
        journal: &mut Journal,
    ) -> Result<(), Unreplayable> {
        let path = into.join(format!("{}.sql", sink.name()));

        std::fs::create_dir_all(into)?;
        std::fs::write(
            &path,
            statements
                .iter()
                .map(|statement| format!("{statement};\n"))
                .collect::<Vec<_>>()
                .join("\n"),
        )?;
        journal.note(&format!(
            "wrote {} statements to {}",
            statements.len(),
            path.display()
        ));

        Ok(())
    }

    fn duckdb(&self, sink: &Sink<Warehouse>, store: &Store) -> Result<DuckDbClient, Unreplayable> {
        Ok(
            ObjectStore::open(store, self.credential.key_id(), self.credential.secret())
                .client(sink.change_log())?,
        )
    }

    fn bigquery(
        &self,
        sink: &Sink<Warehouse>,
        config: &bigquery::config::Config,
    ) -> Result<BigQueryClient, Unreplayable> {
        Ok(BigQueryClient::open(config, sink.change_log())?)
    }

    fn read(
        &self,
        sink: &Sink<Warehouse>,
        source: &Source<Capture>,
        topic_prefix: &str,
        journal: &mut Journal,
    ) -> Result<(), Unreplayable> {
        match sink.warehouse() {
            Warehouse::DuckDb(store) => {
                self.read_duckdb(&self.duckdb(sink, store)?, topic_prefix, source, journal)
            }
            Warehouse::BigQuery(config) => {
                self.read_bigquery(&self.bigquery(sink, config)?, topic_prefix, source, journal)
            }
        }
    }

    fn read_duckdb(
        &self,
        client: &DuckDbClient,
        topic_prefix: &str,
        source: &Source<Capture>,
        journal: &mut Journal,
    ) -> Result<(), Unreplayable> {
        source
            .capture()
            .with_duckdb_change_logs(topic_prefix, Catalog::new(client, &self.window, journal))
    }

    fn read_bigquery(
        &self,
        client: &BigQueryClient,
        topic_prefix: &str,
        source: &Source<Capture>,
        journal: &mut Journal,
    ) -> Result<(), Unreplayable> {
        source
            .capture()
            .with_bigquery_change_logs(topic_prefix, Catalog::new(client, &self.window, journal))
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

struct Catalog<'a, C> {
    reader: &'a C,
    window: &'a Window,
    journal: &'a mut Journal,
}

impl<'a, C> Catalog<'a, C> {
    fn new(reader: &'a C, window: &'a Window, journal: &'a mut Journal) -> Self {
        Self {
            reader,
            window,
            journal,
        }
    }
}

impl DuckDbChangeLogsFn for Catalog<'_, DuckDbClient> {
    type Output = Result<(), Unreplayable>;

    fn call_duckdb<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: duckdb::types::FromSql,
    {
        self.reader.catalog::<Unreplayable, Journal>(
            &(),
            logs.schema_changes(),
            self.window,
            &mut CatalogModel::default(),
            self.journal,
        )
    }
}

impl BigQueryChangeLogsFn for Catalog<'_, BigQueryClient> {
    type Output = Result<(), Unreplayable>;

    fn call_bigquery<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: TryFrom<Row, Error = RowError>,
    {
        self.reader.catalog::<Unreplayable, Journal>(
            &(),
            logs.schema_changes(),
            self.window,
            &mut CatalogModel::default(),
            self.journal,
        )
    }
}
