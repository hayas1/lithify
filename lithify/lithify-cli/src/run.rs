use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use lithify_config::Pipeline;
use lithify_config::infra::{Sink, Source};
use lithify_core::sql::SqlString;
use lithify_core::window::Window;
use lithify_protocol::sink::bigquery;
use lithify_protocol::sink::bigquery::client::{BigQueryCapture, BigQueryClient};
use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbClient};
use lithify_protocol::sink::duckdb::config::Store;
use lithify_protocol::sink::duckdb::store::ObjectStore;

use crate::credential::Credential;
use crate::journal::Journal;
use crate::pipeline::{Capture, Warehouse};
use crate::plan::SnapshotPlan;
use crate::unrunnable::Unrunnable;

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

    pub fn plan(&self, into: &Path) -> Result<Journal, Unrunnable> {
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
        ) -> Result<(), Unrunnable>,
    ) -> Result<Journal, Unrunnable> {
        let pipeline: Pipeline<Capture, Warehouse> = Pipeline::open(&self.directory)?;
        let mut sources = pipeline.sources();
        let source = sources.next().ok_or(Unrunnable::WithoutSource)?;

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

            if let Err(unrunnable) = act(sink, source, topic_prefix, &mut journal) {
                journal.failed(sink.name(), &unrunnable);
            }
        }

        Ok(journal)
    }

    fn plan_statements(
        &self,
        sink: &Sink<Warehouse>,
        source: &Source<Capture>,
        topic_prefix: &str,
    ) -> Result<Vec<SqlString>, Unrunnable> {
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
    ) -> Result<(), Unrunnable> {
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

    fn duckdb(&self, sink: &Sink<Warehouse>, store: &Store) -> Result<DuckDbClient, Unrunnable> {
        Ok(
            ObjectStore::open(store, self.credential.key_id(), self.credential.secret())
                .client(sink.change_log())?,
        )
    }

    fn bigquery(
        &self,
        sink: &Sink<Warehouse>,
        config: &bigquery::config::Config,
    ) -> Result<BigQueryClient, Unrunnable> {
        Ok(BigQueryClient::open(config, sink.change_log())?)
    }

    fn sinks<'a>(
        &self,
        pipeline: &'a Pipeline<Capture, Warehouse>,
    ) -> Result<Vec<&'a Sink<Warehouse>>, Unrunnable> {
        let sinks: Vec<_> = pipeline
            .sinks()
            .filter(|sink| match &self.sink {
                Some(named) => named == sink.name(),
                None => true,
            })
            .collect();

        match (sinks.is_empty(), &self.sink) {
            (true, Some(named)) => Err(Unrunnable::UnknownSink(named.clone())),
            _ => Ok(sinks),
        }
    }
}
