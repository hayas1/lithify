use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use lithify_config::Pipeline;
use lithify_config::infra::{Sink, Source};
use lithify_protocol::sink::bigquery;
use lithify_protocol::sink::bigquery::client::{BigQueryCapture, BigQueryClient};
use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbClient};
use lithify_protocol::sink::duckdb::config::Config;
use lithify_protocol::sink::duckdb::store::ObjectStore;

use crate::catch_up::CatchUp;
use crate::credential::Credential;
use crate::journal::Journal;
use crate::pipeline::{Capture, Warehouse};
use crate::plan::SnapshotPlan;
use crate::settlement::{Build, DryRun, Settlement};
use crate::unrunnable::Unrunnable;

pub struct Run {
    directory: PathBuf,
    credential: Credential,
    sink: Option<String>,
    onto: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
}

impl Run {
    pub fn new(
        directory: PathBuf,
        credential: Credential,
        sink: Option<String>,
        onto: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            directory,
            credential,
            sink,
            onto,
            now,
        }
    }

    pub fn plan(&self, into: &Path) -> Result<Journal, Unrunnable> {
        self.each_sink(|sink, source, topic_prefix, journal| {
            self.settle(
                sink,
                source,
                topic_prefix,
                &DryRun::new(into, sink.name()),
                journal,
            )
        })
    }

    pub fn build(&self) -> Result<Journal, Unrunnable> {
        self.each_sink(|sink, source, topic_prefix, journal| {
            self.settle(
                sink,
                source,
                topic_prefix,
                &Build::new(sink, source)?,
                journal,
            )
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

    fn settle<S: Settlement>(
        &self,
        sink: &Sink<Warehouse>,
        source: &Source<Capture>,
        topic_prefix: &str,
        settlement: &S,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        let catch_up = CatchUp::new(sink, source, self.onto, self.now);

        match sink.warehouse() {
            Warehouse::DuckDb(config) => {
                let client = self.duckdb(sink, config)?;

                catch_up.settle(&client, settlement, journal, |window, base| {
                    source.capture().with_duckdb_change_logs(
                        topic_prefix,
                        SnapshotPlan::new(&client, sink, window, base, settlement),
                    )
                })
            }
            Warehouse::BigQuery(config) => {
                let client = self.bigquery(sink, config)?;

                catch_up.settle(&client, settlement, journal, |window, base| {
                    source.capture().with_bigquery_change_logs(
                        topic_prefix,
                        SnapshotPlan::new(&client, sink, window, base, settlement),
                    )
                })
            }
        }
    }

    fn duckdb(&self, sink: &Sink<Warehouse>, config: &Config) -> Result<DuckDbClient, Unrunnable> {
        Ok(
            ObjectStore::open(config, self.credential.key_id(), self.credential.secret())
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
