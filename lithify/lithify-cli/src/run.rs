use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use lithify_config::infra::{Sink, Source};
use lithify_config::{ArtifactKind, Pipeline};
use lithify_core::sink::OpenWarehouse;
use lithify_core::ts::Ts;
use lithify_protocol::sink::bigquery;
use lithify_protocol::sink::bigquery::client::{BigQueryCapture, BigQueryClient};
use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbClient};
use lithify_protocol::sink::duckdb::config::Config;
use lithify_protocol::{BigQueryWarehouse, DuckDbWarehouse};

use crate::course::Course;
use crate::credential::Credentials;
use crate::journal::Journal;
use crate::pipeline::{Capture, WarehouseConfig};
use crate::plan::SnapshotPlan;
use crate::settlement::{Build, DryRun, Settlement};
use crate::unrunnable::Unrunnable;
use lithify_core::check::{Expectation, Inspection};

pub struct Run {
    directory: PathBuf,
    credentials: Credentials,
    sink: Option<String>,
    now: DateTime<Utc>,
}

impl Run {
    pub fn new(
        directory: PathBuf,
        credentials: Credentials,
        sink: Option<String>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            directory,
            credentials,
            sink,
            now,
        }
    }

    pub fn plan(&self, course: &Course, into: &Path) -> Result<Journal, Unrunnable> {
        self.each_sink(|sink, source, topic_prefix, journal| {
            self.settle(
                course,
                sink,
                source,
                topic_prefix,
                &DryRun::new(into, sink.name()),
                journal,
            )
        })
    }

    pub fn build(&self, course: &Course) -> Result<Journal, Unrunnable> {
        self.each_sink(|sink, source, topic_prefix, journal| {
            self.settle(
                course,
                sink,
                source,
                topic_prefix,
                &Build::new(sink, source, self.now)?,
                journal,
            )
        })
    }

    pub fn check(&self) -> Result<Journal, Unrunnable> {
        self.each_sink(|sink, source, _, journal| {
            for discrepancy in self.inspection(sink, source)?.discrepancies() {
                journal.discrepancy(discrepancy);
            }

            Ok(())
        })
    }

    fn inspection(
        &self,
        sink: &Sink<WarehouseConfig>,
        source: &Source<Capture>,
    ) -> Result<Inspection, Unrunnable> {
        let expectation = Expectation::new(
            sink.artifacts().schedule(),
            source.since(),
            &source.capture().to_json()?,
            Ts::from(self.now),
        );
        let namespace = sink.artifacts().namespace(ArtifactKind::Snapshot);

        match sink.warehouse() {
            WarehouseConfig::DuckDb(config) => {
                Ok(expectation.read(&self.duckdb(sink, config)?, namespace)?)
            }
            WarehouseConfig::BigQuery(config) => {
                Ok(expectation.read(&self.bigquery(sink, config)?, namespace)?)
            }
        }
    }

    fn source(
        pipeline: &Pipeline<Capture, WarehouseConfig>,
    ) -> Result<&Source<Capture>, Unrunnable> {
        let mut sources = pipeline.sources();
        let source = sources.next().ok_or(Unrunnable::WithoutSource)?;

        if sources.next().is_some() {
            todo!("order the change log across the boundary between connection eras");
        }

        Ok(source)
    }

    fn each_sink(
        &self,
        mut act: impl FnMut(
            &Sink<WarehouseConfig>,
            &Source<Capture>,
            &str,
            &mut Journal,
        ) -> Result<(), Unrunnable>,
    ) -> Result<Journal, Unrunnable> {
        let pipeline: Pipeline<Capture, WarehouseConfig> = Pipeline::open(&self.directory)?;
        let source = Self::source(&pipeline)?;
        let mut journal = Journal::default();

        for sink in self.selected(&pipeline)? {
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
        course: &Course,
        sink: &Sink<WarehouseConfig>,
        source: &Source<Capture>,
        topic_prefix: &str,
        settlement: &S,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        match sink.warehouse() {
            WarehouseConfig::DuckDb(config) => {
                let client = self.duckdb(sink, config)?;

                course.itinerary(sink, source, self.now, &client)?.settle(
                    settlement,
                    journal,
                    |window, previous, tables| {
                        source.capture().with_duckdb_change_logs(
                            topic_prefix,
                            SnapshotPlan::new(&client, sink, window, previous, tables, settlement),
                        )
                    },
                )
            }
            WarehouseConfig::BigQuery(config) => {
                let client = self.bigquery(sink, config)?;

                course.itinerary(sink, source, self.now, &client)?.settle(
                    settlement,
                    journal,
                    |window, previous, tables| {
                        source.capture().with_bigquery_change_logs(
                            topic_prefix,
                            SnapshotPlan::new(&client, sink, window, previous, tables, settlement),
                        )
                    },
                )
            }
        }
    }

    fn duckdb(
        &self,
        sink: &Sink<WarehouseConfig>,
        config: &Config,
    ) -> Result<DuckDbClient, Unrunnable> {
        Ok(DuckDbWarehouse::open(
            config,
            self.credentials.duckdb()?,
            sink.change_log(),
        )?)
    }

    fn bigquery(
        &self,
        sink: &Sink<WarehouseConfig>,
        config: &bigquery::config::Config,
    ) -> Result<BigQueryClient, Unrunnable> {
        Ok(BigQueryWarehouse::open(config, &(), sink.change_log())?)
    }

    fn selected<'a>(
        &self,
        pipeline: &'a Pipeline<Capture, WarehouseConfig>,
    ) -> Result<Vec<&'a Sink<WarehouseConfig>>, Unrunnable> {
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
