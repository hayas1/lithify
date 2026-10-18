use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use lithify_config::Pipeline;
use lithify_config::infra::{Sink, Source};
use lithify_core::sink::OpenWarehouse;
use lithify_core::ts::Ts;
use lithify_protocol::sink::bigquery::client::{BigQueryCapture, BigQueryClient};
use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbClient};
use lithify_protocol::sink::duckdb::config::Config;
use lithify_protocol::sink::{bigquery, duckdb};
use lithify_protocol::{BigQueryWarehouse, DuckDbWarehouse};

use crate::artifacts::{ArtifactConfig, DeclaredSnapshot};
use crate::course::Course;
use crate::credential::Credentials;
use crate::journal::Journal;
use crate::pipeline::{CaptureConfig, Reached, WarehouseConfig};
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
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        source: &Source<CaptureConfig>,
    ) -> Result<Inspection, Unrunnable> {
        let snapshot = DeclaredSnapshot::of(sink)?;
        let expectation = Expectation::new(
            snapshot.schedule(),
            source.since(),
            &source.capture().to_json()?,
            Ts::from(self.now),
        );
        let namespace = snapshot.namespace();

        match Self::reach(sink)? {
            Reached::DuckDb(config, change_log) => {
                Ok(expectation.read(&self.duckdb(config, change_log)?, namespace)?)
            }
            Reached::BigQuery(config, change_log) => {
                Ok(expectation.read(&self.bigquery(config, change_log)?, namespace)?)
            }
        }
    }

    fn source(
        pipeline: &Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
    ) -> Result<&Source<CaptureConfig>, Unrunnable> {
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
            &Sink<WarehouseConfig, ArtifactConfig>,
            &Source<CaptureConfig>,
            &str,
            &mut Journal,
        ) -> Result<(), Unrunnable>,
    ) -> Result<Journal, Unrunnable> {
        let pipeline: Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> =
            Pipeline::open(&self.directory)?;
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
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        source: &Source<CaptureConfig>,
        topic_prefix: &str,
        settlement: &S,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        match Self::reach(sink)? {
            Reached::DuckDb(config, change_log) => {
                let client = self.duckdb(config, change_log)?;

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
            Reached::BigQuery(config, change_log) => {
                let client = self.bigquery(config, change_log)?;

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

    fn reach(sink: &Sink<WarehouseConfig, ArtifactConfig>) -> Result<Reached<'_>, Unrunnable> {
        sink.warehouse()
            .reach(sink.change_log())
            .ok_or(Unrunnable::Unreached)
    }

    fn duckdb(
        &self,
        config: &Config,
        change_log: &duckdb::config::Location,
    ) -> Result<DuckDbClient, Unrunnable> {
        Ok(DuckDbWarehouse::open(
            config,
            self.credentials.duckdb()?,
            change_log,
        )?)
    }

    fn bigquery(
        &self,
        config: &bigquery::config::Config,
        change_log: &bigquery::config::Location,
    ) -> Result<BigQueryClient, Unrunnable> {
        Ok(BigQueryWarehouse::open(config, &(), change_log)?)
    }

    fn selected<'a>(
        &self,
        pipeline: &'a Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
    ) -> Result<Vec<&'a Sink<WarehouseConfig, ArtifactConfig>>, Unrunnable> {
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
