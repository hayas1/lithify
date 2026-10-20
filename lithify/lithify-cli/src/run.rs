use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use lithify_config::Pipeline;
use lithify_config::infra::{Sink, Source};
use lithify_config::table;
use lithify_core::sink::OpenWarehouse;
use lithify_core::ts::Ts;
use lithify_protocol::sink::bigquery::client::BigQueryClient;
use lithify_protocol::sink::duckdb::client::DuckDbClient;
use lithify_protocol::sink::duckdb::config::Config;
use lithify_protocol::sink::{bigquery, duckdb};
use lithify_protocol::{BigQueryWarehouse, DuckDbWarehouse};

use crate::artifacts::{ArtifactConfig, DeclaredSnapshot};
use crate::command::RunCommand;
use crate::credential::Credentials;
use crate::journal::Journal;
use crate::pipeline::{CaptureConfig, Reached, WarehouseConfig};
use crate::plan::Planning;
use crate::report::{DryRun, Report};
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

    pub fn plan(&self, command: &RunCommand, into: &Path) -> Result<Journal, Unrunnable> {
        self.each_sink(|sink, source, listing, journal| {
            self.settle(
                command,
                sink,
                source,
                listing,
                &DryRun::new(into, sink.name()),
                journal,
            )
        })
    }

    pub fn build(&self, command: &RunCommand) -> Result<Journal, Unrunnable> {
        self.each_sink(|sink, source, listing, journal| {
            self.settle(
                command,
                sink,
                source,
                listing,
                &DeclaredSnapshot::of(sink)?.build(source, self.now)?,
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
        let (namespace, series) = (snapshot.namespace(), snapshot.series());

        match Self::reach(sink)? {
            Reached::DuckDb(config, change_log) => expectation.read::<_, Unrunnable>(
                &self.duckdb(config, change_log)?,
                namespace,
                &series,
            ),
            Reached::BigQuery(config, change_log) => expectation.read::<_, Unrunnable>(
                &self.bigquery(config, change_log)?,
                namespace,
                &series,
            ),
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
            &table::Tables,
            &mut Journal,
        ) -> Result<(), Unrunnable>,
    ) -> Result<Journal, Unrunnable> {
        let pipeline: Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> =
            Pipeline::open(&self.directory)?;
        let source = Self::source(&pipeline)?;
        let mut journal = Journal::default();

        for sink in self.selected(&pipeline)? {
            journal.sink(sink.name());

            if let Err(unrunnable) = act(sink, source, pipeline.tables(), &mut journal) {
                journal.failed(sink.name(), &unrunnable);
            }
        }

        Ok(journal)
    }

    fn settle<R: Report>(
        &self,
        command: &RunCommand,
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        source: &Source<CaptureConfig>,
        listing: &table::Tables,
        runner: &R,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        let Some(topic_prefix) = source.topic_prefix() else {
            journal.note(&format!(
                "{} names no topic prefix to read its change logs by",
                source.name()
            ));

            return Ok(());
        };

        match Self::reach(sink)? {
            Reached::DuckDb(config, change_log) => {
                let client = self.duckdb(config, change_log)?;

                command.settle(
                    sink,
                    source,
                    self.now,
                    &Planning::new(&client, source.capture(), topic_prefix, listing),
                    runner,
                    journal,
                )
            }
            Reached::BigQuery(config, change_log) => {
                let client = self.bigquery(config, change_log)?;

                command.settle(
                    sink,
                    source,
                    self.now,
                    &Planning::new(&client, source.capture(), topic_prefix, listing),
                    runner,
                    journal,
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
