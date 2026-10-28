use std::path::{Path, PathBuf};

use lithify_core::sink::OpenWarehouse;
use lithify_core::ts::{Replay, Ts};
use lithify_pipeline::Pipeline;
use lithify_pipeline::infra::{Sink, Source};
use lithify_pipeline::table;
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
use crate::report::{DryRunBuildLogWrite, Report};
use crate::step::Taking;
use crate::unrunnable::Unrunnable;
use lithify_pipeline::check::{Expectation, Inspection};

pub struct Run {
    directory: PathBuf,
    credentials: Credentials,
    sink: Option<String>,
    now: Ts<Replay>,
}

impl Run {
    pub fn new(
        directory: PathBuf,
        credentials: Credentials,
        sink: Option<String>,
        now: Ts<Replay>,
    ) -> Self {
        Self {
            directory,
            credentials,
            sink,
            now,
        }
    }

    pub async fn dry_run(&self, command: &RunCommand, into: &Path) -> Result<Journal, Unrunnable> {
        self.each_sink(async |sink, source, listing, journal| {
            let build = DeclaredSnapshot::of(sink)?.build(source, self.now)?;

            self.settle(
                command,
                sink,
                source,
                listing,
                &DryRunBuildLogWrite::new(build, into, sink.name()),
                journal,
            )
            .await
        })
        .await
    }

    pub async fn build(&self, command: &RunCommand) -> Result<Journal, Unrunnable> {
        self.each_sink(async |sink, source, listing, journal| {
            self.settle(
                command,
                sink,
                source,
                listing,
                &DeclaredSnapshot::of(sink)?.build(source, self.now)?,
                journal,
            )
            .await
        })
        .await
    }

    pub async fn check(&self) -> Result<Journal, Unrunnable> {
        self.each_sink(async |sink, source, _, journal| {
            for discrepancy in self.inspection(sink, source).await?.discrepancies() {
                journal.discrepancy(discrepancy);
            }

            Ok(())
        })
        .await
    }

    async fn inspection(
        &self,
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        source: &Source<CaptureConfig>,
    ) -> Result<Inspection, Unrunnable> {
        let snapshot = DeclaredSnapshot::of(sink)?;
        let expectation = Expectation::new(
            snapshot.schedule(),
            source.since(),
            &source.capture().to_json()?,
            self.now,
        );
        let (namespace, series) = (snapshot.namespace(), snapshot.series());

        match Self::reach(sink)? {
            Reached::DuckDb(config, change_log) => {
                expectation
                    .read::<_, Unrunnable>(
                        &self.duckdb(config, change_log).await?,
                        namespace,
                        &series,
                    )
                    .await
            }
            Reached::BigQuery(config, change_log) => {
                expectation
                    .read::<_, Unrunnable>(
                        &self.bigquery(config, change_log).await?,
                        namespace,
                        &series,
                    )
                    .await
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

    async fn each_sink(
        &self,
        mut act: impl AsyncFnMut(
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

            if let Err(unrunnable) = act(sink, source, pipeline.tables(), &mut journal).await {
                journal.failed(sink.name(), &unrunnable);
            }
        }

        Ok(journal)
    }

    async fn settle<B: Report>(
        &self,
        command: &RunCommand,
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        source: &Source<CaptureConfig>,
        listing: &table::Tables,
        writer: &B,
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
                let client = self
                    .duckdb(config, change_log)
                    .await?
                    .executing(writer.execution());

                command
                    .settle(
                        sink,
                        source,
                        self.now,
                        &Taking::new(&client, source.capture(), topic_prefix, listing),
                        writer,
                        journal,
                    )
                    .await
            }
            Reached::BigQuery(config, change_log) => {
                let client = self
                    .bigquery(config, change_log)
                    .await?
                    .executing(writer.execution());

                command
                    .settle(
                        sink,
                        source,
                        self.now,
                        &Taking::new(&client, source.capture(), topic_prefix, listing),
                        writer,
                        journal,
                    )
                    .await
            }
        }
    }

    fn reach(sink: &Sink<WarehouseConfig, ArtifactConfig>) -> Result<Reached<'_>, Unrunnable> {
        sink.warehouse()
            .reach(sink.change_log())
            .ok_or(Unrunnable::Unreached)
    }

    async fn duckdb(
        &self,
        config: &Config,
        change_log: &duckdb::config::Location,
    ) -> Result<DuckDbClient, Unrunnable> {
        Ok(DuckDbWarehouse::open(config, self.credentials.duckdb()?, change_log).await?)
    }

    async fn bigquery(
        &self,
        config: &bigquery::config::Config,
        change_log: &bigquery::config::Location,
    ) -> Result<BigQueryClient, Unrunnable> {
        Ok(BigQueryWarehouse::open(config, &(), change_log).await?)
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
