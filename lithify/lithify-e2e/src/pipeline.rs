use std::time::{Duration, Instant};

use chrono::Utc;

use lithify_artifact::snapshot::Snapshot;
use lithify_build::artifact::Artifact;
use lithify_build::sink::TablePath;
use lithify_cli::artifacts::ArtifactConfig;
use lithify_cli::credential::Credentials;
use lithify_cli::pipeline::{CaptureConfig, WarehouseConfig};
use lithify_cli::unrunnable::Unrunnable;
use lithify_pipeline::{Pipeline, infra};
use lithify_schema::model::{TableId, TableModel};
use lithify_schema::ts::{Event, Processing, Ts};
use lithify_schema::window::Window;

use crate::row::Rows;
use crate::run::Unverifiable;
use crate::scenario::{Action, Scenario, SinkConnection, SourceConnection};
use crate::sink::{SinkClient, Unbuildable};
use crate::source::SourceClient;

pub struct ConnectedPipeline<'a> {
    sources: Vec<SourceEnd<'a>>,
    sinks: Vec<SinkEnd<'a>>,
    watch: Watch,
}

impl<'a> ConnectedPipeline<'a> {
    pub async fn connect(
        scenario: &'a Scenario,
        pipeline: &'a Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
        watch: Watch,
        credentials: &Credentials,
    ) -> Result<Self, Unverifiable> {
        let mut sources = Vec::new();
        for (name, connection) in scenario.sources() {
            sources.push(SourceEnd::connect(pipeline, name, connection).await?);
        }

        let mut sinks = Vec::new();
        for (name, connection) in scenario.sinks() {
            sinks.push(SinkEnd::connect(pipeline, name, connection, credentials).await?);
        }

        Ok(Self {
            sources,
            sinks,
            watch,
        })
    }

    pub fn source(&mut self, name: &str) -> Result<&mut SourceEnd<'a>, Unverifiable> {
        SourceEnd::find(&mut self.sources, name)
    }

    pub fn sink(&mut self, name: &str) -> Result<&mut SinkEnd<'a>, Unverifiable> {
        SinkEnd::find(&mut self.sinks, name)
    }

    pub fn ends(
        &mut self,
        source: &str,
        sink: &str,
    ) -> Result<(&mut SourceEnd<'a>, &mut SinkEnd<'a>, &Watch), Unverifiable> {
        Ok((
            SourceEnd::find(&mut self.sources, source)?,
            SinkEnd::find(&mut self.sinks, sink)?,
            &self.watch,
        ))
    }
}

pub struct Watch {
    started: Ts<Event>,
    patience: Duration,
    settling: Duration,
}

impl Watch {
    pub fn new(started: Ts<Event>, patience: Duration) -> Self {
        Self {
            started,
            patience,
            settling: Duration::from_secs(10),
        }
    }

    async fn wait<T>(
        &self,
        table: &TableId,
        mut ready: impl AsyncFnMut() -> Result<Option<T>, Unverifiable>,
    ) -> Result<T, Unverifiable> {
        let since = Instant::now();

        loop {
            if let Some(read) = ready().await? {
                return Ok(read);
            }

            if since.elapsed() >= self.patience {
                return Err(Unverifiable::Unlanded(table.to_string()));
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    fn changes(&self) -> Window {
        Window::new(self.started, Self::end())
    }

    fn end() -> Ts<Event> {
        Ts::try_from(4102444800000).unwrap()
    }
}

pub struct SourceEnd<'a> {
    config: &'a infra::Source<CaptureConfig>,
    client: SourceClient,
    applied: Option<Ts<Processing>>,
}

impl<'a> SourceEnd<'a> {
    fn find<'s>(ends: &'s mut [Self], name: &str) -> Result<&'s mut Self, Unverifiable> {
        ends.iter_mut()
            .find(|end| end.config.name() == name)
            .ok_or_else(|| Unverifiable::UnknownSource(name.to_owned()))
    }

    async fn connect(
        pipeline: &'a Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
        name: &str,
        connection: &'a SourceConnection,
    ) -> Result<Self, Unverifiable> {
        Ok(Self {
            config: pipeline
                .sources()
                .find(|source| source.name() == name)
                .ok_or_else(|| Unverifiable::UnknownSource(name.to_owned()))?,
            client: SourceClient::connect(connection.url()).await?,
            applied: None,
        })
    }

    pub async fn query(&mut self, action: &Action) -> Result<(), Unverifiable> {
        let applied = Ts::from(Utc::now());

        for query in action.queries() {
            self.client.execute(query).await?;
            self.applied = Some(applied);
        }

        Ok(())
    }

    pub async fn rows(&mut self, model: &TableModel) -> Result<Rows, Unverifiable> {
        Ok(self.client.rows(model).await?)
    }

    fn capture(&self) -> &CaptureConfig {
        self.config.capture()
    }
}

pub struct SinkEnd<'a> {
    config: &'a infra::Sink<WarehouseConfig, ArtifactConfig>,
    client: SinkClient,
}

impl<'a> SinkEnd<'a> {
    fn find<'s>(ends: &'s mut [Self], name: &str) -> Result<&'s mut Self, Unverifiable> {
        ends.iter_mut()
            .find(|end| end.config.name() == name)
            .ok_or_else(|| Unverifiable::UnknownSink(name.to_owned()))
    }

    async fn connect(
        pipeline: &'a Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
        name: &str,
        _: &'a SinkConnection,
        credentials: &Credentials,
    ) -> Result<Self, Unverifiable> {
        let config = pipeline
            .sinks()
            .find(|sink| sink.name() == name)
            .ok_or_else(|| Unverifiable::UnknownSink(name.to_owned()))?;

        Ok(Self {
            config,
            client: SinkClient::connect(config.warehouse(), config.change_log(), credentials)
                .await?,
        })
    }

    pub async fn query(&mut self, action: &Action) -> Result<(), Unverifiable> {
        for query in action.queries() {
            self.client.execute(query).await?;
        }

        Ok(())
    }

    pub async fn table_model(
        &mut self,
        source: &SourceEnd<'_>,
        table: &TableId,
        watch: &Watch,
    ) -> Result<TableModel, Unverifiable> {
        self.settle(source, table, watch).await?;

        watch
            .wait(table, async || {
                match self
                    .client
                    .model(source.capture(), table, Watch::end())
                    .await
                {
                    Ok(model) => Ok(Some(model)),
                    Err(Unbuildable::UnknownTable(_)) => Ok(None),
                    Err(unbuildable) => Err(unbuildable.into()),
                }
            })
            .await
    }

    async fn settle(
        &mut self,
        source: &SourceEnd<'_>,
        table: &TableId,
        watch: &Watch,
    ) -> Result<(), Unverifiable> {
        let Some(at) = source.applied else {
            return Ok(());
        };
        let mut settling = (0, Instant::now());

        watch
            .wait(table, async || {
                let landed = self.client.landed(source.capture(), table, at).await?;
                let (previous, since) = settling;

                match (landed, landed == previous) {
                    (0, _) => Ok(None),
                    (_, true) if since.elapsed() >= watch.settling => Ok(Some(())),
                    (_, true) => Ok(None),
                    (_, false) => {
                        settling = (landed, Instant::now());

                        Ok(None)
                    }
                }
            })
            .await
    }

    pub async fn build_snapshot(
        &mut self,
        source: &SourceEnd<'_>,
        model: &TableModel,
        watch: &Watch,
    ) -> Result<(), Unverifiable> {
        let snapshot = self.snapshot()?;

        Ok(self
            .client
            .snapshot(
                source.capture(),
                model.id(),
                &snapshot,
                self.config.scheduled().namespace(),
                &watch.changes(),
            )
            .await?)
    }

    fn snapshot(&self) -> Result<Snapshot, Unverifiable> {
        let name = self.config.scheduled().series().artifact();

        ArtifactConfig::snapshot(self.config, name).ok_or_else(|| {
            Unverifiable::Unrunnable(Unrunnable::Undeclared {
                sink: self.config.name().to_owned(),
                artifact: name.clone(),
            })
        })
    }

    pub async fn snapshot_rows(
        &mut self,
        model: &TableModel,
        watch: &Watch,
    ) -> Result<Rows, Unverifiable> {
        let target = TablePath::new(
            self.config.scheduled().namespace().clone(),
            &self.snapshot()?.table_name(model.id(), &watch.changes()),
        );

        Ok(self.client.rows(&target, model).await?)
    }
}
