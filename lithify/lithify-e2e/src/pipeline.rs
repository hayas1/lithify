use std::time::{Duration, Instant};

use chrono::Utc;

use lithify_cli::artifacts::Artifacts;
use lithify_cli::pipeline::{CaptureConfig, WarehouseConfig};
use lithify_config::{Pipeline, infra};
use lithify_core::artifact::Artifact;
use lithify_core::model::{TableId, TableModel};
use lithify_core::ts::{Event, Processing, Ts};
use lithify_core::window::Window;

use crate::row::Rows;
use crate::run::Unverifiable;
use crate::scenario::{Action, Scenario, SinkConnection, SourceConnection};
use crate::sink::{Sink, Unbuildable};
use crate::source::Source;

pub struct ConnectedPipeline<'a> {
    sources: Vec<SourceEnd<'a>>,
    sinks: Vec<SinkEnd<'a>>,
    watch: Watch,
}

impl<'a> ConnectedPipeline<'a> {
    pub fn connect(
        scenario: &'a Scenario,
        pipeline: &'a Pipeline<CaptureConfig, WarehouseConfig>,
        watch: Watch,
        key_id: &str,
        secret: &str,
    ) -> Result<Self, Unverifiable> {
        Ok(Self {
            sources: scenario
                .sources()
                .map(|(name, connection)| SourceEnd::connect(pipeline, name, connection))
                .collect::<Result<_, _>>()?,
            sinks: scenario
                .sinks()
                .map(|(name, connection)| {
                    SinkEnd::connect(pipeline, name, connection, key_id, secret)
                })
                .collect::<Result<_, _>>()?,
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

    fn wait<T>(
        &self,
        table: &TableId,
        mut ready: impl FnMut() -> Result<Option<T>, Unverifiable>,
    ) -> Result<T, Unverifiable> {
        let since = Instant::now();

        loop {
            if let Some(read) = ready()? {
                return Ok(read);
            }

            if since.elapsed() >= self.patience {
                return Err(Unverifiable::Unlanded(table.to_string()));
            }

            std::thread::sleep(Duration::from_secs(2));
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
    client: Box<dyn Source>,
    applied: Option<Ts<Processing>>,
}

impl<'a> SourceEnd<'a> {
    fn find<'s>(ends: &'s mut [Self], name: &str) -> Result<&'s mut Self, Unverifiable> {
        ends.iter_mut()
            .find(|end| end.config.name() == name)
            .ok_or_else(|| Unverifiable::UnknownSource(name.to_owned()))
    }

    fn connect(
        pipeline: &'a Pipeline<CaptureConfig, WarehouseConfig>,
        name: &str,
        connection: &'a SourceConnection,
    ) -> Result<Self, Unverifiable> {
        Ok(Self {
            config: pipeline
                .sources()
                .find(|source| source.name() == name)
                .ok_or_else(|| Unverifiable::UnknownSource(name.to_owned()))?,
            client: <dyn Source>::connect(connection.url())?,
            applied: None,
        })
    }

    pub fn query(&mut self, action: &Action) -> Result<(), Unverifiable> {
        let applied = Ts::from(Utc::now());

        for query in action.queries() {
            self.client.execute(query)?;
            self.applied = Some(applied);
        }

        Ok(())
    }

    pub fn rows(&mut self, model: &TableModel) -> Result<Rows, Unverifiable> {
        Ok(self.client.rows(model)?)
    }

    fn capture(&self) -> &CaptureConfig {
        self.config.capture()
    }
}

pub struct SinkEnd<'a> {
    config: &'a infra::Sink<WarehouseConfig>,
    client: Box<dyn Sink>,
}

impl<'a> SinkEnd<'a> {
    fn find<'s>(ends: &'s mut [Self], name: &str) -> Result<&'s mut Self, Unverifiable> {
        ends.iter_mut()
            .find(|end| end.config.name() == name)
            .ok_or_else(|| Unverifiable::UnknownSink(name.to_owned()))
    }

    fn connect(
        pipeline: &'a Pipeline<CaptureConfig, WarehouseConfig>,
        name: &str,
        _: &'a SinkConnection,
        key_id: &str,
        secret: &str,
    ) -> Result<Self, Unverifiable> {
        let config = pipeline
            .sinks()
            .find(|sink| sink.name() == name)
            .ok_or_else(|| Unverifiable::UnknownSink(name.to_owned()))?;

        Ok(Self {
            config,
            client: <dyn Sink>::connect(config.warehouse(), config.change_log(), key_id, secret)?,
        })
    }

    pub fn query(&mut self, action: &Action) -> Result<(), Unverifiable> {
        for query in action.queries() {
            self.client.execute(query)?;
        }

        Ok(())
    }

    pub fn table_model(
        &mut self,
        source: &SourceEnd,
        table: &TableId,
        watch: &Watch,
    ) -> Result<TableModel, Unverifiable> {
        let topic_prefix = Self::topic_prefix(source)?;

        self.settle(source, topic_prefix, table, watch)?;

        watch.wait(table, || {
            match self
                .client
                .model(source.capture(), topic_prefix, table, Watch::end())
            {
                Ok(model) => Ok(Some(model)),
                Err(Unbuildable::UnknownTable(_)) => Ok(None),
                Err(unbuildable) => Err(unbuildable.into()),
            }
        })
    }

    fn settle(
        &mut self,
        source: &SourceEnd,
        topic_prefix: &str,
        table: &TableId,
        watch: &Watch,
    ) -> Result<(), Unverifiable> {
        let Some(at) = source.applied else {
            return Ok(());
        };
        let mut settling = (0, Instant::now());

        watch.wait(table, || {
            let landed = self
                .client
                .landed(source.capture(), topic_prefix, table, at)?;
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
    }

    pub fn build_snapshot(
        &mut self,
        source: &SourceEnd,
        model: &TableModel,
        watch: &Watch,
    ) -> Result<(), Unverifiable> {
        let table = model.id();

        Ok(self.client.snapshot(
            source.capture(),
            Self::topic_prefix(source)?,
            table,
            &self.config.snapshot(),
            &watch.changes(),
        )?)
    }

    fn topic_prefix<'s>(source: &'s SourceEnd) -> Result<&'s str, Unverifiable> {
        source
            .config
            .topic_prefix()
            .ok_or(Unverifiable::WithoutTopicPrefix)
    }

    pub fn snapshot_rows(
        &mut self,
        model: &TableModel,
        watch: &Watch,
    ) -> Result<Rows, Unverifiable> {
        let target = self
            .config
            .snapshot()
            .target(model.id(), &watch.changes())
            .map_err(Unbuildable::from)?;

        Ok(self.client.rows(&target, model)?)
    }
}
