use std::time::{Duration, Instant};

use chrono::Utc;

use event_replay_cli::pipeline::{Capture, Warehouse};
use event_replay_config::{Artifact, Pipeline, infra};
use event_replay_core::model::{TableId, TableModel};
use event_replay_core::position::RecordedAt;
use event_replay_core::sink::TablePath;
use event_replay_core::window::Window;

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
        pipeline: &'a Pipeline<Capture, Warehouse>,
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
    started: RecordedAt,
    patience: Duration,
    settling: Duration,
}

impl Watch {
    pub fn new(started: RecordedAt, patience: Duration) -> Self {
        Self {
            started,
            patience,
            settling: Duration::from_secs(10),
        }
    }

    fn wait<T>(
        &self,
        changes: &TablePath,
        mut ready: impl FnMut() -> Result<Option<T>, Unverifiable>,
    ) -> Result<T, Unverifiable> {
        let since = Instant::now();

        loop {
            if let Some(read) = ready()? {
                return Ok(read);
            }

            if since.elapsed() >= self.patience {
                return Err(Unverifiable::Unlanded(changes.to_string()));
            }

            std::thread::sleep(Duration::from_secs(2));
        }
    }

    fn changes(&self) -> Window {
        Window::new(self.started, Self::end())
    }

    fn end() -> RecordedAt {
        RecordedAt::from(4102444800000)
    }
}

pub struct SourceEnd<'a> {
    config: &'a infra::Source<Capture>,
    connection: &'a SourceConnection,
    client: Box<dyn Source>,
    applied: Option<RecordedAt>,
}

impl<'a> SourceEnd<'a> {
    fn find<'s>(ends: &'s mut [Self], name: &str) -> Result<&'s mut Self, Unverifiable> {
        ends.iter_mut()
            .find(|end| end.config.name() == name)
            .ok_or_else(|| Unverifiable::UnknownSource(name.to_owned()))
    }

    fn connect(
        pipeline: &'a Pipeline<Capture, Warehouse>,
        name: &str,
        connection: &'a SourceConnection,
    ) -> Result<Self, Unverifiable> {
        Ok(Self {
            config: pipeline
                .sources()
                .find(|source| source.name() == name)
                .ok_or_else(|| Unverifiable::UnknownSource(name.to_owned()))?,
            connection,
            client: <dyn Source>::connect(connection.url())?,
            applied: None,
        })
    }

    pub fn query(&mut self, action: &Action) -> Result<(), Unverifiable> {
        let applied = RecordedAt::from(Utc::now());

        for query in action.queries() {
            self.client.execute(query)?;
            self.applied = Some(applied);
        }

        Ok(())
    }

    pub fn rows(&mut self, model: &TableModel) -> Result<Rows, Unverifiable> {
        Ok(self.client.rows(model)?)
    }

    fn capture(&self) -> &Capture {
        self.config.capture()
    }
}

pub struct SinkEnd<'a> {
    config: &'a infra::Sink<Warehouse>,
    client: Box<dyn Sink>,
}

impl<'a> SinkEnd<'a> {
    fn find<'s>(ends: &'s mut [Self], name: &str) -> Result<&'s mut Self, Unverifiable> {
        ends.iter_mut()
            .find(|end| end.config.name() == name)
            .ok_or_else(|| Unverifiable::UnknownSink(name.to_owned()))
    }

    fn connect(
        pipeline: &'a Pipeline<Capture, Warehouse>,
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
            client: <dyn Sink>::connect(config.warehouse(), key_id, secret)?,
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
        let changes = &self.changes(source, table);
        let statements = self.schema_changes(source)?;

        self.settle(source, changes, watch)?;

        watch.wait(changes, || {
            match self
                .client
                .model(source.capture(), &statements, table, Watch::end())
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
        changes: &TablePath,
        watch: &Watch,
    ) -> Result<(), Unverifiable> {
        let Some(at) = source.applied else {
            return Ok(());
        };
        let mut settling = (0, Instant::now());

        watch.wait(changes, || {
            let landed = self.client.landed(changes, source.capture(), at)?;
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
            &self.schema_changes(source)?,
            table,
            &self.changes(source, table),
            &self.snapshot(table),
            &watch.changes(),
        )?)
    }

    fn schema_changes(&self, source: &SourceEnd) -> Result<TablePath, Unverifiable> {
        self.config
            .schema_changes(source.config)
            .ok_or(Unverifiable::WithoutSchemaChanges)
    }

    pub fn snapshot_rows(&mut self, model: &TableModel) -> Result<Rows, Unverifiable> {
        Ok(self.client.rows(&self.snapshot(model.id()), model)?)
    }

    fn snapshot(&self, table: &TableId) -> TablePath {
        TablePath::new(
            self.config.replay().namespace(Artifact::Snapshot),
            table.table(),
        )
    }

    fn changes(&self, source: &SourceEnd, table: &TableId) -> TablePath {
        TablePath::new(
            self.config.change_log(),
            &source.connection.change_log(table),
        )
    }
}
