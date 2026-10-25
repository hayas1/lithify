use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, NaiveDate, Utc};

use crate::artifact::{Artifact, UnbuildableArtifact};
use crate::build_log::{BuildLog, Run, Series, TableBuilt, UnreadableBuildLog};
use crate::listener::Finish;
use crate::listener::OutOfOrder;
use crate::model::TableId;
use crate::sink::{Namespace, WarehouseClient};
use crate::source::{ChangeLogs, SchemaChangeStream};
use crate::table_listeners::TableListeners;
use crate::ts::{Event, Ts};
use crate::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unsteppable {
    CaptureChanged {
        logged: String,
        configured: String,
    },
    Unopened(NaiveDate),
    Unclosed(NaiveDate),
    Overtaken {
        date: NaiveDate,
        built_until: Ts<Event>,
    },
}

impl Display for Unsteppable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::CaptureChanged { logged, configured } => write!(
                formatter,
                "the build log was captured as {logged} but the pipeline is captured as {configured} now"
            ),
            Self::Unopened(date) => write!(
                formatter,
                "no window closes by the end of {date} since the change log opened"
            ),
            Self::Unclosed(date) => write!(formatter, "{date} has not closed yet"),
            Self::Overtaken { date, built_until } => write!(
                formatter,
                "the build log has built past {date}, up to {built_until}"
            ),
        }
    }
}

impl std::error::Error for Unsteppable {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Tables {
    #[default]
    Every,
    Except(BTreeSet<TableId>),
}

impl Tables {
    pub fn includes(&self, table: &TableId) -> bool {
        match self {
            Self::Every => true,
            Self::Except(tables) => !tables.contains(table),
        }
    }
}

pub trait BuildLogWriter {
    type Output;

    fn advancing(&self, namespace: &Namespace, series: &Series) -> Self;

    fn write<K, E>(&self, client: &K, built: Vec<TableBuilt>) -> Result<Self::Output, E>
    where
        K: WarehouseClient,
        E: From<K::Error> + From<UnbuildableArtifact>;
}

pub struct BuildLogWrite {
    namespace: Namespace,
    run: Run,
    clock: fn() -> DateTime<Utc>,
}

impl BuildLogWrite {
    pub fn new(namespace: Namespace, run: Run, clock: fn() -> DateTime<Utc>) -> Self {
        Self {
            namespace,
            run,
            clock,
        }
    }
}

impl BuildLogWriter for BuildLogWrite {
    type Output = Vec<BuildLog>;

    fn advancing(&self, namespace: &Namespace, series: &Series) -> Self {
        Self {
            namespace: namespace.clone(),
            run: self.run.advancing(series),
            clock: self.clock,
        }
    }

    fn write<K, E>(&self, client: &K, built: Vec<TableBuilt>) -> Result<Self::Output, E>
    where
        K: WarehouseClient,
        E: From<K::Error> + From<UnbuildableArtifact>,
    {
        let finished_at = (self.clock)();
        let logs: Vec<_> = built
            .into_iter()
            .map(|built| built.logged(&self.run, finished_at))
            .collect();
        client.create_build_log::<E>(&self.namespace)?;
        client.append_build_logs(&self.namespace, &logs)?;

        Ok(logs)
    }
}

pub struct Replayer<'a, K, C> {
    client: &'a K,
    logs: &'a C,
}

impl<'a, K, C> Replayer<'a, K, C>
where
    C: ChangeLogs,
    K: SchemaChangeStream<C::Schema> + WarehouseClient,
{
    pub fn new(client: &'a K, logs: &'a C) -> Self {
        Self { client, logs }
    }

    pub fn built<A: Artifact, E>(
        &self,
        artifact: &'a A,
        window: &'a Window,
        previous: &[BuildLog],
        finishes: impl Fn(&TableId) -> bool,
    ) -> Result<Vec<TableBuilt>, E>
    where
        E: From<OutOfOrder> + From<Infallible>,
        <K as SchemaChangeStream<C::Schema>>::Error: Into<E>,
    {
        let mut listeners = TableListeners::new(
            |table: &TableId| {
                artifact.builder::<_, K::Warehouse>(
                    self.logs.row_changes(),
                    table.clone(),
                    window,
                    artifact.base(previous, table, window),
                )
            },
            finishes,
        );
        let catalog = self.client.replay::<E, _>(
            self.client.warehouse(),
            self.logs.schema_changes(),
            window,
            &mut listeners,
        )?;

        Ok(listeners
            .finish(self.client.warehouse(), catalog)?
            .into_iter()
            .map(|build| self.client.build(build))
            .collect())
    }

    pub fn run<A: Artifact, B: BuildLogWriter, E>(
        &self,
        artifact: &'a A,
        window: &'a Window,
        previous: &[BuildLog],
        tables: &Tables,
        builds: impl Fn(&TableId) -> bool,
        writer: &B,
    ) -> Result<B::Output, E>
    where
        E: From<OutOfOrder>
            + From<Infallible>
            + From<UnbuildableArtifact>
            + From<<K as WarehouseClient>::Error>,
        <K as SchemaChangeStream<C::Schema>>::Error: Into<E>,
    {
        let built = self.built::<A, E>(artifact, window, previous, |table| {
            tables.includes(table) && builds(table)
        })?;

        writer.write(self.client, built)
    }
}

pub trait StepTaker {
    type Client: WarehouseClient;
    type Error: From<<Self::Client as WarehouseClient>::Error>
        + From<UnbuildableArtifact>
        + From<UnreadableBuildLog>
        + From<Unsteppable>;

    fn client(&self) -> &Self::Client;

    fn take<A: Artifact, B: BuildLogWriter>(
        &self,
        artifact: &A,
        window: &Window,
        previous: &[BuildLog],
        tables: &Tables,
        writer: &B,
    ) -> Result<B::Output, Self::Error>;
}
