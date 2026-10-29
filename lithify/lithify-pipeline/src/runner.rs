use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt::{self, Display, Formatter};

use chrono::NaiveDate;

use lithify_core::artifact::UnbuildableArtifact;
use lithify_core::listener::OutOfOrder;
use lithify_core::model::TableId;
use lithify_core::runner::Replayer;
use lithify_core::sink::{Namespace, WarehouseClient};
use lithify_core::source::{ChangeLogs, SchemaChangeStream};
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

use crate::artifact::ArtifactControl;
use crate::build_log::{BuildLog, Run, Series, TableBuilt, UnreadableBuildLog};
use crate::warehouse::WarehouseControl;

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

    async fn write<K, E>(&self, client: &K, built: Vec<TableBuilt>) -> Result<Self::Output, E>
    where
        K: WarehouseControl,
        E: From<K::Error> + From<UnbuildableArtifact>;
}

pub struct BuildLogWrite {
    namespace: Namespace,
    run: Run,
}

impl BuildLogWrite {
    pub fn new(namespace: Namespace, run: Run) -> Self {
        Self { namespace, run }
    }
}

impl BuildLogWriter for BuildLogWrite {
    type Output = Vec<BuildLog>;

    fn advancing(&self, namespace: &Namespace, series: &Series) -> Self {
        Self {
            namespace: namespace.clone(),
            run: self.run.advancing(series),
        }
    }

    async fn write<K, E>(&self, client: &K, built: Vec<TableBuilt>) -> Result<Self::Output, E>
    where
        K: WarehouseControl,
        E: From<K::Error> + From<UnbuildableArtifact>,
    {
        let logs: Vec<_> = built
            .into_iter()
            .map(|built| built.logged(&self.run))
            .collect();
        client.create_build_log::<E>(&self.namespace).await?;
        client.append_build_logs(&self.namespace, &logs).await?;

        Ok(logs)
    }
}

pub struct Runner<'a, K, C> {
    client: &'a K,
    logs: &'a C,
}

impl<'a, K, C> Runner<'a, K, C>
where
    C: ChangeLogs,
    K: SchemaChangeStream<C::Schema> + WarehouseControl,
{
    pub fn new(client: &'a K, logs: &'a C) -> Self {
        Self { client, logs }
    }

    pub async fn built<A: ArtifactControl, E>(
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
        let history = self
            .client
            .history::<E>(self.logs.schema_changes(), window)
            .await?;
        let mut built = Vec::new();

        for build in Replayer::new(self.client.warehouse(), self.logs).planned::<A, E>(
            &history,
            artifact,
            window,
            |table| {
                artifact
                    .base(previous, table, window)
                    .and_then(BuildLog::target)
                    .cloned()
            },
            finishes,
        )? {
            built.push(self.client.build(build).await);
        }

        Ok(built)
    }

    pub async fn run<A: ArtifactControl, B: BuildLogWriter, E>(
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
        let built = self
            .built::<A, E>(artifact, window, previous, |table| {
                tables.includes(table) && builds(table)
            })
            .await?;

        writer.write(self.client, built).await
    }
}

pub trait StepTaker {
    type Client: WarehouseControl;
    type Error: From<<Self::Client as WarehouseClient>::Error>
        + From<UnbuildableArtifact>
        + From<UnreadableBuildLog>
        + From<Unsteppable>;

    fn client(&self) -> &Self::Client;

    async fn take<A: ArtifactControl, B: BuildLogWriter>(
        &self,
        artifact: &A,
        window: &Window,
        previous: &[BuildLog],
        tables: &Tables,
        writer: &B,
    ) -> Result<B::Output, Self::Error>;
}
