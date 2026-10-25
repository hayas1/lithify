use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, NaiveDate, Utc};

use crate::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use crate::build_log::{BuildLog, Outcome, Run, Series, Status, UnreadableBuildLog};
use crate::listener::OutOfOrder;
use crate::model::TableId;
use crate::sink::{Namespace, WarehouseClient};
use crate::source::{ChangeLogs, SchemaChangeStream};
use crate::table_listeners::{TableListeners, Track};
use crate::ts::{Event, Ts};
use crate::window::Window;

type BuiltOf<'b, A, C, K> =
    Built<<A as Artifact>::Builder<'b, <C as ChangeLogs>::Row, <K as WarehouseClient>::Warehouse>>;

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
    pub fn narrow<B: ArtifactBuilder>(&self, mut builders: Vec<Built<B>>) -> Vec<Built<B>> {
        match self {
            Self::Every => builders,
            Self::Except(tables) => {
                builders.retain(|built| !tables.contains(built.listener().table()));

                builders
            }
        }
    }
}

pub type Built<B> = Track<B, UnbuildableArtifact>;

pub trait Runner {
    type Output;

    fn advancing(&self, namespace: &Namespace, series: &Series) -> Self;

    fn run<K, B, E>(
        &self,
        client: &K,
        window: &Window,
        builders: &[Built<B>],
    ) -> Result<Self::Output, E>
    where
        K: WarehouseClient,
        B: ArtifactBuilder,
        E: From<K::Error> + From<UnbuildableArtifact> + From<Unsteppable>;
}

pub struct Build {
    namespace: Namespace,
    run: Run,
    clock: fn() -> DateTime<Utc>,
}

impl Build {
    pub fn new(namespace: Namespace, run: Run, clock: fn() -> DateTime<Utc>) -> Self {
        Self {
            namespace,
            run,
            clock,
        }
    }

    fn outcome<K: WarehouseClient, B: ArtifactBuilder>(
        &self,
        client: &K,
        built: &Built<B>,
    ) -> Outcome {
        let builder = built.listener();
        let built = match (built.failure(), builder.missing_base()) {
            (Some(failure), _) => Err(Unbuilt::from(failure.clone())),
            (None, Some(table)) => Err(Unbuilt::from(UnbuildableArtifact::WithoutBase(
                table.clone(),
            ))),
            (None, None) => client.build::<Unbuilt>(builder),
        };
        let status = match built {
            Ok(()) => Status::Succeeded,
            Err(Unbuilt(error)) => Status::Failed(error),
        };

        Outcome::new(status, (self.clock)())
    }
}

impl Runner for Build {
    type Output = Vec<BuildLog>;

    fn advancing(&self, namespace: &Namespace, series: &Series) -> Self {
        Self {
            namespace: namespace.clone(),
            run: self.run.advancing(series),
            clock: self.clock,
        }
    }

    fn run<K, B, E>(&self, client: &K, _: &Window, builders: &[Built<B>]) -> Result<Self::Output, E>
    where
        K: WarehouseClient,
        B: ArtifactBuilder,
        E: From<K::Error> + From<UnbuildableArtifact> + From<Unsteppable>,
    {
        client.create_build_log::<E>(&self.namespace)?;

        builders
            .iter()
            .map(|built| {
                let log = built
                    .listener()
                    .build_log(&self.run, self.outcome(client, built));
                client.append_build_log(&self.namespace, &log)?;

                Ok(log)
            })
            .collect()
    }
}

struct Unbuilt(String);

impl<E: std::error::Error> From<E> for Unbuilt {
    fn from(cause: E) -> Self {
        Self(cause.to_string())
    }
}

pub struct Replay<'a, K, C> {
    client: &'a K,
    logs: &'a C,
}

impl<'a, K, C> Replay<'a, K, C>
where
    C: ChangeLogs,
    K: SchemaChangeStream<C::Schema> + WarehouseClient,
{
    pub fn new(client: &'a K, logs: &'a C) -> Self {
        Self { client, logs }
    }

    pub fn builders<A: Artifact, E>(
        &self,
        artifact: &'a A,
        window: &'a Window,
        previous: &[BuildLog],
    ) -> Result<Vec<BuiltOf<'a, A, C, K>>, E>
    where
        E: From<OutOfOrder> + From<Infallible>,
        <K as SchemaChangeStream<C::Schema>>::Error: Into<E>,
    {
        let mut tracker = TableListeners::new(|table: &TableId| {
            artifact.builder::<_, K::Warehouse>(
                self.logs.row_changes(),
                table.clone(),
                window,
                artifact.base(previous, table, window),
            )
        });
        let catalog = self.client.replay::<E, _>(
            self.client.warehouse(),
            self.logs.schema_changes(),
            window,
            &mut tracker,
        )?;

        Ok(tracker.finish(self.client.warehouse(), &catalog))
    }

    pub fn run<A: Artifact, R: Runner, E>(
        &self,
        artifact: &'a A,
        window: &'a Window,
        previous: &[BuildLog],
        tables: &Tables,
        builds: impl Fn(&TableId) -> bool,
        runner: &R,
    ) -> Result<R::Output, E>
    where
        E: From<OutOfOrder>
            + From<Infallible>
            + From<UnbuildableArtifact>
            + From<Unsteppable>
            + From<<K as WarehouseClient>::Error>,
        <K as SchemaChangeStream<C::Schema>>::Error: Into<E>,
    {
        let mut builders = tables.narrow(self.builders::<A, E>(artifact, window, previous)?);
        builders.retain(|built| builds(built.listener().table()));

        runner.run(self.client, window, &builders)
    }
}

pub trait Planner {
    type Client: WarehouseClient;
    type Error: From<<Self::Client as WarehouseClient>::Error>
        + From<UnbuildableArtifact>
        + From<UnreadableBuildLog>
        + From<Unsteppable>;

    fn client(&self) -> &Self::Client;

    fn plan<A: Artifact, R: Runner>(
        &self,
        artifact: &A,
        window: &Window,
        previous: &[BuildLog],
        tables: &Tables,
        runner: &R,
    ) -> Result<R::Output, Self::Error>;
}
