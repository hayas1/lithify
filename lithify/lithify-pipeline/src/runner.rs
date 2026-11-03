use std::convert::Infallible;
use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use ulid::Ulid;

use lithify_core::artifact::UnbuildableArtifact;
use lithify_core::build::BuildContext;
use lithify_core::listener::OutOfOrder;
use lithify_core::model::TableId;
use lithify_core::runner::Replayer;
use lithify_core::sink::{Namespace, WarehouseClient};
use lithify_core::source::{ChangeLogs, SchemaChangeStream};
use lithify_core::ts::{Event, Replay, Ts};
use lithify_core::window::Window;

use crate::artifact::ArtifactControl;
use crate::build_log::{BuildLog, Run, TableBuilt};
use crate::capture::ChangeLogsFn;
use crate::check::CaptureChanged;
use crate::infra::Source;
use crate::materialization::Materialization;
use crate::warehouse::WarehouseControl;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unsteppable {
    CaptureChanged(CaptureChanged),
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
            Self::CaptureChanged(changed) => changed.fmt(formatter),
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

pub trait BuildLogWriter {
    type Output;

    async fn write<K, E>(
        &self,
        client: &K,
        materialization: &Materialization,
        built: Vec<TableBuilt>,
    ) -> Result<Self::Output, E>
    where
        K: WarehouseControl,
        E: From<K::Error> + From<UnbuildableArtifact>;
}

pub struct BuildLogWrite {
    run: Run,
}

impl BuildLogWrite {
    pub fn new(run: Run) -> Self {
        Self { run }
    }

    pub fn of<C: Serialize>(
        source: &Source<C>,
        now: Ts<Replay>,
        version: &str,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self::new(Run::new(
            Ulid::from_datetime(DateTime::<Utc>::from(now).into()),
            now,
            source.name(),
            &source.written_capture()?,
            version,
        )))
    }
}

impl BuildLogWriter for BuildLogWrite {
    type Output = Vec<BuildLog>;

    async fn write<K, E>(
        &self,
        client: &K,
        materialization: &Materialization,
        built: Vec<TableBuilt>,
    ) -> Result<Self::Output, E>
    where
        K: WarehouseControl,
        E: From<K::Error> + From<UnbuildableArtifact>,
    {
        let namespace = materialization.namespace();
        let logs: Vec<_> = built
            .into_iter()
            .map(|built| built.logged(&self.run, materialization.series()))
            .collect();
        client.create_build_log::<E>(namespace).await?;
        client.append_build_logs(namespace, &logs).await?;

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
        namespace: &'a Namespace,
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

        let context = BuildContext::new(self.client.warehouse(), namespace);

        for build in Replayer::new(context, self.logs.row_changes()).planned::<A, E>(
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
        materialization: &'a Materialization,
        window: &'a Window,
        previous: &[BuildLog],
        finishes: impl Fn(&TableId) -> bool,
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
            .built::<A, E>(
                artifact,
                materialization.namespace(),
                window,
                previous,
                finishes,
            )
            .await?;

        writer.write(self.client, materialization, built).await
    }
}

pub struct Replaying<'a, K, A, B, F, E> {
    client: &'a K,
    artifact: &'a A,
    materialization: &'a Materialization,
    window: &'a Window,
    previous: &'a [BuildLog],
    finishes: F,
    writer: &'a B,
    failure: PhantomData<fn() -> E>,
}

impl<'a, K, A, B, F, E> Replaying<'a, K, A, B, F, E> {
    pub fn new(
        client: &'a K,
        artifact: &'a A,
        materialization: &'a Materialization,
        window: &'a Window,
        previous: &'a [BuildLog],
        finishes: F,
        writer: &'a B,
    ) -> Self {
        Self {
            client,
            artifact,
            materialization,
            window,
            previous,
            finishes,
            writer,
            failure: PhantomData,
        }
    }
}

impl<K, A, B, F, E> ChangeLogsFn<K> for Replaying<'_, K, A, B, F, E>
where
    K: WarehouseControl,
    A: ArtifactControl,
    B: BuildLogWriter,
    F: Fn(&TableId) -> bool,
    E: From<OutOfOrder>
        + From<Infallible>
        + From<UnbuildableArtifact>
        + From<<K as WarehouseClient>::Error>,
{
    type Output = Result<B::Output, E>;

    async fn call<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        K: SchemaChangeStream<C::Schema, Error = <K as WarehouseClient>::Error>,
    {
        Runner::new(self.client, &logs)
            .run(
                self.artifact,
                self.materialization,
                self.window,
                self.previous,
                self.finishes,
                self.writer,
            )
            .await
    }
}
