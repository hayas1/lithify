use std::collections::BTreeMap;
use std::convert::Infallible;
use std::marker::PhantomData;

use chrono::NaiveDate;

use crate::artifact::{ArtifactControl, ArtifactFn, Declare};
use crate::build_log::{BuildLog, Latest, Series, UnreadableBuildLog};
use crate::capture::Capture;
use crate::check::Expectation;
use crate::infra::{Sink, Source};
use crate::materialization::Materialization;
use crate::progress::{Progress, Step};
use crate::runner::{BuildLogWriter, Replaying, Unsteppable};
use crate::table;

use crate::warehouse::{SinkWarehouse, WarehouseControl};
use lithify_build::artifact::{ArtifactName, UnbuildableArtifact};
use lithify_build::sink::WarehouseClient;
use lithify_schema::listener::OutOfOrder;
use lithify_schema::model::TableId;
use lithify_schema::ts::{Event, Ts};
use lithify_schema::window::Window;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Target {
    artifact: Option<ArtifactName>,
}

impl Target {
    pub fn new(artifact: Option<ArtifactName>) -> Self {
        Self { artifact }
    }

    fn names(&self, materialization: &Materialization) -> bool {
        match &self.artifact {
            Some(artifact) => materialization.artifact() == artifact,
            None => true,
        }
    }
}

pub struct Stepping<'a, K, C, E> {
    client: &'a K,
    source: &'a Source<C>,
    expectation: Expectation,
    listing: &'a table::Tables,
    failure: PhantomData<fn() -> E>,
}

impl<'a, K, C, E> Stepping<'a, K, C, E>
where
    K: WarehouseControl,
    C: Capture<K>,
    E: From<<K as WarehouseClient>::Error>
        + From<UnbuildableArtifact>
        + From<UnreadableBuildLog>
        + From<Unsteppable>
        + From<OutOfOrder>
        + From<Infallible>,
{
    pub fn new(
        client: &'a K,
        source: &'a Source<C>,
        expectation: Expectation,
        listing: &'a table::Tables,
    ) -> Self {
        Self {
            client,
            source,
            expectation,
            listing,
            failure: PhantomData,
        }
    }

    pub async fn up_to(self, date: NaiveDate, scheduled: &Materialization) -> Result<Self, E> {
        let timeline = self.expectation.timeline();
        let until = timeline.through(date).ok_or(Unsteppable::Unopened(date))?;
        if until > timeline.range().end {
            return Err(Unsteppable::Unclosed(date).into());
        }
        let standing = self.progress(scheduled).await?.standing();

        match standing.filter(|standing| *standing > until) {
            Some(built_until) => Err(Unsteppable::Overtaken { date, built_until }.into()),
            None => Ok(Self {
                expectation: self.expectation.up_to(until),
                ..self
            }),
        }
    }

    pub async fn step<W, A, B>(
        &self,
        sink: &Sink<W, A>,
        target: &Target,
        writer: &B,
        mut report: impl FnMut(&Series, &Window, Result<B::Output, E>) -> Result<(), E>,
    ) -> Result<Option<(Series, Window)>, E>
    where
        W: SinkWarehouse,
        A: Declare,
        B: BuildLogWriter,
    {
        self.advance(sink, target, &BTreeMap::new(), writer, &mut report)
            .await
    }

    pub async fn catch_up<W, A, B>(
        &self,
        sink: &Sink<W, A>,
        target: &Target,
        writer: &B,
        mut report: impl FnMut(&Series, &Window, Result<B::Output, E>) -> Result<(), E>,
    ) -> Result<(), E>
    where
        W: SinkWarehouse,
        A: Declare,
        B: BuildLogWriter,
    {
        let mut taken: BTreeMap<Series, Vec<Window>> = BTreeMap::new();

        while let Some((series, window)) = self
            .advance(sink, target, &taken, writer, &mut report)
            .await?
        {
            taken.entry(series).or_default().push(window);
        }

        Ok(())
    }

    async fn advance<W, A, B>(
        &self,
        sink: &Sink<W, A>,
        target: &Target,
        taken: &BTreeMap<Series, Vec<Window>>,
        writer: &B,
        report: &mut impl FnMut(&Series, &Window, Result<B::Output, E>) -> Result<(), E>,
    ) -> Result<Option<(Series, Window)>, E>
    where
        W: SinkWarehouse,
        A: Declare,
        B: BuildLogWriter,
    {
        for (declared, materialization) in sink
            .materializations()
            .filter(|(_, materialization)| target.names(materialization))
        {
            let series = materialization.series();
            let advance = Advance {
                stepping: self,
                materialization,
                taken: taken.get(series).map(Vec::as_slice).unwrap_or_default(),
                writer,
            };

            if let Some((window, output)) = declared
                .declare(materialization.artifact(), sink.time_zone(), advance)
                .await?
            {
                report(series, &window, output)?;

                return Ok(Some((series.clone(), window)));
            }
        }

        Ok(None)
    }

    pub async fn next<A: ArtifactControl>(
        &self,
        artifact: &A,
        materialization: &Materialization,
        taken: &[Window],
    ) -> Result<Option<Step>, E> {
        let progress = self.progress(materialization).await?;
        let derived = materialization.base().is_some();
        let base = match materialization.base() {
            Some(base) => Some(self.progress(base).await?),
            None => None,
        };

        Ok(progress.step(base.as_ref(), taken, |built, base| {
            self.window(artifact, built, base, derived)
        }))
    }

    fn window<A: ArtifactControl>(
        &self,
        artifact: &A,
        built: Option<&Window>,
        base: Option<&Window>,
        derived: bool,
    ) -> Option<Window> {
        let timeline = self.expectation.timeline();
        let from = match (built, derived) {
            (Some(built), _) => *built.to(),
            (None, false) => timeline.range().start,
            (None, true) => *base?.from(),
        };
        let shard = base.map(|base| *base.to());
        // An artifact built from another and never built yet starts from the last shard its base
        // built, whichever side of the day its window opens on.
        let fresh = derived && built.is_none();
        let window = timeline
            .days(from)
            .filter_map(|day| artifact.window(&day, timeline))
            .skip_while(|window| fresh && shard.is_some_and(|shard| *window.from() < shard))
            .take_while(|window| !derived || shard.is_some_and(|shard| *window.from() <= shard))
            .next()?;

        timeline.reached(&window).then_some(window)
    }

    pub async fn take<A: ArtifactControl, B: BuildLogWriter>(
        &self,
        artifact: &A,
        materialization: &Materialization,
        step: &Step,
        writer: &B,
    ) -> Result<B::Output, E> {
        let window = step.window();
        let previous = self
            .built(
                materialization.base().unwrap_or(materialization),
                *window.from(),
            )
            .await?;

        self.source
            .capture()
            .with_change_logs(
                self.client.warehouse(),
                Replaying::<_, _, _, _, E>::new(
                    self.client,
                    artifact,
                    materialization,
                    window,
                    &previous,
                    self.finishes(materialization, step),
                    writer,
                ),
            )
            .await
    }

    fn finishes<'f>(
        &'f self,
        materialization: &'f Materialization,
        step: &'f Step,
    ) -> impl Fn(&TableId) -> bool + 'f {
        move |table| {
            step.includes(table) && self.listing.table(table).builds(materialization.artifact())
        }
    }

    async fn built(
        &self,
        materialization: &Materialization,
        closing: Ts<Event>,
    ) -> Result<Vec<BuildLog>, E> {
        self.latest(materialization, Latest::SucceededUntil(closing))
            .await
    }

    async fn latest(
        &self,
        materialization: &Materialization,
        latest: Latest,
    ) -> Result<Vec<BuildLog>, E> {
        self.client
            .build_logs::<E>(
                materialization.namespace(),
                materialization.series(),
                &latest.into(),
            )
            .await
    }

    async fn progress(&self, materialization: &Materialization) -> Result<Progress, E> {
        let latest = self.latest(materialization, Latest::Attempted).await?;
        if let Some(changed) = self.expectation.capture_changed(&latest) {
            return Err(Unsteppable::CaptureChanged(changed).into());
        }

        Ok(Progress::read(
            &latest,
            &self.latest(materialization, Latest::Succeeded).await?,
        ))
    }
}

struct Advance<'s, 'a, K, C, E, B> {
    stepping: &'s Stepping<'a, K, C, E>,
    materialization: &'s Materialization,
    taken: &'s [Window],
    writer: &'s B,
}

impl<K, C, E, B> ArtifactFn for Advance<'_, '_, K, C, E, B>
where
    K: WarehouseControl,
    C: Capture<K>,
    E: From<<K as WarehouseClient>::Error>
        + From<UnbuildableArtifact>
        + From<UnreadableBuildLog>
        + From<Unsteppable>
        + From<OutOfOrder>
        + From<Infallible>,
    B: BuildLogWriter,
{
    type Output = Result<Option<(Window, Result<B::Output, E>)>, E>;

    async fn call<A: ArtifactControl>(self, artifact: &A) -> Self::Output {
        let Some(step) = self
            .stepping
            .next(artifact, self.materialization, self.taken)
            .await?
        else {
            return Ok(None);
        };
        let output = self
            .stepping
            .take(artifact, self.materialization, &step, self.writer)
            .await;

        Ok(Some((step.window().clone(), output)))
    }
}
