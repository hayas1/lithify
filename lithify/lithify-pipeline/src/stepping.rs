use chrono::NaiveDate;

use crate::artifact::ArtifactControl;
use crate::build_log::{BuildLog, Latest};
use crate::check::Expectation;
use crate::materialization::Materialization;
use crate::progress::{Progress, Step};
use crate::runner::{BuildLogWriter, StepTaker, Unsteppable};
use crate::table;

use crate::warehouse::WarehouseControl;
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

pub struct Stepping<'a, P> {
    taker: &'a P,
    expectation: Expectation,
    listing: &'a table::Tables,
}

impl<'a, P: StepTaker> Stepping<'a, P> {
    pub fn new(taker: &'a P, expectation: Expectation, listing: &'a table::Tables) -> Self {
        Self {
            taker,
            expectation,
            listing,
        }
    }

    pub async fn up_to(
        self,
        date: NaiveDate,
        scheduled: &Materialization,
    ) -> Result<Self, P::Error> {
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

    pub async fn next<A: ArtifactControl>(
        &self,
        artifact: &A,
        materialization: &Materialization,
        taken: &[Window],
    ) -> Result<Option<Step>, P::Error> {
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
    ) -> Result<B::Output, P::Error> {
        let window = step.window();
        let previous = self
            .built(
                materialization.base().unwrap_or(materialization),
                *window.from(),
            )
            .await?;

        self.taker
            .take(
                artifact,
                materialization,
                window,
                &previous,
                |table| {
                    step.includes(table)
                        && self.listing.table(table).builds(materialization.artifact())
                },
                writer,
            )
            .await
    }

    async fn built(
        &self,
        materialization: &Materialization,
        closing: Ts<Event>,
    ) -> Result<Vec<BuildLog>, P::Error> {
        self.latest(materialization, Latest::SucceededUntil(closing))
            .await
    }

    async fn latest(
        &self,
        materialization: &Materialization,
        latest: Latest,
    ) -> Result<Vec<BuildLog>, P::Error> {
        self.taker
            .client()
            .build_logs::<P::Error>(
                materialization.namespace(),
                materialization.series(),
                &latest.into(),
            )
            .await
    }

    async fn progress(&self, materialization: &Materialization) -> Result<Progress, P::Error> {
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
