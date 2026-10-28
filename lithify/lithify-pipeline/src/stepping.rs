use chrono::NaiveDate;

use crate::artifact::ArtifactControl;
use crate::build_log::{BuildLog, Latest, Series};
use crate::progress::{Progress, Step};
use crate::runner::{BuildLogWriter, StepTaker, Tables, Unsteppable};
use crate::schedule::Timeline;
use lithify_core::sink::Namespace;

use crate::warehouse::WarehouseControl;
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesLog {
    namespace: Namespace,
    series: Series,
}

impl SeriesLog {
    pub fn new(namespace: Namespace, series: Series) -> Self {
        Self { namespace, series }
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub fn series(&self) -> &Series {
        &self.series
    }
}

pub struct Stepping<'a, P> {
    taker: &'a P,
    timeline: Timeline,
    capture: String,
}

impl<'a, P: StepTaker> Stepping<'a, P> {
    pub fn new(taker: &'a P, timeline: Timeline, capture: String) -> Self {
        Self {
            taker,
            timeline,
            capture,
        }
    }

    pub async fn up_to(self, date: NaiveDate, snapshot: &SeriesLog) -> Result<Self, P::Error> {
        let until = self
            .timeline
            .through(date)
            .ok_or(Unsteppable::Unopened(date))?;
        if until > self.timeline.range().end {
            return Err(Unsteppable::Unclosed(date).into());
        }
        let standing = self.progress(snapshot).await?.standing();

        match standing.filter(|standing| *standing > until) {
            Some(built_until) => Err(Unsteppable::Overtaken { date, built_until }.into()),
            None => Ok(Self {
                timeline: self.timeline.up_to(until),
                ..self
            }),
        }
    }

    pub async fn next<A: ArtifactControl>(
        &self,
        artifact: &A,
        log: &SeriesLog,
        base: Option<&SeriesLog>,
        taken: &[Window],
    ) -> Result<Option<Step>, P::Error> {
        let progress = self.progress(log).await?;
        let derived = base.is_some();
        let base = match base {
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
        base: Option<Ts<Event>>,
        derived: bool,
    ) -> Option<Window> {
        let from = match (built, derived) {
            (Some(built), _) => *built.to(),
            (None, false) => self.timeline.range().start,
            (None, true) => base?,
        };
        let window = self
            .timeline
            .days(from)
            .take_while(|day| !derived || base.is_some_and(|base| *day.from() <= base))
            .find_map(|day| artifact.window(&day, &self.timeline))?;

        self.timeline.reached(&window).then_some(window)
    }

    pub async fn take<A: ArtifactControl, B: BuildLogWriter>(
        &self,
        artifact: &A,
        log: &SeriesLog,
        base: Option<&SeriesLog>,
        window: &Window,
        tables: &Tables,
        writer: &B,
    ) -> Result<B::Output, P::Error> {
        let previous = self.built(base.unwrap_or(log), *window.from()).await?;

        self.taker
            .take(
                artifact,
                window,
                &previous,
                tables,
                &writer.advancing(log.namespace(), log.series()),
            )
            .await
    }

    async fn built(&self, log: &SeriesLog, closing: Ts<Event>) -> Result<Vec<BuildLog>, P::Error> {
        self.latest(log, Latest::SucceededUntil(closing)).await
    }

    async fn latest(&self, log: &SeriesLog, latest: Latest) -> Result<Vec<BuildLog>, P::Error> {
        self.taker
            .client()
            .build_logs::<P::Error>(log.namespace(), log.series(), &latest.into())
            .await
    }

    async fn progress(&self, log: &SeriesLog) -> Result<Progress, P::Error> {
        let progress = Progress::read(
            &self.latest(log, Latest::Attempted).await?,
            &self.latest(log, Latest::Succeeded).await?,
        );
        let changed = progress
            .captures()
            .find(|logged| *logged != self.capture)
            .map(str::to_owned);

        match changed {
            Some(logged) => Err(Unsteppable::CaptureChanged {
                logged,
                configured: self.capture.clone(),
            }
            .into()),
            None => Ok(progress),
        }
    }
}
