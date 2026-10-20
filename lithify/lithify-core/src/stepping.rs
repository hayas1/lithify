use chrono::NaiveDate;

use crate::artifact::Artifact;
use crate::build_log::{BuildLog, Latest, Series};
use crate::progress::{Progress, Step};
use crate::runner::{Planner, Runner, Tables, Unsteppable};
use crate::schedule::Timeline;
use crate::sink::{Namespace, WarehouseClient};
use crate::ts::{Event, Ts};
use crate::window::Window;

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
    planner: &'a P,
    timeline: Timeline,
    now: Ts<Event>,
    capture: String,
}

impl<'a, P: Planner> Stepping<'a, P> {
    pub fn new(planner: &'a P, timeline: Timeline, capture: String) -> Self {
        Self {
            planner,
            now: timeline.until(),
            timeline,
            capture,
        }
    }

    pub fn up_to(self, date: NaiveDate, snapshot: &SeriesLog) -> Result<Self, P::Error> {
        let until = *self.dated(self.timeline.shard(date), date)?.to();
        let standing = self.progress(snapshot)?.standing();

        match standing.filter(|standing| *standing > until) {
            Some(built_until) => Err(Unsteppable::Overtaken { date, built_until }.into()),
            None => Ok(Self {
                timeline: self.timeline.up_to(until),
                ..self
            }),
        }
    }

    pub fn next<A: Artifact>(
        &self,
        artifact: &A,
        log: &SeriesLog,
        base: Option<&SeriesLog>,
        taken: &[Window],
    ) -> Result<Option<Step>, P::Error> {
        let progress = self.progress(log)?;
        let base = base.map(|base| self.progress(base)).transpose()?;

        Ok(progress.step(base.as_ref(), taken, |built, base| {
            artifact.next(&self.timeline, built, base)
        }))
    }

    pub fn take<A: Artifact, R: Runner>(
        &self,
        artifact: &A,
        log: &SeriesLog,
        base: Option<&SeriesLog>,
        window: &Window,
        tables: &Tables,
        runner: &R,
    ) -> Result<R::Output, P::Error> {
        let previous = self.built(base.unwrap_or(log), *window.from())?;

        self.planner.plan(
            artifact,
            window,
            &previous,
            tables,
            &runner.advancing(log.namespace(), log.series()),
        )
    }

    pub fn on<A: Artifact>(&self, artifact: &A, date: NaiveDate) -> Result<Window, Unsteppable> {
        let interval = self
            .timeline
            .shard(date)
            .ok_or(Unsteppable::Unsharded(date))?;
        let window = artifact
            .window(&interval, self.now)
            .ok_or_else(|| Unsteppable::Unclosed(interval.clone()))?;

        self.dated(Some(window), date)
    }

    fn dated(&self, window: Option<Window>, date: NaiveDate) -> Result<Window, Unsteppable> {
        match window {
            None => Err(Unsteppable::Unsharded(date)),
            Some(window) if *window.to() > self.now => Err(Unsteppable::Unclosed(window)),
            Some(window) => Ok(window),
        }
    }

    fn built(&self, log: &SeriesLog, closing: Ts<Event>) -> Result<Vec<BuildLog>, P::Error> {
        let client = self.planner.client();
        client.create_build_log::<P::Error>(log.namespace())?;

        client.build_logs::<P::Error>(
            log.namespace(),
            log.series(),
            &Latest::SucceededUntil(closing).into(),
        )
    }

    fn progress(&self, log: &SeriesLog) -> Result<Progress, P::Error> {
        let client = self.planner.client();
        client.create_build_log::<P::Error>(log.namespace())?;
        let read = |latest: Latest| {
            client.build_logs::<P::Error>(log.namespace(), log.series(), &latest.into())
        };
        let progress = Progress::read(&read(Latest::Attempted)?, &read(Latest::Succeeded)?);
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
