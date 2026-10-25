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
    capture: String,
}

impl<'a, P: Planner> Stepping<'a, P> {
    pub fn new(planner: &'a P, timeline: Timeline, capture: String) -> Self {
        Self {
            planner,
            timeline,
            capture,
        }
    }

    pub fn up_to(self, date: NaiveDate, snapshot: &SeriesLog) -> Result<Self, P::Error> {
        let until = self
            .timeline
            .through(date)
            .ok_or(Unsteppable::Unopened(date))?;
        if until > self.timeline.range().end {
            return Err(Unsteppable::Unclosed(date).into());
        }
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
        let derived = base.is_some();
        let base = base.map(|base| self.progress(base)).transpose()?;

        Ok(progress.step(base.as_ref(), taken, |built, base| {
            self.window(artifact, built, base, derived)
        }))
    }

    fn window<A: Artifact>(
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

    fn built(&self, log: &SeriesLog, closing: Ts<Event>) -> Result<Vec<BuildLog>, P::Error> {
        self.planner.client().build_logs::<P::Error>(
            log.namespace(),
            log.series(),
            &Latest::SucceededUntil(closing).into(),
        )
    }

    fn progress(&self, log: &SeriesLog) -> Result<Progress, P::Error> {
        let client = self.planner.client();
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
