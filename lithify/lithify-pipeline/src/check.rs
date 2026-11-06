use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use serde::Serialize;

use lithify_core::model::TableId;
use lithify_core::sink::TablePath;
use lithify_core::ts::{Event, Replay, Ts};
use lithify_core::window::Window;

use crate::build_log::{BuildLog, Latest, UnreadableBuildLog};
use crate::infra::{Sink, Source};
use crate::materialization::Materialization;
use crate::schedule::{Schedule, Timeline};
use crate::warehouse::{SinkWarehouse, WarehouseControl, WarehouseFn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Discrepancy {
    NeverBuilt {
        since: Ts<Event>,
    },
    Overdue {
        table: TableId,
        built_until: Ts<Event>,
        due_until: Ts<Event>,
    },
    Failed {
        table: TableId,
        window: Window,
        error: String,
    },
    Missing {
        table: TableId,
        target: TablePath,
    },
    CaptureChanged(CaptureChanged),
}

impl Display for Discrepancy {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NeverBuilt { since } => write!(formatter, "nothing is built since {since}"),
            Self::Overdue {
                table,
                built_until,
                due_until,
            } => write!(
                formatter,
                "{table} is built until {built_until} but due until {due_until}"
            ),
            Self::Failed {
                table,
                window,
                error,
            } => write!(
                formatter,
                "{table} failed to build the window from {} to {}: {error}",
                window.from(),
                window.to()
            ),
            Self::Missing { table, target } => write!(
                formatter,
                "{table} is logged as built into {target} but {target} is not there"
            ),
            Self::CaptureChanged(changed) => changed.fmt(formatter),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureChanged {
    logged: String,
    configured: String,
}

impl CaptureChanged {
    pub fn new(logged: &str, configured: &str) -> Self {
        Self {
            logged: logged.to_owned(),
            configured: configured.to_owned(),
        }
    }
}

impl Display for CaptureChanged {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "the last run captured as {} but the pipeline is captured as {} now",
            self.logged, self.configured
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inspection {
    schedule: Schedule,
    latest: Vec<BuildLog>,
    discrepancies: Vec<Discrepancy>,
}

impl Inspection {
    pub fn schedule(&self) -> Schedule {
        self.schedule
    }

    pub fn latest(&self) -> &[BuildLog] {
        &self.latest
    }

    pub fn discrepancies(&self) -> &[Discrepancy] {
        &self.discrepancies
    }
}

pub struct Inspecting {
    expectation: Expectation,
    scheduled: Materialization,
}

impl Inspecting {
    pub fn of<C: Serialize, W: SinkWarehouse, A>(
        sink: &Sink<W, A>,
        source: &Source<C>,
        now: Ts<Replay>,
    ) -> Result<Self, Uninspectable> {
        Ok(Self {
            expectation: Expectation::of(sink, source, now)
                .map_err(|unwritten| Uninspectable::Unwritten(unwritten.to_string()))?,
            scheduled: sink.scheduled().clone(),
        })
    }

    pub fn scheduled(&self) -> &Materialization {
        &self.scheduled
    }

    pub async fn read<K, E>(&self, client: &K) -> Result<Inspection, E>
    where
        K: WarehouseControl,
        E: From<K::Error> + From<UnreadableBuildLog>,
    {
        self.expectation.read(client, &self.scheduled).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Uninspectable {
    Unwritten(String),
    Unread(String),
}

impl Display for Uninspectable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unwritten(cause) => {
                write!(formatter, "the capture could not be written: {cause}")
            }
            Self::Unread(cause) => formatter.write_str(cause),
        }
    }
}

impl std::error::Error for Uninspectable {}

impl WarehouseFn for &Inspecting {
    type Output = Result<Inspection, Uninspectable>;

    async fn call<K: WarehouseControl>(self, client: &K) -> Self::Output {
        self.read::<K, Unread>(client)
            .await
            .map_err(|Unread(cause)| Uninspectable::Unread(cause))
    }
}

struct Unread(String);

impl<E: std::error::Error> From<E> for Unread {
    fn from(cause: E) -> Self {
        Self(cause.to_string())
    }
}

pub struct Expectation {
    timeline: Timeline,
    capture: String,
}

impl Expectation {
    pub fn new(timeline: Timeline, capture: &str) -> Self {
        Self {
            timeline,
            capture: capture.to_owned(),
        }
    }

    pub fn of<C: Serialize, W: SinkWarehouse, A>(
        sink: &Sink<W, A>,
        source: &Source<C>,
        now: Ts<Replay>,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self::new(
            Timeline::new(sink.schedule(), source.since()..now.as_event()),
            &source.written_capture()?,
        ))
    }

    pub fn timeline(&self) -> &Timeline {
        &self.timeline
    }

    pub fn capture(&self) -> &str {
        &self.capture
    }

    pub fn up_to(self, until: Ts<Event>) -> Self {
        Self {
            timeline: self.timeline.up_to(until),
            ..self
        }
    }

    pub async fn read<K, E>(
        &self,
        client: &K,
        materialization: &Materialization,
    ) -> Result<Inspection, E>
    where
        K: WarehouseControl,
        E: From<K::Error> + From<UnreadableBuildLog>,
    {
        let (namespace, series) = (materialization.namespace(), materialization.series());
        if !client.exists(&BuildLog::path(namespace)).await? {
            return Ok(Inspection {
                schedule: self.timeline.schedule(),
                latest: Vec::new(),
                discrepancies: self.discrepancies(&[], &[], &BTreeSet::new()),
            });
        }

        let latest = client
            .build_logs::<E>(namespace, series, &Latest::Attempted.into())
            .await?;
        let succeeded = client
            .build_logs::<E>(namespace, series, &Latest::Succeeded.into())
            .await?;
        let mut present = BTreeSet::new();
        for target in succeeded.iter().filter_map(BuildLog::target) {
            if client.exists(target).await? {
                present.insert(target.clone());
            }
        }

        Ok(Inspection {
            schedule: self.timeline.schedule(),
            discrepancies: self.discrepancies(&latest, &succeeded, &present),
            latest,
        })
    }

    // TODO: expect the tables the catalog holds at the end of the last closed window rather than
    // only the ones the build log has seen, which leaves out a table never built at all.
    pub fn discrepancies(
        &self,
        latest: &[BuildLog],
        succeeded: &[BuildLog],
        present: &BTreeSet<TablePath>,
    ) -> Vec<Discrepancy> {
        if latest.is_empty() {
            let since = self.timeline.range().start;

            return match self.due_until(since) {
                Some(_) => vec![Discrepancy::NeverBuilt { since }],
                None => vec![],
            };
        }

        self.capture_changed(latest)
            .map(Discrepancy::CaptureChanged)
            .into_iter()
            .chain(
                latest
                    .iter()
                    .filter_map(|log| log.table_to().map(|table| (table, log)))
                    .flat_map(|(table, log)| {
                        let built = log.last_success(succeeded);

                        self.missing_from(table, built, present).into_iter().chain(
                            self.failed(table, log)
                                .or_else(|| self.overdue(table, built)),
                        )
                    }),
            )
            .collect()
    }

    fn due_until(&self, built_until: Ts<Event>) -> Option<Ts<Event>> {
        self.timeline
            .windows(built_until)
            .last()
            .map(|window| *window.to())
    }

    pub fn capture_changed(&self, latest: &[BuildLog]) -> Option<CaptureChanged> {
        let last_run = latest.iter().map(BuildLog::run_id).max()?;

        latest
            .iter()
            .filter(|log| log.run_id() == last_run)
            .find(|log| log.capture() != self.capture)
            .map(|log| CaptureChanged::new(log.capture(), &self.capture))
    }

    fn missing_from(
        &self,
        table: &TableId,
        built: Option<&BuildLog>,
        present: &BTreeSet<TablePath>,
    ) -> Option<Discrepancy> {
        built.and_then(|built| self.missing(table, built, present))
    }

    fn failed(&self, table: &TableId, log: &BuildLog) -> Option<Discrepancy> {
        log.status().error().map(|error| Discrepancy::Failed {
            table: table.clone(),
            window: log.window().clone(),
            error: error.to_owned(),
        })
    }

    fn missing(
        &self,
        table: &TableId,
        built: &BuildLog,
        present: &BTreeSet<TablePath>,
    ) -> Option<Discrepancy> {
        let target = built.target()?;

        (!present.contains(target)).then(|| Discrepancy::Missing {
            table: table.clone(),
            target: target.clone(),
        })
    }

    fn overdue(&self, table: &TableId, built: Option<&BuildLog>) -> Option<Discrepancy> {
        let built_until = built.map_or(self.timeline.range().start, |built| *built.window().to());

        self.due_until(built_until)
            .map(|due_until| Discrepancy::Overdue {
                table: table.clone(),
                built_until,
                due_until,
            })
    }
}
