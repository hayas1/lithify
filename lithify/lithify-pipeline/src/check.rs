use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use serde::Serialize;

use lithify_core::model::TableId;
use lithify_core::sink::{Namespace, TablePath};
use lithify_core::ts::{Event, Replay, Ts};
use lithify_core::window::Window;

use crate::build_log::{BuildLog, Latest, Series, UnreadableBuildLog};
use crate::infra::{Sink, Source};
use crate::schedule::Schedule;
use crate::stepping::SeriesLog;
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
    CaptureChanged {
        logged: String,
        configured: String,
    },
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
            Self::CaptureChanged { logged, configured } => write!(
                formatter,
                "the last run captured as {logged} but the pipeline is captured as {configured} now"
            ),
        }
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
    log: SeriesLog,
}

impl Inspecting {
    pub fn of<C: Serialize, W: SinkWarehouse, A>(
        sink: &Sink<W, A>,
        source: &Source<C>,
        now: Ts<Replay>,
    ) -> Result<Self, Uninspectable> {
        Ok(Self {
            expectation: Expectation::new(
                sink.schedule(),
                source.since(),
                &source
                    .written_capture()
                    .map_err(|unwritten| Uninspectable::Unwritten(unwritten.to_string()))?,
                now,
            ),
            log: sink.scheduled().clone(),
        })
    }

    pub fn namespace(&self) -> &Namespace {
        self.log.namespace()
    }

    pub fn series(&self) -> &Series {
        self.log.series()
    }

    pub async fn read<K, E>(&self, client: &K) -> Result<Inspection, E>
    where
        K: WarehouseControl,
        E: From<K::Error> + From<UnreadableBuildLog>,
    {
        self.expectation
            .read(client, self.namespace(), self.series())
            .await
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
    schedule: Schedule,
    since: Ts<Event>,
    capture: String,
    now: Ts<Replay>,
}

impl Expectation {
    pub fn new(schedule: Schedule, since: Ts<Event>, capture: &str, now: Ts<Replay>) -> Self {
        Self {
            schedule,
            since,
            capture: capture.to_owned(),
            now,
        }
    }

    pub async fn read<K, E>(
        &self,
        client: &K,
        namespace: &Namespace,
        series: &Series,
    ) -> Result<Inspection, E>
    where
        K: WarehouseControl,
        E: From<K::Error> + From<UnreadableBuildLog>,
    {
        if !client.exists(&BuildLog::path(namespace)).await? {
            return Ok(Inspection {
                schedule: self.schedule,
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
            schedule: self.schedule,
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
        let Some(last_run) = latest.iter().map(BuildLog::run_id).max() else {
            return match self.due_until(self.since) {
                Some(_) => vec![Discrepancy::NeverBuilt { since: self.since }],
                None => vec![],
            };
        };

        let built_last = latest.iter().filter(|log| log.run_id() == last_run);

        self.capture_changed(built_last)
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
        self.schedule
            .windows(built_until, self.now.as_event())
            .last()
            .map(|window| *window.to())
    }

    fn capture_changed<'b>(
        &self,
        mut built_last: impl Iterator<Item = &'b BuildLog>,
    ) -> Option<Discrepancy> {
        built_last
            .find(|log| log.capture() != self.capture)
            .map(|log| Discrepancy::CaptureChanged {
                logged: log.capture().to_owned(),
                configured: self.capture.clone(),
            })
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
        let built_until = built.map_or(self.since, |built| *built.window().to());

        self.due_until(built_until)
            .map(|due_until| Discrepancy::Overdue {
                table: table.clone(),
                built_until,
                due_until,
            })
    }
}
