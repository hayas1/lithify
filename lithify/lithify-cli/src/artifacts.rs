use chrono::{NaiveDate, NaiveTime};
use chrono_tz::Tz;
use serde::Deserialize;

use lithify_artifact::instant::{self, Instant};
use lithify_artifact::scd1::{self, Scd1};
use lithify_artifact::scd2::{self, Scd2};
use lithify_artifact::snapshot::{self, Snapshot};
use lithify_config::infra::Sink;
use lithify_config::{Declaration, Place};
use lithify_core::artifact::Artifact;
use lithify_core::build_log::Series;
use lithify_core::progress::Step;
use lithify_core::schedule::Schedule;
use lithify_core::sink::{Namespace, WarehouseClient};
use lithify_core::window::Window;

use crate::journal::Journal;
use crate::pipeline::{LocationConfig, WarehouseConfig};
use crate::plan::{Planner, Tables};
use crate::settlement::Settlement;
use crate::stepping::Stepping;
use crate::unrunnable::Unrunnable;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum ArtifactConfig {
    Snapshot(snapshot::Config),
    Instant(instant::Config),
    Scd1(scd1::Config),
    Scd2(scd2::Config),
}

impl ArtifactConfig {
    pub fn as_snapshot(&self) -> Option<&snapshot::Config> {
        match self {
            Self::Snapshot(config) => Some(config),
            Self::Instant(_) | Self::Scd1(_) | Self::Scd2(_) => None,
        }
    }

    pub fn as_instant(&self) -> Option<&instant::Config> {
        match self {
            Self::Instant(config) => Some(config),
            Self::Snapshot(_) | Self::Scd1(_) | Self::Scd2(_) => None,
        }
    }

    pub fn as_scd1(&self) -> Option<&scd1::Config> {
        match self {
            Self::Scd1(config) => Some(config),
            Self::Snapshot(_) | Self::Instant(_) | Self::Scd2(_) => None,
        }
    }

    pub fn as_scd2(&self) -> Option<&scd2::Config> {
        match self {
            Self::Scd2(config) => Some(config),
            Self::Snapshot(_) | Self::Instant(_) | Self::Scd1(_) => None,
        }
    }
}

impl Declaration for ArtifactConfig {
    fn source(&self) -> Option<&'static str> {
        match self {
            Self::Snapshot(_) | Self::Scd2(_) => None,
            Self::Instant(_) => Some(<Instant as Artifact>::From::KIND),
            Self::Scd1(_) => Some(<Scd1 as Artifact>::From::KIND),
        }
    }

    fn cuts(&self) -> Option<Vec<String>> {
        match self {
            Self::Instant(cuts) => Some(cuts.iter().map(|cut| Instant::cut(cut.at())).collect()),
            Self::Snapshot(_) | Self::Scd1(_) | Self::Scd2(_) => None,
        }
    }

    fn schedule(&self, time_zone: Tz) -> Option<Schedule> {
        match self {
            Self::Snapshot(config) => Some(config.schedule(time_zone)),
            Self::Instant(_) | Self::Scd1(_) | Self::Scd2(_) => None,
        }
    }
}

pub struct Placement<'a> {
    places: &'a [LocationConfig],
    namespace: Namespace,
    time_zone: Tz,
}

impl<'a> Placement<'a> {
    fn of(
        sink: &'a Sink<WarehouseConfig, ArtifactConfig>,
        artifact: &'static str,
        places: &'a [LocationConfig],
    ) -> Result<Self, Unrunnable> {
        let place = places.first().ok_or_else(|| Unrunnable::Undeclared {
            sink: sink.name().to_owned(),
            artifact,
        })?;

        Ok(Self {
            places,
            namespace: place.clone().into(),
            time_zone: sink.time_zone(),
        })
    }

    fn whole(
        sink: &'a Sink<WarehouseConfig, ArtifactConfig>,
        artifact: &'static str,
    ) -> Result<Self, Unrunnable> {
        Self::of(
            sink,
            artifact,
            sink.place(artifact)
                .and_then(Place::as_whole)
                .unwrap_or_default(),
        )
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    fn dated(&self, shard: &str) -> Snapshot {
        self.placed(Snapshot::new(self.namespace.clone(), shard))
    }

    fn current(&self) -> Snapshot {
        self.placed(Snapshot::current(self.namespace.clone()))
    }

    fn placed(&self, snapshot: Snapshot) -> Snapshot {
        snapshot.named_in(self.time_zone).copied_into(
            self.places
                .iter()
                .skip(1)
                .map(|place| place.clone().into())
                .collect(),
        )
    }
}

pub struct DeclaredSnapshot<'a> {
    config: &'a snapshot::Config,
    placement: Placement<'a>,
}

impl<'a> DeclaredSnapshot<'a> {
    pub const ARTIFACT: &'static str = Snapshot::KIND;

    pub fn of(sink: &'a Sink<WarehouseConfig, ArtifactConfig>) -> Result<Self, Unrunnable> {
        let config = sink
            .artifact(Self::ARTIFACT)
            .and_then(ArtifactConfig::as_snapshot)
            .ok_or_else(|| Unrunnable::Undeclared {
                sink: sink.name().to_owned(),
                artifact: Self::ARTIFACT,
            })?;

        Ok(Self {
            config,
            placement: Placement::whole(sink, Self::ARTIFACT)?,
        })
    }

    pub fn namespace(&self) -> &Namespace {
        self.placement.namespace()
    }

    pub fn schedule(&self) -> Schedule {
        self.config.schedule(self.placement.time_zone)
    }

    pub fn series(&self) -> Series {
        self.artifact().series()
    }

    pub fn artifact(&self) -> Snapshot {
        self.placement.dated(self.config.shard())
    }
}

pub struct DeclaredInstant<'a> {
    config: &'a snapshot::Config,
    placement: Placement<'a>,
}

impl<'a> DeclaredInstant<'a> {
    pub const ARTIFACT: &'static str = Instant::KIND;

    pub fn of(sink: &'a Sink<WarehouseConfig, ArtifactConfig>) -> Result<Vec<Self>, Unrunnable> {
        sink.artifact(Self::ARTIFACT)
            .and_then(ArtifactConfig::as_instant)
            .into_iter()
            .flatten()
            .filter_map(|config| {
                let places = sink
                    .place(Self::ARTIFACT)?
                    .cut(&Instant::cut(config.at()))?;

                Some(
                    Placement::of(sink, Self::ARTIFACT, places)
                        .map(|placement| Self { config, placement }),
                )
            })
            .collect()
    }

    pub fn namespace(&self) -> &Namespace {
        self.placement.namespace()
    }

    pub fn series(&self) -> Series {
        self.artifact().series()
    }

    pub fn artifact(&self) -> Instant {
        Instant::new(
            self.config.at(),
            self.placement.time_zone,
            self.placement.dated(self.config.shard()),
        )
    }
}

pub struct DeclaredScd1<'a> {
    placement: Placement<'a>,
}

impl<'a> DeclaredScd1<'a> {
    pub const ARTIFACT: &'static str = Scd1::KIND;

    pub fn of(sink: &'a Sink<WarehouseConfig, ArtifactConfig>) -> Result<Option<Self>, Unrunnable> {
        sink.artifact(Self::ARTIFACT)
            .and_then(ArtifactConfig::as_scd1)
            .map(|_| {
                Ok(Self {
                    placement: Placement::whole(sink, Self::ARTIFACT)?,
                })
            })
            .transpose()
    }

    pub fn namespace(&self) -> &Namespace {
        self.placement.namespace()
    }

    pub fn series(&self) -> Series {
        self.artifact().series()
    }

    pub fn artifact(&self) -> Scd1 {
        Scd1::new(self.placement.current())
    }
}

pub struct DeclaredScd2<'a> {
    config: &'a scd2::Config,
    placement: Placement<'a>,
}

impl<'a> DeclaredScd2<'a> {
    pub const ARTIFACT: &'static str = Scd2::KIND;

    pub fn of(sink: &'a Sink<WarehouseConfig, ArtifactConfig>) -> Result<Option<Self>, Unrunnable> {
        sink.artifact(Self::ARTIFACT)
            .and_then(ArtifactConfig::as_scd2)
            .map(|config| {
                Ok(Self {
                    config,
                    placement: Placement::whole(sink, Self::ARTIFACT)?,
                })
            })
            .transpose()
    }

    pub fn namespace(&self) -> &Namespace {
        self.placement.namespace()
    }

    pub fn series(&self) -> Series {
        self.artifact().series()
    }

    pub fn artifact(&self) -> Scd2 {
        Scd2::new(
            self.placement.namespace.clone(),
            self.config.shard(),
            self.placement.time_zone,
        )
    }
}

pub enum Declared<'a> {
    Snapshot(DeclaredSnapshot<'a>),
    Instant(DeclaredInstant<'a>),
    Scd2(DeclaredScd2<'a>),
    Scd1(DeclaredScd1<'a>),
}

impl<'a> Declared<'a> {
    pub const KINDS: [&'static str; 4] = [
        DeclaredSnapshot::ARTIFACT,
        DeclaredInstant::ARTIFACT,
        DeclaredScd1::ARTIFACT,
        DeclaredScd2::ARTIFACT,
    ];

    pub fn of(sink: &'a Sink<WarehouseConfig, ArtifactConfig>) -> Result<Vec<Self>, Unrunnable> {
        Ok(std::iter::once(Self::Snapshot(DeclaredSnapshot::of(sink)?))
            .chain(DeclaredInstant::of(sink)?.into_iter().map(Self::Instant))
            .chain(DeclaredScd2::of(sink)?.map(Self::Scd2))
            .chain(DeclaredScd1::of(sink)?.map(Self::Scd1))
            .collect())
    }

    pub fn series(&self) -> Series {
        match self {
            Self::Snapshot(declared) => declared.series(),
            Self::Instant(declared) => declared.series(),
            Self::Scd2(declared) => declared.series(),
            Self::Scd1(declared) => declared.series(),
        }
    }

    pub fn is(&self, artifact: &str, at: Option<NaiveTime>) -> bool {
        let series = self.series();

        series.artifact() == artifact && at.is_none_or(|at| series.schedule() == Instant::cut(at))
    }

    pub fn on<P>(&self, stepping: &Stepping<P>, date: NaiveDate) -> Option<Window>
    where
        P: Planner,
        Unrunnable: From<<P::Client as WarehouseClient>::Error>,
    {
        let day = stepping.shard(date)?;

        match self {
            Self::Snapshot(declared) => declared.artifact().window(&day),
            Self::Instant(declared) => declared.artifact().window(&day),
            Self::Scd2(declared) => declared.artifact().window(&day),
            Self::Scd1(declared) => declared.artifact().window(&day),
        }
    }

    pub fn next<P>(
        &self,
        stepping: &Stepping<P>,
        taken: &[Window],
    ) -> Result<Option<Step>, Unrunnable>
    where
        P: Planner,
        Unrunnable: From<<P::Client as WarehouseClient>::Error>,
    {
        match self {
            Self::Snapshot(declared) => {
                stepping.next(&declared.artifact(), declared.namespace(), taken)
            }
            Self::Instant(declared) => {
                stepping.next(&declared.artifact(), declared.namespace(), taken)
            }
            Self::Scd2(declared) => {
                stepping.next(&declared.artifact(), declared.namespace(), taken)
            }
            Self::Scd1(declared) => {
                stepping.next(&declared.artifact(), declared.namespace(), taken)
            }
        }
    }

    pub fn take<P, S>(
        &self,
        stepping: &Stepping<P>,
        window: &Window,
        tables: &Tables,
        settlement: &S,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable>
    where
        P: Planner,
        Unrunnable: From<<P::Client as WarehouseClient>::Error>,
        S: Settlement,
    {
        match self {
            Self::Snapshot(declared) => stepping.take(
                &declared.artifact(),
                declared.namespace(),
                window,
                tables,
                settlement,
                journal,
            ),
            Self::Instant(declared) => stepping.take(
                &declared.artifact(),
                declared.namespace(),
                window,
                tables,
                settlement,
                journal,
            ),
            Self::Scd2(declared) => stepping.take(
                &declared.artifact(),
                declared.namespace(),
                window,
                tables,
                settlement,
                journal,
            ),
            Self::Scd1(declared) => stepping.take(
                &declared.artifact(),
                declared.namespace(),
                window,
                tables,
                settlement,
                journal,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use lithify_config::Pipeline;
    use lithify_core::artifact::Artifact;
    use lithify_core::model::TableId;
    use lithify_core::sink::TablePath;
    use lithify_core::ts::Ts;
    use lithify_core::window::Window;

    use super::*;
    use crate::pipeline::CaptureConfig;
    use crate::plan::tests::{INFRA, opened};

    #[test]
    fn a_sink_builds_its_snapshots_in_the_namespace_and_shards_the_pipeline_names() {
        let pipeline: Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> = Pipeline::open(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql"),
        )
        .unwrap();
        let sink = pipeline.sinks().next().unwrap();
        let window = Window::new(
            Ts::try_from(1789862400000).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );

        assert_eq!(
            DeclaredSnapshot::of(sink)
                .unwrap()
                .artifact()
                .target(&TableId::new("shop", "customers"), &window)
                .unwrap(),
            TablePath::new("mysql_shop", "customers_20260920")
        );
    }

    #[test]
    fn the_example_pipeline_builds_every_artifact_into_each_sink() {
        let pipeline: Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> = Pipeline::open(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql"),
        )
        .unwrap();

        for sink in pipeline.sinks() {
            assert_eq!(
                DeclaredInstant::of(sink)
                    .unwrap()
                    .iter()
                    .map(DeclaredInstant::series)
                    .collect::<Vec<_>>(),
                [Series::new("instant", "12:00")]
            );
            assert!(DeclaredScd1::of(sink).unwrap().is_some());
            assert!(DeclaredScd2::of(sink).unwrap().is_some());
        }
    }

    #[test]
    fn a_sink_cuts_only_the_instants_its_warehouse_places() {
        let pipeline = opened(
            "partly-placed-instants",
            &format!(
                "[[artifact.instant]]\nat = \"06:00\"\nshard = \"%Y%m%d%H\"\n[[artifact.instant]]\nat = \"18:00\"\nshard = \"%Y%m%d%H\"\n{INFRA}place.instant.\"06:00\" = [{{ prefix = \"mysql_shop_0600\" }}]\n"
            ),
            "",
        );
        let instants = pipeline
            .sinks()
            .map(|sink| DeclaredInstant::of(sink).unwrap().len())
            .collect::<Vec<_>>();

        assert_eq!(instants, [0, 1]);
    }

    #[test]
    fn a_sink_cuts_its_snapshot_at_the_instant_it_declares_in_the_time_zone_of_the_pipeline() {
        let pipeline = opened(
            "zoned-snapshot",
            &format!(
                "time_zone = \"Asia/Tokyo\"\n{}",
                INFRA.replacen("shard = ", "at = \"09:00\"\nshard = ", 1)
            ),
            "",
        );

        assert_eq!(
            DeclaredSnapshot::of(pipeline.sinks().next().unwrap())
                .unwrap()
                .schedule(),
            Schedule::daily(
                chrono::NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                chrono_tz::Asia::Tokyo
            )
        );
    }

    #[test]
    fn a_snapshot_placed_more_than_once_is_built_into_its_first_placement_and_copied_into_the_others()
     {
        let pipeline = opened(
            "copied-snapshot",
            &INFRA.replacen(
                "place.snapshot = [{ dataset = \"mysql_shop\" }]",
                "place.snapshot = [{ dataset = \"mysql_shop\" }, { project = \"analytics\", dataset = \"shop\" }]",
                1,
            ),
            "",
        );

        assert_eq!(
            DeclaredSnapshot::of(pipeline.sinks().next().unwrap())
                .unwrap()
                .artifact(),
            Snapshot::new("mysql_shop", "%Y%m%d")
                .copied_into(vec![Namespace::from("analytics.shop")])
        );
    }
}
