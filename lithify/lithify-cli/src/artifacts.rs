use std::collections::BTreeMap;

use chrono::{DateTime, NaiveTime, Utc};
use chrono_tz::Tz;
use serde::Deserialize;
use ulid::Ulid;

use lithify_artifact::instant::{self, Instant};
use lithify_artifact::scd1::{self, Scd1};
use lithify_artifact::scd2::{self, Scd2};
use lithify_artifact::shard::Shard;
use lithify_artifact::snapshot::{self, Snapshot};
use lithify_config::infra::{Sink, Source};
use lithify_config::{Declaration, Place};
use lithify_core::artifact::Artifact;
use lithify_core::build_log::{Run, Series};
use lithify_core::progress::Step;
use lithify_core::runner::{Build, Planner, Tables};
use lithify_core::schedule::Schedule;
use lithify_core::sink::Namespace;
use lithify_core::stepping::{SeriesLog, Stepping};
use lithify_core::window::Window;

use crate::journal::Journal;
use crate::pipeline::{CaptureConfig, LocationConfig, WarehouseConfig};
use crate::report::Report;
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

    fn shard(&self, format: &str) -> Shard {
        Shard::new(format, self.time_zone)
    }

    fn copies(&self) -> Vec<Namespace> {
        self.places
            .iter()
            .skip(1)
            .map(|place| place.clone().into())
            .collect()
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
        Snapshot::new(
            self.placement.namespace.clone(),
            self.placement.shard(self.config.shard()),
        )
        .copied_into(self.placement.copies())
    }

    pub fn build(
        &self,
        source: &Source<CaptureConfig>,
        now: DateTime<Utc>,
    ) -> Result<Build, Unrunnable> {
        let series = self.series();

        Ok(Build::new(
            self.namespace().clone(),
            Run::new(
                Ulid::from_datetime(now.into()),
                now,
                series.artifact(),
                series.schedule(),
                source.name(),
                &source.capture().to_json()?,
                env!("CARGO_PKG_VERSION"),
            ),
            Utc::now,
        ))
    }
}

pub struct DeclaredInstant<'a> {
    config: &'a snapshot::Config,
    placement: Placement<'a>,
    schedule: Schedule,
}

impl<'a> DeclaredInstant<'a> {
    pub const ARTIFACT: &'static str = Instant::KIND;

    pub fn of(sink: &'a Sink<WarehouseConfig, ArtifactConfig>) -> Result<Vec<Self>, Unrunnable> {
        let Some(configs) = sink
            .artifact(Self::ARTIFACT)
            .and_then(ArtifactConfig::as_instant)
        else {
            return Ok(Vec::new());
        };
        let schedule = DeclaredSnapshot::of(sink)?.schedule();

        configs
            .iter()
            .filter_map(|config| {
                let places = sink
                    .place(Self::ARTIFACT)?
                    .cut(&Instant::cut(config.at()))?;

                Some(
                    Placement::of(sink, Self::ARTIFACT, places).map(|placement| Self {
                        config,
                        placement,
                        schedule,
                    }),
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
            self.schedule,
            self.placement.namespace.clone(),
            self.placement.shard(self.config.shard()),
        )
        .copied_into(self.placement.copies())
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
        Scd1::new(self.placement.namespace.clone()).copied_into(self.placement.copies())
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

    fn log(&self) -> SeriesLog {
        let namespace = match self {
            Self::Snapshot(declared) => declared.namespace(),
            Self::Instant(declared) => declared.namespace(),
            Self::Scd2(declared) => declared.namespace(),
            Self::Scd1(declared) => declared.namespace(),
        };

        SeriesLog::new(namespace.clone(), self.series())
    }

    fn base<'s>(&self, snapshot: &'s SeriesLog) -> Option<&'s SeriesLog> {
        match self {
            Self::Snapshot(_) | Self::Scd2(_) => None,
            Self::Instant(_) | Self::Scd1(_) => Some(snapshot),
        }
    }

    fn next<P: Planner<Error = Unrunnable>>(
        &self,
        stepping: &Stepping<P>,
        snapshot: &SeriesLog,
        taken: &[Window],
    ) -> Result<Option<Step>, Unrunnable> {
        let (log, base) = (self.log(), self.base(snapshot));

        match self {
            Self::Snapshot(declared) => stepping.next(&declared.artifact(), &log, base, taken),
            Self::Instant(declared) => stepping.next(&declared.artifact(), &log, base, taken),
            Self::Scd2(declared) => stepping.next(&declared.artifact(), &log, base, taken),
            Self::Scd1(declared) => stepping.next(&declared.artifact(), &log, base, taken),
        }
    }

    fn take<P: Planner<Error = Unrunnable>, R: Report>(
        &self,
        stepping: &Stepping<P>,
        snapshot: &SeriesLog,
        window: &Window,
        tables: &Tables,
        runner: &R,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        let (log, base) = (self.log(), self.base(snapshot));
        journal.window(window);
        let output = match self {
            Self::Snapshot(declared) => {
                stepping.take(&declared.artifact(), &log, base, window, tables, runner)
            }
            Self::Instant(declared) => {
                stepping.take(&declared.artifact(), &log, base, window, tables, runner)
            }
            Self::Scd2(declared) => {
                stepping.take(&declared.artifact(), &log, base, window, tables, runner)
            }
            Self::Scd1(declared) => {
                stepping.take(&declared.artifact(), &log, base, window, tables, runner)
            }
        }?;

        runner.report(log.series(), window, output, journal)
    }
}

pub struct Lineup<'a> {
    snapshot: SeriesLog,
    declared: Vec<Declared<'a>>,
}

impl<'a> Lineup<'a> {
    pub fn of(sink: &'a Sink<WarehouseConfig, ArtifactConfig>) -> Result<Self, Unrunnable> {
        let snapshot = Declared::Snapshot(DeclaredSnapshot::of(sink)?);

        Ok(Self {
            snapshot: snapshot.log(),
            declared: std::iter::once(snapshot)
                .chain(
                    DeclaredInstant::of(sink)?
                        .into_iter()
                        .map(Declared::Instant),
                )
                .chain(DeclaredScd2::of(sink)?.map(Declared::Scd2))
                .chain(DeclaredScd1::of(sink)?.map(Declared::Scd1))
                .collect(),
        })
    }

    pub fn snapshot(&self) -> &SeriesLog {
        &self.snapshot
    }

    pub fn catch_up<P: Planner<Error = Unrunnable>, R: Report>(
        &self,
        stepping: &Stepping<P>,
        named: impl Fn(&Declared) -> bool,
        runner: &R,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        let mut taken: BTreeMap<Series, Vec<Window>> = BTreeMap::new();

        while let Some((series, window)) = self.step(stepping, &named, &taken, runner, journal)? {
            taken.entry(series).or_default().push(window);
        }

        Ok(())
    }

    pub fn step<P: Planner<Error = Unrunnable>, R: Report>(
        &self,
        stepping: &Stepping<P>,
        named: impl Fn(&Declared) -> bool,
        taken: &BTreeMap<Series, Vec<Window>>,
        runner: &R,
        journal: &mut Journal,
    ) -> Result<Option<(Series, Window)>, Unrunnable> {
        for declared in self.declared.iter().filter(|declared| named(declared)) {
            let series = declared.series();
            let taken = taken.get(&series).map(Vec::as_slice).unwrap_or_default();

            if let Some(step) = declared.next(stepping, &self.snapshot, taken)? {
                let tables = Tables::Except(step.left_out().clone());
                declared.take(
                    stepping,
                    &self.snapshot,
                    step.window(),
                    &tables,
                    runner,
                    journal,
                )?;

                return Ok(Some((series, step.window().clone())));
            }
        }

        Ok(None)
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

        let artifact = DeclaredSnapshot::of(sink).unwrap().artifact();

        assert_eq!(
            artifact
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
            Snapshot::new("mysql_shop", Shard::new("%Y%m%d", Tz::UTC))
                .copied_into(vec![Namespace::from("analytics.shop")])
        );
    }
}
