use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use serde::Deserialize;
use ulid::Ulid;

use lithify_artifact::instant::Instant;
use lithify_artifact::scd1::Scd1;
use lithify_artifact::scd2::Scd2;
use lithify_artifact::snapshot::Snapshot;
use lithify_core::artifact::{Artifact, ArtifactName};
use lithify_core::sink::Namespace;
use lithify_core::ts::{Replay, Ts};
use lithify_core::window::Window;
use lithify_pipeline::Declaration;
use lithify_pipeline::artifact::{ArtifactControl, Placement};
use lithify_pipeline::build_log::{Run, Series};
use lithify_pipeline::infra::{Sink, Source};
use lithify_pipeline::progress::Step;
use lithify_pipeline::runner::{BuildLogWrite, StepTaker, Tables};
use lithify_pipeline::schedule::Schedule;
use lithify_pipeline::stepping::{SeriesLog, Stepping};

use crate::journal::Journal;
use crate::pipeline::{CaptureConfig, WarehouseConfig};
use crate::report::Report;
use crate::unrunnable::Unrunnable;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ArtifactConfig {
    Snapshot(<Snapshot as ArtifactControl>::Config),
    Instant(<Instant as ArtifactControl>::Config),
    Scd1(<Scd1 as ArtifactControl>::Config),
    Scd2(<Scd2 as ArtifactControl>::Config),
}

impl ArtifactConfig {
    pub fn as_snapshot(&self) -> Option<&<Snapshot as ArtifactControl>::Config> {
        match self {
            Self::Snapshot(config) => Some(config),
            Self::Instant(_) | Self::Scd1(_) | Self::Scd2(_) => None,
        }
    }

    fn declared(
        &self,
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        name: &ArtifactName,
    ) -> Result<Declared, Unrunnable> {
        Ok(match self {
            Self::Snapshot(config) => Declared::Snapshot(Declared::artifact(sink, name, config)?),
            Self::Instant(config) => Declared::Instant(
                Declared::artifact::<Instant>(sink, name, config)?
                    .based_on(&DeclaredSnapshot::named(sink, name, config.base())?.snapshot),
            ),
            Self::Scd1(config) => Declared::Scd1(
                Declared::artifact::<Scd1>(sink, name, config)?
                    .based_on(&DeclaredSnapshot::named(sink, name, config.base())?.snapshot),
            ),
            Self::Scd2(config) => Declared::Scd2(Declared::artifact(sink, name, config)?),
        })
    }
}

impl Declaration for ArtifactConfig {
    fn kind(&self) -> &'static str {
        match self {
            Self::Snapshot(_) => Snapshot::KIND,
            Self::Instant(_) => Instant::KIND,
            Self::Scd1(_) => Scd1::KIND,
            Self::Scd2(_) => Scd2::KIND,
        }
    }

    fn built_from(&self) -> &'static str {
        match self {
            Self::Snapshot(_) => <Snapshot as ArtifactControl>::From::KIND,
            Self::Instant(_) => <Instant as ArtifactControl>::From::KIND,
            Self::Scd1(_) => <Scd1 as ArtifactControl>::From::KIND,
            Self::Scd2(_) => <Scd2 as ArtifactControl>::From::KIND,
        }
    }

    fn base(&self) -> Option<&ArtifactName> {
        match self {
            Self::Snapshot(_) | Self::Scd2(_) => None,
            Self::Instant(config) => Some(config.base()),
            Self::Scd1(config) => Some(config.base()),
        }
    }

    fn schedule(&self, time_zone: Tz) -> Option<Schedule> {
        match self {
            Self::Snapshot(config) => Some(config.schedule(time_zone)),
            Self::Instant(_) | Self::Scd1(_) | Self::Scd2(_) => None,
        }
    }
}

pub struct DeclaredSnapshot {
    snapshot: Snapshot,
}

impl DeclaredSnapshot {
    pub fn of(sink: &Sink<WarehouseConfig, ArtifactConfig>) -> Result<Self, Unrunnable> {
        let mut snapshots = sink
            .artifacts()
            .filter_map(|(name, declared)| Some((name, declared.as_snapshot()?)));
        let (name, config) = snapshots.next().ok_or_else(|| Unrunnable::Undeclared {
            sink: sink.name().to_owned(),
            artifact: ArtifactName::from(Snapshot::KIND),
        })?;

        match snapshots.next() {
            Some(_) => Err(Unrunnable::SnapshotPlacedTwice(sink.name().to_owned())),
            None => Ok(Self {
                snapshot: Declared::artifact(sink, name, config)?,
            }),
        }
    }

    fn named(
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        artifact: &ArtifactName,
        base: &ArtifactName,
    ) -> Result<Self, Unrunnable> {
        let config = sink
            .artifact(base)
            .and_then(ArtifactConfig::as_snapshot)
            .ok_or_else(|| Unrunnable::Unbased {
                artifact: artifact.clone(),
                base: base.clone(),
            })?;

        Ok(Self {
            snapshot: Declared::artifact(sink, base, config)?,
        })
    }

    pub fn namespace(&self) -> &Namespace {
        self.snapshot.namespace()
    }

    pub fn schedule(&self) -> Schedule {
        self.snapshot.schedule()
    }

    pub fn series(&self) -> Series {
        self.snapshot.series()
    }

    pub fn artifact(&self) -> Snapshot {
        self.snapshot.clone()
    }

    pub fn build(
        &self,
        source: &Source<CaptureConfig>,
        now: Ts<Replay>,
    ) -> Result<BuildLogWrite, Unrunnable> {
        let series = self.series();

        Ok(BuildLogWrite::new(
            self.namespace().clone(),
            Run::new(
                Ulid::from_datetime(DateTime::<Utc>::from(now).into()),
                now,
                series.artifact(),
                series.schedule(),
                source.name(),
                &source.written_capture()?,
                env!("CARGO_PKG_VERSION"),
            ),
        ))
    }
}

pub enum Declared {
    Snapshot(Snapshot),
    Instant(Instant),
    Scd2(Scd2),
    Scd1(Scd1),
}

impl Declared {
    fn artifact<A: ArtifactControl>(
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        name: &ArtifactName,
        config: &A::Config,
    ) -> Result<A, Unrunnable> {
        Ok(A::declared(
            name,
            config,
            &Self::placement(sink, name)?,
            sink.time_zone(),
        ))
    }

    fn placement(
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        artifact: &ArtifactName,
    ) -> Result<Placement, Unrunnable> {
        let mut places = sink
            .place(artifact)
            .unwrap_or_default()
            .iter()
            .map(|place| Namespace::from(place.clone()));
        let namespace = places.next().ok_or_else(|| Unrunnable::Undeclared {
            sink: sink.name().to_owned(),
            artifact: artifact.clone(),
        })?;

        Ok(Placement::new(namespace, places.collect()))
    }

    pub fn series(&self) -> Series {
        match self {
            Self::Snapshot(artifact) => artifact.series(),
            Self::Instant(artifact) => artifact.series(),
            Self::Scd2(artifact) => artifact.series(),
            Self::Scd1(artifact) => artifact.series(),
        }
    }

    pub fn is(&self, artifact: &ArtifactName) -> bool {
        self.series().artifact() == artifact
    }

    fn log(&self) -> SeriesLog {
        let namespace = match self {
            Self::Snapshot(artifact) => artifact.namespace(),
            Self::Instant(artifact) => artifact.namespace(),
            Self::Scd2(artifact) => artifact.namespace(),
            Self::Scd1(artifact) => artifact.namespace(),
        };

        SeriesLog::new(namespace.clone(), self.series())
    }

    fn base<'s>(&self, snapshot: &'s SeriesLog) -> Option<&'s SeriesLog> {
        match self {
            Self::Snapshot(_) | Self::Scd2(_) => None,
            Self::Instant(_) | Self::Scd1(_) => Some(snapshot),
        }
    }

    async fn next<P: StepTaker<Error = Unrunnable>>(
        &self,
        stepping: &Stepping<'_, P>,
        snapshot: &SeriesLog,
        taken: &[Window],
    ) -> Result<Option<Step>, Unrunnable> {
        let (log, base) = (self.log(), self.base(snapshot));

        match self {
            Self::Snapshot(artifact) => stepping.next(artifact, &log, base, taken).await,
            Self::Instant(artifact) => stepping.next(artifact, &log, base, taken).await,
            Self::Scd2(artifact) => stepping.next(artifact, &log, base, taken).await,
            Self::Scd1(artifact) => stepping.next(artifact, &log, base, taken).await,
        }
    }

    async fn take<P: StepTaker<Error = Unrunnable>, B: Report>(
        &self,
        stepping: &Stepping<'_, P>,
        snapshot: &SeriesLog,
        window: &Window,
        tables: &Tables,
        writer: &B,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        let (log, base) = (self.log(), self.base(snapshot));
        journal.window(window);
        let output = match self {
            Self::Snapshot(artifact) => {
                stepping
                    .take(artifact, &log, base, window, tables, writer)
                    .await
            }
            Self::Instant(artifact) => {
                stepping
                    .take(artifact, &log, base, window, tables, writer)
                    .await
            }
            Self::Scd2(artifact) => {
                stepping
                    .take(artifact, &log, base, window, tables, writer)
                    .await
            }
            Self::Scd1(artifact) => {
                stepping
                    .take(artifact, &log, base, window, tables, writer)
                    .await
            }
        }?;

        writer.report(log.series(), window, output, journal)
    }
}

pub struct Lineup {
    snapshot: SeriesLog,
    declared: Vec<Declared>,
}

impl Lineup {
    pub fn of(sink: &Sink<WarehouseConfig, ArtifactConfig>) -> Result<Self, Unrunnable> {
        let snapshot = DeclaredSnapshot::of(sink)?;

        Ok(Self {
            snapshot: SeriesLog::new(snapshot.namespace().clone(), snapshot.series()),
            declared: sink
                .artifacts()
                .map(|(name, declared)| declared.declared(sink, name))
                .collect::<Result<_, _>>()?,
        })
    }

    pub fn snapshot(&self) -> &SeriesLog {
        &self.snapshot
    }

    pub async fn catch_up<P: StepTaker<Error = Unrunnable>, B: Report>(
        &self,
        stepping: &Stepping<'_, P>,
        named: impl Fn(&Declared) -> bool,
        writer: &B,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        let mut taken: BTreeMap<Series, Vec<Window>> = BTreeMap::new();

        while let Some((series, window)) =
            self.step(stepping, &named, &taken, writer, journal).await?
        {
            taken.entry(series).or_default().push(window);
        }

        Ok(())
    }

    pub async fn step<P: StepTaker<Error = Unrunnable>, B: Report>(
        &self,
        stepping: &Stepping<'_, P>,
        named: impl Fn(&Declared) -> bool,
        taken: &BTreeMap<Series, Vec<Window>>,
        writer: &B,
        journal: &mut Journal,
    ) -> Result<Option<(Series, Window)>, Unrunnable> {
        for declared in self.declared.iter().filter(|declared| named(declared)) {
            let series = declared.series();
            let taken = taken.get(&series).map(Vec::as_slice).unwrap_or_default();

            if let Some(step) = declared.next(stepping, &self.snapshot, taken).await? {
                let tables = Tables::Except(step.left_out().clone());
                declared
                    .take(
                        stepping,
                        &self.snapshot,
                        step.window(),
                        &tables,
                        writer,
                        journal,
                    )
                    .await?;

                return Ok(Some((series, step.window().clone())));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use lithify_artifact::shard::Shard;
    use lithify_core::artifact::Artifact;
    use lithify_core::model::TableId;
    use lithify_core::sink::TablePath;
    use lithify_core::window::Window;
    use lithify_pipeline::Pipeline;

    use super::*;
    use crate::pipeline::CaptureConfig;
    use lithify_pipeline::build_log::{Status, TableBuilt};

    use lithify_pipeline::{Cause, Misdeclaring};

    use crate::step::tests::{ARTIFACTS, INFRA, opened, opening};

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

    fn declared(sink: &Sink<WarehouseConfig, ArtifactConfig>) -> Vec<Series> {
        sink.artifacts()
            .map(|(name, declared)| declared.declared(sink, name).unwrap().series())
            .collect()
    }

    #[test]
    fn the_example_pipeline_builds_every_artifact_into_each_sink() {
        let pipeline: Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> = Pipeline::open(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql"),
        )
        .unwrap();

        for sink in pipeline.sinks() {
            assert_eq!(
                declared(sink),
                [
                    Series::new("snapshot".into(), "daily"),
                    Series::new("instant_0900".into(), "09:00"),
                    Series::new("instant_1200".into(), "12:00"),
                    Series::new("instant_1800".into(), "18:00"),
                    Series::new("scd1".into(), "current"),
                    Series::new("scd2".into(), "daily"),
                ]
            );
        }
    }

    const INSTANTS: &str = "[[artifact]]\nname = \"six\"\nkind = \"instant\"\nbase = \"snapshot\"\nat = \"06:00\"\nshard = \"%Y%m%d%H\"\n\n[[artifact]]\nname = \"eighteen\"\nkind = \"instant\"\nbase = \"snapshot\"\nat = \"18:00\"\nshard = \"%Y%m%d%H\"\n";

    #[test]
    fn a_sink_cuts_only_the_instants_it_places() {
        let pipeline = opened(
            "partly-placed-instants",
            &format!("{ARTIFACTS}{INSTANTS}"),
            &format!("{INFRA}place.six = [{{ prefix = \"mysql_shop_0600\" }}]\n"),
            "",
        );

        assert_eq!(
            pipeline.sinks().map(declared).collect::<Vec<_>>(),
            [
                vec![Series::new("snapshot".into(), "daily")],
                vec![
                    Series::new("snapshot".into(), "daily"),
                    Series::new("six".into(), "06:00")
                ]
            ]
        );
    }

    #[test]
    fn an_instant_cut_off_a_snapshot_is_built_onto_that_snapshot() {
        let pipeline = opened(
            "based-instant",
            &format!("{ARTIFACTS}{INSTANTS}"),
            &format!("{INFRA}place.six = [{{ prefix = \"mysql_shop_0600\" }}]\n"),
            "",
        );
        let sink = pipeline.sinks().nth(1).unwrap();
        let built = |series: Series| {
            TableBuilt::new(
                Some(TableId::new("shop", "people")),
                Some(TableId::new("shop", "people")),
                None,
                Some(TablePath::new("mysql_shop", "people_20260919")),
                Window::new(
                    Ts::try_from(1789776000000).unwrap(),
                    Ts::try_from(1789862400000).unwrap(),
                ),
                Status::Succeeded,
                Ts::try_from(1789862400000).unwrap(),
            )
            .logged(&Run::new(
                Ulid::nil(),
                Ts::try_from(0).unwrap(),
                series.artifact(),
                series.schedule(),
                "primary",
                "{}",
                "0.1.0",
            ))
        };
        let Ok(Declared::Instant(six)) = sink
            .artifact(&"six".into())
            .unwrap()
            .declared(sink, &"six".into())
        else {
            panic!("six is an instant");
        };
        let window = Window::new(
            Ts::try_from(1789862400000).unwrap(),
            Ts::try_from(1789884000000).unwrap(),
        );
        let onto = built(Series::new("snapshot".into(), "daily"));
        let elsewhere = built(Series::new("weekly".into(), "daily"));

        assert_eq!(
            six.base(
                &[elsewhere.clone(), onto.clone()],
                &TableId::new("shop", "people"),
                &window
            ),
            Some(&onto)
        );
        assert_eq!(
            six.base(&[elsewhere], &TableId::new("shop", "people"), &window),
            None
        );
    }

    #[test]
    fn a_sink_placing_two_snapshots_is_refused() {
        let pipeline = opened(
            "two-snapshots",
            &format!(
                "{ARTIFACTS}[[artifact]]\nname = \"weekly\"\nkind = \"snapshot\"\nshard = \"%Y%m%d\"\n"
            ),
            &format!("{INFRA}place.weekly = [{{ prefix = \"mysql_shop_weekly\" }}]\n"),
            "",
        );

        assert!(matches!(
            DeclaredSnapshot::of(pipeline.sinks().nth(1).unwrap()),
            Err(Unrunnable::SnapshotPlacedTwice(sink)) if sink == "duckdb_s3"
        ));
    }

    #[test]
    fn an_instant_built_from_what_is_no_snapshot_is_refused_as_it_is_declared() {
        let unreadable = opening(
            "instant-off-scd2",
            &format!(
                "{ARTIFACTS}[[artifact]]\nname = \"scd2\"\nkind = \"scd2\"\nshard = \"%Y%m%d\"\n\n[[artifact]]\nname = \"six\"\nkind = \"instant\"\nbase = \"scd2\"\nat = \"06:00\"\nshard = \"%Y%m%d%H\"\n"
            ),
            INFRA,
            "",
        )
        .unwrap_err();

        assert!(matches!(
            unreadable.cause(),
            Cause::Misdeclaring(Misdeclaring::Misbased { artifact, base, kind })
                if artifact.as_str() == "six" && base.as_str() == "scd2" && *kind == "snapshot"
        ));
    }

    #[test]
    fn a_sink_cuts_its_snapshot_at_the_instant_it_declares_in_the_time_zone_of_the_pipeline() {
        let pipeline = opened(
            "zoned-snapshot",
            &ARTIFACTS.replacen("shard = ", "at = \"09:00\"\nshard = ", 1),
            &format!("time_zone = \"Asia/Tokyo\"\n{INFRA}"),
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
            ARTIFACTS,
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
