use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use serde::Deserialize;
use ulid::Ulid;

use lithify_artifact::instant::Instant;
use lithify_artifact::scd1::Scd1;
use lithify_artifact::scd2::Scd2;
use lithify_artifact::snapshot::Snapshot;
use lithify_artifact::snapshot_provide::SnapshotProvide;
use lithify_core::artifact::{Artifact, ArtifactName};
use lithify_core::model::TableId;
use lithify_core::sink::{Namespace, TablePath};
use lithify_core::ts::{Replay, Ts};
use lithify_core::window::Window;
use lithify_pipeline::Declaration;
use lithify_pipeline::artifact::ArtifactControl;
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
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArtifactConfig {
    Snapshot(<Snapshot as ArtifactControl>::Config),
    Instant(<Instant as ArtifactControl>::Config),
    Scd1(<Scd1 as ArtifactControl>::Config),
    Scd2(<Scd2 as ArtifactControl>::Config),
    SnapshotProvide(<SnapshotProvide as ArtifactControl>::Config),
}

impl ArtifactConfig {
    pub fn as_snapshot(&self) -> Option<&<Snapshot as ArtifactControl>::Config> {
        match self {
            Self::Snapshot(config) => Some(config),
            Self::Instant(_) | Self::Scd1(_) | Self::Scd2(_) | Self::SnapshotProvide(_) => None,
        }
    }

    fn declared(
        &self,
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        name: &ArtifactName,
    ) -> Result<Declared, Unrunnable> {
        Ok(match self {
            Self::Snapshot(config) => {
                let (snapshot, namespace) = Declared::artifact(sink, name, config)?;
                Declared::Snapshot(snapshot, namespace)
            }
            Self::Instant(config) => {
                let (instant, namespace) = Declared::artifact::<Instant>(sink, name, config)?;
                let base = DeclaredSnapshot::named(sink, name, config.base())?;
                Declared::Instant(instant.based_on(&base.snapshot), namespace)
            }
            Self::Scd1(config) => {
                let (scd1, namespace) = Declared::artifact::<Scd1>(sink, name, config)?;
                let base = DeclaredSnapshot::named(sink, name, config.base())?;
                Declared::Scd1(scd1.based_on(&base.snapshot), namespace)
            }
            Self::Scd2(config) => {
                let (scd2, namespace) = Declared::artifact(sink, name, config)?;
                Declared::Scd2(scd2, namespace)
            }
            Self::SnapshotProvide(config) => {
                let (provide, namespace) =
                    Declared::artifact::<SnapshotProvide>(sink, name, config)?;
                let base = DeclaredSnapshot::named(sink, name, config.base())?;
                Declared::SnapshotProvide(provide.based_on(&base.snapshot), namespace)
            }
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
            Self::SnapshotProvide(_) => SnapshotProvide::KIND,
        }
    }

    fn built_from(&self) -> &'static str {
        match self {
            Self::Snapshot(_) => <Snapshot as Artifact>::From::KIND,
            Self::Instant(_) => <Instant as Artifact>::From::KIND,
            Self::Scd1(_) => <Scd1 as Artifact>::From::KIND,
            Self::Scd2(_) => <Scd2 as Artifact>::From::KIND,
            Self::SnapshotProvide(_) => <SnapshotProvide as Artifact>::From::KIND,
        }
    }

    fn base(&self) -> Option<&ArtifactName> {
        match self {
            Self::Snapshot(_) | Self::Scd2(_) => None,
            Self::Instant(config) => Some(config.base()),
            Self::Scd1(config) => Some(config.base()),
            Self::SnapshotProvide(config) => Some(config.base()),
        }
    }

    fn schedule(&self, time_zone: Tz) -> Option<Schedule> {
        match self {
            Self::Snapshot(config) => Some(config.schedule(time_zone)),
            Self::Instant(_) | Self::Scd1(_) | Self::Scd2(_) | Self::SnapshotProvide(_) => None,
        }
    }
}

pub struct DeclaredSnapshot {
    snapshot: Snapshot,
    namespace: Namespace,
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
            None => {
                let (snapshot, namespace) = Declared::artifact(sink, name, config)?;

                Ok(Self {
                    snapshot,
                    namespace,
                })
            }
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

        let (snapshot, namespace) = Declared::artifact(sink, base, config)?;

        Ok(Self {
            snapshot,
            namespace,
        })
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub fn target(&self, table: &TableId, window: &Window) -> TablePath {
        TablePath::new(
            self.namespace.clone(),
            &self.snapshot.table_name(table, window),
        )
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
    Snapshot(Snapshot, Namespace),
    Instant(Instant, Namespace),
    Scd2(Scd2, Namespace),
    Scd1(Scd1, Namespace),
    SnapshotProvide(SnapshotProvide, Namespace),
}

impl Declared {
    fn artifact<A: ArtifactControl>(
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        name: &ArtifactName,
        config: &A::Config,
    ) -> Result<(A, Namespace), Unrunnable> {
        Ok((
            A::declared(name, config, sink.time_zone()),
            Self::namespace(sink, name)?,
        ))
    }

    fn namespace(
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        artifact: &ArtifactName,
    ) -> Result<Namespace, Unrunnable> {
        sink.place(artifact)
            .map(|place| Namespace::from(place.clone()))
            .ok_or_else(|| Unrunnable::Undeclared {
                sink: sink.name().to_owned(),
                artifact: artifact.clone(),
            })
    }

    pub fn series(&self) -> Series {
        match self {
            Self::Snapshot(artifact, _) => artifact.series(),
            Self::Instant(artifact, _) => artifact.series(),
            Self::Scd2(artifact, _) => artifact.series(),
            Self::Scd1(artifact, _) => artifact.series(),
            Self::SnapshotProvide(artifact, _) => artifact.series(),
        }
    }

    pub fn is(&self, artifact: &ArtifactName) -> bool {
        self.series().artifact() == artifact
    }

    fn base<'s>(&self, snapshot: &'s SeriesLog) -> Option<&'s SeriesLog> {
        match self {
            Self::Snapshot(..) | Self::Scd2(..) => None,
            Self::Instant(..) | Self::Scd1(..) | Self::SnapshotProvide(..) => Some(snapshot),
        }
    }

    async fn next<P: StepTaker<Error = Unrunnable>>(
        &self,
        stepping: &Stepping<'_, P>,
        snapshot: &SeriesLog,
        taken: &[Window],
    ) -> Result<Option<Step>, Unrunnable> {
        let base = self.base(snapshot);

        match self {
            Self::Snapshot(artifact, namespace) => {
                stepping.next(artifact, namespace, base, taken).await
            }
            Self::Instant(artifact, namespace) => {
                stepping.next(artifact, namespace, base, taken).await
            }
            Self::Scd2(artifact, namespace) => {
                stepping.next(artifact, namespace, base, taken).await
            }
            Self::Scd1(artifact, namespace) => {
                stepping.next(artifact, namespace, base, taken).await
            }
            Self::SnapshotProvide(artifact, namespace) => {
                stepping.next(artifact, namespace, base, taken).await
            }
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
        let base = self.base(snapshot);
        journal.window(window);
        let output = match self {
            Self::Snapshot(artifact, namespace) => {
                stepping
                    .take(artifact, namespace, base, window, tables, writer)
                    .await
            }
            Self::Instant(artifact, namespace) => {
                stepping
                    .take(artifact, namespace, base, window, tables, writer)
                    .await
            }
            Self::Scd2(artifact, namespace) => {
                stepping
                    .take(artifact, namespace, base, window, tables, writer)
                    .await
            }
            Self::Scd1(artifact, namespace) => {
                stepping
                    .take(artifact, namespace, base, window, tables, writer)
                    .await
            }
            Self::SnapshotProvide(artifact, namespace) => {
                stepping
                    .take(artifact, namespace, base, window, tables, writer)
                    .await
            }
        }?;

        writer.report(&self.series(), window, output, journal)
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
            snapshot: SeriesLog::of(&snapshot.snapshot, &snapshot.namespace),
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

        assert_eq!(
            DeclaredSnapshot::of(sink)
                .unwrap()
                .target(&TableId::new("shop", "customers"), &window),
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

    const INSTANTS: &str = include_str!("../testdata/artifacts/instants.toml");

    #[test]
    fn a_sink_cuts_only_the_instants_it_places() {
        let pipeline = opened(
            "partly-placed-instants",
            &format!("{ARTIFACTS}{INSTANTS}"),
            &format!("{INFRA}place.six = {{ prefix = \"mysql_shop_0600\" }}\n"),
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
            &format!("{INFRA}place.six = {{ prefix = \"mysql_shop_0600\" }}\n"),
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
        let Ok(Declared::Instant(six, _)) = sink
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
            &format!("{INFRA}place.weekly = {{ prefix = \"mysql_shop_weekly\" }}\n"),
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
}
