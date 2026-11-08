use chrono_tz::Tz;
use serde::Deserialize;

use lithify_artifact::instant::Instant;
use lithify_artifact::scd1::Scd1;
use lithify_artifact::scd2::Scd2;
use lithify_artifact::snapshot::Snapshot;
use lithify_artifact::snapshot_provide::SnapshotProvide;
use lithify_build::artifact::{Artifact, ArtifactName};
use lithify_pipeline::artifact::ArtifactControl;
use lithify_pipeline::build_log::Series;
use lithify_pipeline::infra::Sink;
use lithify_pipeline::schedule::Schedule;
use lithify_pipeline::{ArtifactFn, Declaration, Declare};

use crate::pipeline::WarehouseConfig;

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

    pub fn snapshot(
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        name: &ArtifactName,
    ) -> Option<Snapshot> {
        let config = sink.artifact(name)?.as_snapshot()?;

        Some(Snapshot::declared(name, config, sink.time_zone()))
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

    fn series(&self, name: &ArtifactName, time_zone: Tz) -> Series {
        match self {
            Self::Snapshot(config) => Snapshot::declared(name, config, time_zone).series(),
            Self::Instant(config) => Instant::declared(name, config, time_zone).series(),
            Self::Scd1(config) => Scd1::declared(name, config, time_zone).series(),
            Self::Scd2(config) => Scd2::declared(name, config, time_zone).series(),
            Self::SnapshotProvide(config) => {
                SnapshotProvide::declared(name, config, time_zone).series()
            }
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

impl Declare for ArtifactConfig {
    async fn declare<F: ArtifactFn>(&self, name: &ArtifactName, zone: Tz, f: F) -> F::Output {
        match self {
            Self::Snapshot(config) => f.call(&Snapshot::declared(name, config, zone)).await,
            Self::Instant(config) => f.call(&Instant::declared(name, config, zone)).await,
            Self::Scd1(config) => f.call(&Scd1::declared(name, config, zone)).await,
            Self::Scd2(config) => f.call(&Scd2::declared(name, config, zone)).await,
            Self::SnapshotProvide(config) => {
                f.call(&SnapshotProvide::declared(name, config, zone)).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use lithify_build::sink::TablePath;
    use lithify_pipeline::Pipeline;
    use lithify_schema::model::TableId;
    use lithify_schema::window::Window;

    use super::*;
    use crate::pipeline::CaptureConfig;

    use lithify_pipeline::infra::Miswiring;
    use lithify_pipeline::{Cause, Misdeclaring};
    use lithify_schema::ts::Ts;

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

        let snapshot = ArtifactConfig::snapshot(sink, &"snapshot".into()).unwrap();

        assert_eq!(
            TablePath::new(
                sink.materialization(&"snapshot".into())
                    .unwrap()
                    .namespace()
                    .clone(),
                &snapshot.table_name(&TableId::new("shop", "customers"), &window)
            ),
            TablePath::new("mysql_shop", "customers_20260920")
        );
    }

    fn declared(sink: &Sink<WarehouseConfig, ArtifactConfig>) -> Vec<Series> {
        sink.materializations()
            .map(|(_, materialization)| materialization.series().clone())
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
    fn a_sink_placing_two_snapshots_is_refused() {
        let unreadable = opening(
            "two-snapshots",
            &format!(
                "{ARTIFACTS}[[artifact]]\nname = \"weekly\"\nkind = \"snapshot\"\nshard = \"%Y%m%d\"\n"
            ),
            &format!("{INFRA}place.weekly = {{ prefix = \"mysql_shop_weekly\" }}\n"),
            "",
        )
        .unwrap_err();

        assert!(matches!(
            unreadable.cause(),
            Cause::Miswiring(Miswiring::ScheduledTwice(sink)) if sink == "duckdb_s3"
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
            pipeline.sinks().next().unwrap().schedule(),
            Schedule::daily(
                chrono::NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                chrono_tz::Asia::Tokyo
            )
        );
    }
}
