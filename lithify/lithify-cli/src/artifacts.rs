use std::collections::BTreeMap;

use chrono_tz::Tz;
use serde::Deserialize;

use lithify_artifact::instant::{self, Instant};
use lithify_artifact::masked::{self, Masked};
use lithify_artifact::snapshot::{self, Snapshot};
use lithify_config::Declaration;
use lithify_config::infra::Sink;
use lithify_core::artifact::Artifact;
use lithify_core::build_log::Series;
use lithify_core::model::{Mask, TableId};
use lithify_core::schedule::Schedule;
use lithify_core::sink::Namespace;

use crate::pipeline::{LocationConfig, WarehouseConfig};
use crate::unrunnable::Unrunnable;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum ArtifactConfig {
    Snapshot(snapshot::Config<LocationConfig>),
    Instant(instant::Config<LocationConfig>),
    Masked(masked::Config<LocationConfig>),
}

impl ArtifactConfig {
    pub fn as_snapshot(&self) -> Option<&snapshot::Config<LocationConfig>> {
        match self {
            Self::Snapshot(config) => Some(config),
            Self::Instant(_) | Self::Masked(_) => None,
        }
    }

    pub fn as_instant(&self) -> Option<&instant::Config<LocationConfig>> {
        match self {
            Self::Instant(config) => Some(config),
            Self::Snapshot(_) | Self::Masked(_) => None,
        }
    }

    pub fn as_masked(&self) -> Option<&masked::Config<LocationConfig>> {
        match self {
            Self::Masked(config) => Some(config),
            Self::Snapshot(_) | Self::Instant(_) => None,
        }
    }
}

impl Declaration for ArtifactConfig {
    type Location = LocationConfig;

    fn source(&self) -> Option<&'static str> {
        match self {
            Self::Snapshot(_) => None,
            Self::Instant(_) => Some(<Instant as Artifact>::From::KIND),
            Self::Masked(_) => Some(<Masked as Artifact>::From::KIND),
        }
    }

    fn places(&self) -> impl Iterator<Item = &LocationConfig> {
        match self {
            Self::Snapshot(config) => config.place().iter().collect::<Vec<_>>(),
            Self::Instant(cuts) => cuts.iter().flat_map(|cut| cut.place()).collect(),
            Self::Masked(config) => config.place().iter().collect(),
        }
        .into_iter()
    }

    fn schedule(&self, time_zone: Tz) -> Option<Schedule> {
        match self {
            Self::Snapshot(config) => Some(config.schedule(time_zone)),
            Self::Instant(_) | Self::Masked(_) => None,
        }
    }
}

pub struct Placement<'a> {
    shard: &'a str,
    places: &'a [LocationConfig],
    namespace: Namespace,
    time_zone: Tz,
}

impl<'a> Placement<'a> {
    fn of(
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        artifact: &'static str,
        shard: &'a str,
        places: &'a [LocationConfig],
    ) -> Result<Self, Unrunnable> {
        let place = places.first().ok_or_else(|| Unrunnable::Undeclared {
            sink: sink.name().to_owned(),
            artifact,
        })?;

        Ok(Self {
            shard,
            places,
            namespace: place.clone().into(),
            time_zone: sink.time_zone(),
        })
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot::new(self.namespace.clone(), self.shard)
            .named_in(self.time_zone)
            .copied_into(
                self.places
                    .iter()
                    .skip(1)
                    .map(|place| place.clone().into())
                    .collect(),
            )
    }
}

pub struct DeclaredSnapshot<'a> {
    config: &'a snapshot::Config<LocationConfig>,
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
            placement: Placement::of(sink, Self::ARTIFACT, config.shard(), config.place())?,
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
        self.placement.snapshot()
    }
}

pub struct DeclaredInstant<'a> {
    config: &'a snapshot::Config<LocationConfig>,
    placement: Placement<'a>,
}

impl<'a> DeclaredInstant<'a> {
    pub const ARTIFACT: &'static str = Instant::KIND;

    pub fn of(sink: &'a Sink<WarehouseConfig, ArtifactConfig>) -> Result<Vec<Self>, Unrunnable> {
        sink.artifact(Self::ARTIFACT)
            .and_then(ArtifactConfig::as_instant)
            .into_iter()
            .flatten()
            .map(|config| {
                Ok(Self {
                    config,
                    placement: Placement::of(sink, Self::ARTIFACT, config.shard(), config.place())?,
                })
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
            self.placement.snapshot(),
        )
    }
}

pub struct DeclaredMasked<'a> {
    placement: Placement<'a>,
    masks: &'a BTreeMap<TableId, BTreeMap<String, Mask>>,
}

impl<'a> DeclaredMasked<'a> {
    pub const ARTIFACT: &'static str = Masked::KIND;

    pub fn of(
        sink: &'a Sink<WarehouseConfig, ArtifactConfig>,
        masks: &'a BTreeMap<TableId, BTreeMap<String, Mask>>,
    ) -> Result<Option<Self>, Unrunnable> {
        sink.artifact(Self::ARTIFACT)
            .and_then(ArtifactConfig::as_masked)
            .map(|config| {
                Ok(Self {
                    placement: Placement::of(sink, Self::ARTIFACT, config.shard(), config.place())?,
                    masks,
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

    pub fn artifact(&self) -> Masked {
        Masked::new(self.placement.snapshot(), self.masks.clone())
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
    fn a_sink_cuts_its_snapshot_at_the_instant_it_declares_in_the_time_zone_of_the_pipeline() {
        let directory =
            std::env::temp_dir().join(format!("lithify-cli-zoned-snapshot-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let infra = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql/infra.toml"),
        )
        .unwrap();
        std::fs::write(
            directory.join("infra.toml"),
            format!(
                "time_zone = \"Asia/Tokyo\"\n{}",
                infra.replacen("shard = ", "at = \"09:00\"\nshard = ", 1)
            ),
        )
        .unwrap();
        std::fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql/table.toml"),
            directory.join("table.toml"),
        )
        .unwrap();

        let pipeline: Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> =
            Pipeline::open(&directory).unwrap();
        std::fs::remove_dir_all(&directory).unwrap();

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
        let directory = std::env::temp_dir().join(format!(
            "lithify-cli-copied-snapshot-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let infra = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql/infra.toml"),
        )
        .unwrap();
        std::fs::write(
            directory.join("infra.toml"),
            infra.replacen(
                "place = [{ bigquery.dataset = \"mysql_shop\" }]",
                "place = [{ bigquery.dataset = \"mysql_shop\" }, { bigquery = { project = \"analytics\", dataset = \"shop\" } }]",
                1,
            ),
        )
        .unwrap();
        std::fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql/table.toml"),
            directory.join("table.toml"),
        )
        .unwrap();

        let pipeline: Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> =
            Pipeline::open(&directory).unwrap();
        std::fs::remove_dir_all(&directory).unwrap();

        assert_eq!(
            DeclaredSnapshot::of(pipeline.sinks().next().unwrap())
                .unwrap()
                .artifact(),
            Snapshot::new("mysql_shop", "%Y%m%d")
                .copied_into(vec![Namespace::from("analytics.shop")])
        );
    }
}
