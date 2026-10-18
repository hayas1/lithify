use serde::Deserialize;

use lithify_artifact::snapshot::{self, Snapshot};
use lithify_config::Declaration;
use lithify_config::infra::Sink;
use lithify_core::artifact::Artifact;
use lithify_core::build_log::Series;
use lithify_core::schedule::Schedule;
use lithify_core::sink::Namespace;

use crate::pipeline::{LocationConfig, WarehouseConfig};
use crate::unrunnable::Unrunnable;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum ArtifactConfig {
    Snapshot(snapshot::Config<LocationConfig>),
}

impl ArtifactConfig {
    pub fn as_snapshot(&self) -> Option<&snapshot::Config<LocationConfig>> {
        match self {
            Self::Snapshot(config) => Some(config),
        }
    }
}

impl Declaration for ArtifactConfig {
    type Location = LocationConfig;

    fn source(&self) -> Option<&'static str> {
        match self {
            Self::Snapshot(_) => None,
        }
    }

    fn places(&self) -> impl Iterator<Item = &LocationConfig> {
        match self {
            Self::Snapshot(config) => config.place().iter(),
        }
    }
}

pub struct DeclaredSnapshot<'a> {
    config: &'a snapshot::Config<LocationConfig>,
    namespace: Namespace,
}

impl<'a> DeclaredSnapshot<'a> {
    pub const ARTIFACT: &'static str = Snapshot::KIND;

    pub fn of(sink: &'a Sink<WarehouseConfig, ArtifactConfig>) -> Result<Self, Unrunnable> {
        let undeclared = || Unrunnable::Undeclared {
            sink: sink.name().to_owned(),
            artifact: Self::ARTIFACT,
        };
        let config = sink
            .artifact(Self::ARTIFACT)
            .and_then(ArtifactConfig::as_snapshot)
            .ok_or_else(undeclared)?;
        let place = config.place().first().ok_or_else(undeclared)?;

        Ok(Self {
            config,
            namespace: place.clone().into(),
        })
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub fn schedule(&self) -> Schedule {
        self.config.schedule()
    }

    pub fn series(&self) -> Series {
        self.artifact().series()
    }

    pub fn artifact(&self) -> Snapshot {
        Snapshot::new(self.namespace.clone(), self.config.shard())
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
}
