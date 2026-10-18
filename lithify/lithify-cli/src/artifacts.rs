use lithify_config::ArtifactKind;
use lithify_config::infra::Sink;
use lithify_strategy::snapshot::Snapshot;

use crate::pipeline::WarehouseConfig;

pub trait Artifacts {
    fn snapshot(&self) -> Snapshot;
}

impl Artifacts for Sink<WarehouseConfig> {
    fn snapshot(&self) -> Snapshot {
        let artifacts = self.artifacts();

        Snapshot::new(
            artifacts.namespace(ArtifactKind::Snapshot),
            artifacts.shard(),
        )
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
        let pipeline: Pipeline<CaptureConfig, WarehouseConfig> = Pipeline::open(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql"),
        )
        .unwrap();
        let sink = pipeline.sinks().next().unwrap();
        let window = Window::new(
            Ts::try_from(1789862400000).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        );

        assert_eq!(
            sink.snapshot()
                .target(&TableId::new("shop", "customers"), &window)
                .unwrap(),
            TablePath::new("mysql_shop", "customers_20260920")
        );
    }
}
