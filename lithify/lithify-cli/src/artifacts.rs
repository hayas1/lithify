use lithify_config::infra::Sink;
use lithify_config::ArtifactKind;
use lithify_strategy::snapshot::Snapshot;

use crate::pipeline::Warehouse;

pub trait Artifacts {
    fn snapshot(&self) -> Snapshot;
}

impl Artifacts for Sink<Warehouse> {
    fn snapshot(&self) -> Snapshot {
        let replay = self.replay();

        Snapshot::new(replay.namespace(ArtifactKind::Snapshot), replay.shard())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use lithify_config::Pipeline;
    use lithify_core::artifact::Artifact;
    use lithify_core::model::TableId;
    use lithify_core::position::RecordedAt;
    use lithify_core::sink::TablePath;
    use lithify_core::window::Window;

    use super::*;
    use crate::pipeline::Capture;

    #[test]
    fn a_sink_builds_its_snapshots_in_the_namespace_and_shards_the_pipeline_names() {
        let pipeline: Pipeline<Capture, Warehouse> = Pipeline::open(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql"),
        )
        .unwrap();
        let sink = pipeline.sinks().next().unwrap();
        let window = Window::new(
            RecordedAt::from(1789862400000),
            RecordedAt::from(1789948800000),
        );

        assert_eq!(
            sink.snapshot()
                .target(&TableId::new("shop", "customers"), &window)
                .unwrap(),
            TablePath::new("mysql_shop", "customers_20260920")
        );
    }
}
