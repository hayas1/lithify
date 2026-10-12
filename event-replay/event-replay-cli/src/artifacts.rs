use event_replay_config::ArtifactKind;
use event_replay_config::infra::Sink;
use event_replay_strategy::snapshot::Snapshot;

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

    use event_replay_config::Pipeline;
    use event_replay_core::artifact::Artifact;
    use event_replay_core::model::TableId;
    use event_replay_core::position::RecordedAt;
    use event_replay_core::sink::TablePath;
    use event_replay_core::window::Window;

    use super::*;
    use crate::pipeline::Capture;

    #[test]
    fn a_sink_builds_its_snapshots_in_the_namespace_and_shards_the_pipeline_names() {
        let pipeline: Pipeline<Capture, Warehouse> = Pipeline::open(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/event-replay/mysql"),
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
