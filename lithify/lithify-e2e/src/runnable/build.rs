use crate::pipeline::ConnectedPipeline;
use crate::run::Unverifiable;
use crate::scenario::Build;

impl Build {
    pub fn run(&self, pipeline: &mut ConnectedPipeline) -> Result<(), Unverifiable> {
        match self {
            Self::Snapshot(snapshot) => {
                let (source, sink, watch) = pipeline.ends(snapshot.source(), snapshot.sink())?;

                let model = sink.table_model(source, snapshot.table(), watch)?;

                sink.build_snapshot(source, &model, watch)
            }
        }
    }
}
