use crate::pipeline::ConnectedPipeline;
use crate::run::Unverifiable;
use crate::scenario::Actions;

impl Actions {
    pub async fn run(&self, pipeline: &mut ConnectedPipeline<'_>) -> Result<(), Unverifiable> {
        for (name, action) in self.source() {
            pipeline.source(name)?.query(action).await?;
        }

        for (name, action) in self.sink() {
            pipeline.sink(name)?.query(action).await?;
        }

        Ok(())
    }
}
