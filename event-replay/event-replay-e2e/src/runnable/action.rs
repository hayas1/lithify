use crate::pipeline::ConnectedPipeline;
use crate::run::Unverifiable;
use crate::scenario::Actions;

impl Actions {
    pub fn run(&self, pipeline: &mut ConnectedPipeline) -> Result<(), Unverifiable> {
        for (name, action) in self.source() {
            pipeline.source(name)?.query(action)?;
        }

        for (name, action) in self.sink() {
            pipeline.sink(name)?.query(action)?;
        }

        Ok(())
    }
}
