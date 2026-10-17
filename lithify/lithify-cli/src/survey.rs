use std::path::PathBuf;

use chrono::Utc;

use lithify_config::Pipeline;
use lithify_ui::overview::PipelineOverview;
use lithify_ui::server::Survey;

use crate::credential::Credential;
use crate::pipeline::{Capture, Warehouse};
use crate::run::Run;
use crate::unrunnable::Unrunnable;

pub struct Pipelines {
    root: PathBuf,
    credential: Credential,
}

impl Pipelines {
    pub fn new(root: PathBuf, credential: Credential) -> Self {
        Self { root, credential }
    }

    fn runs(&self) -> Result<Vec<Run>, String> {
        let directories = Pipeline::<Capture, Warehouse>::directories(&self.root)
            .map_err(|unreadable| unreadable.to_string())?;

        if directories.is_empty() {
            return Err(format!("{} configures no pipeline", self.root.display()));
        }

        Ok(directories
            .into_iter()
            .map(|directory| Run::new(directory, self.credential.clone(), None, Utc::now()))
            .collect())
    }
}

impl Survey for Pipelines {
    fn pipelines(&self) -> Result<Vec<String>, String> {
        Ok(self
            .runs()?
            .iter()
            .map(|run| run.directory().display().to_string())
            .collect())
    }

    fn sinks(&self) -> Result<Vec<String>, String> {
        let mut sinks = Vec::new();

        for run in self.runs()? {
            for sink in run
                .sinks()
                .map_err(|unrunnable: Unrunnable| unrunnable.to_string())?
            {
                if !sinks.contains(&sink) {
                    sinks.push(sink);
                }
            }
        }

        Ok(sinks)
    }

    fn inspect(&self) -> Result<Vec<PipelineOverview>, String> {
        Ok(self.runs()?.iter().map(Run::overview).collect())
    }
}
