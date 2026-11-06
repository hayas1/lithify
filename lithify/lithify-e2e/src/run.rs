use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};
use std::time::Duration;

use lithify_cli::artifacts::ArtifactConfig;
use lithify_cli::credential::Credentials;
use lithify_cli::pipeline::{CaptureConfig, WarehouseConfig};
use lithify_cli::unrunnable::Unrunnable;
use lithify_pipeline::Pipeline;
use lithify_schema::ts::{Replay, Ts};

use crate::pipeline::{ConnectedPipeline, Watch};
use crate::runnable::Report;
use crate::scenario::{Scenario, Unwritable};
use crate::sink::Unbuildable;
use crate::source::Unreachable;

pub struct Run {
    scenario: Scenario,
    directory: PathBuf,
    credentials: Credentials,
    patience: Duration,
}

impl Run {
    pub fn new(
        scenario: Scenario,
        directory: &Path,
        credentials: Credentials,
        patience: u64,
    ) -> Self {
        Self {
            scenario,
            directory: directory.to_owned(),
            credentials,
            patience: Duration::from_secs(patience),
        }
    }

    pub async fn report(&self) -> Result<Report, Unverifiable> {
        let config: Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> =
            Pipeline::open(self.directory.join(self.scenario.pipeline()))?;
        let watch = Watch::new(Ts::<Replay>::now().as_event(), self.patience);
        let mut pipeline =
            ConnectedPipeline::connect(&self.scenario, &config, watch, &self.credentials).await?;

        self.scenario.setup().run(&mut pipeline).await?;

        let asserted = self.steps(&mut pipeline).await;
        let cleaned = self.scenario.cleanup().run(&mut pipeline).await;

        let report = asserted?;
        cleaned?;

        Ok(report)
    }

    async fn steps(&self, pipeline: &mut ConnectedPipeline<'_>) -> Result<Report, Unverifiable> {
        let mut report = Report::default();

        for step in self.scenario.steps() {
            step.actions().run(pipeline).await?;

            for build in step.builds() {
                build.run(pipeline).await?;
            }

            for assertion in step.assertions() {
                assertion.run(pipeline, step.name(), &mut report).await?;
            }
        }

        Ok(report)
    }
}

#[derive(Debug)]
pub enum Unverifiable {
    Missing(std::io::Error),
    Unreadable(lithify_pipeline::Unreadable),
    Unreachable(Unreachable),
    Unbuildable(Unbuildable),
    Unrunnable(Unrunnable),
    Unwritable(Unwritable),
    UnknownSource(String),
    UnknownSink(String),
    Unlanded(String),
}

impl Display for Unverifiable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing(cause) => write!(formatter, "{cause}"),
            Self::Unreadable(cause) => write!(formatter, "{cause}"),
            Self::Unreachable(cause) => write!(formatter, "{cause}"),
            Self::Unbuildable(cause) => write!(formatter, "{cause}"),
            Self::Unrunnable(cause) => write!(formatter, "{cause}"),
            Self::Unwritable(cause) => write!(formatter, "{cause}"),
            Self::UnknownSource(name) => write!(formatter, "the pipeline captures no {name}"),
            Self::UnknownSink(name) => write!(formatter, "the pipeline is not sunk into {name}"),
            Self::Unlanded(table) => write!(formatter, "{table} did not land what was applied"),
        }
    }
}

impl std::error::Error for Unverifiable {}

impl From<lithify_pipeline::Unreadable> for Unverifiable {
    fn from(cause: lithify_pipeline::Unreadable) -> Self {
        Self::Unreadable(cause)
    }
}

impl From<Unreachable> for Unverifiable {
    fn from(cause: Unreachable) -> Self {
        Self::Unreachable(cause)
    }
}

impl From<Unbuildable> for Unverifiable {
    fn from(cause: Unbuildable) -> Self {
        Self::Unbuildable(cause)
    }
}

impl From<Unrunnable> for Unverifiable {
    fn from(cause: Unrunnable) -> Self {
        Self::Unrunnable(cause)
    }
}

impl From<Unwritable> for Unverifiable {
    fn from(cause: Unwritable) -> Self {
        Self::Unwritable(cause)
    }
}
