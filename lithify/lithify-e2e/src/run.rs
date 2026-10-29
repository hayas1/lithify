use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};
use std::time::Duration;

use lithify_cli::artifacts::ArtifactConfig;
use lithify_cli::pipeline::{CaptureConfig, WarehouseConfig};
use lithify_cli::unrunnable::Unrunnable;
use lithify_core::ts::{Replay, Ts};
use lithify_pipeline::Pipeline;

use crate::pipeline::{ConnectedPipeline, Watch};
use crate::runnable::Report;
use crate::scenario::{Scenario, Unwritable};
use crate::sink::Unbuildable;
use crate::source::Unreachable;

pub struct Run {
    scenario: Scenario,
    directory: PathBuf,
    key_id: String,
    secret: String,
    patience: Duration,
}

impl Run {
    pub fn new(
        scenario: Scenario,
        directory: &Path,
        key_id: &str,
        secret: &str,
        patience: u64,
    ) -> Self {
        Self {
            scenario,
            directory: directory.to_owned(),
            key_id: key_id.to_owned(),
            secret: secret.to_owned(),
            patience: Duration::from_secs(patience),
        }
    }

    pub fn report(&self) -> Result<Report, Unverifiable> {
        let config: Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> =
            Pipeline::open(self.directory.join(self.scenario.pipeline()))?;
        let watch = Watch::new(Ts::<Replay>::now().as_event(), self.patience);
        let mut pipeline =
            ConnectedPipeline::connect(&self.scenario, &config, watch, &self.key_id, &self.secret)?;

        self.scenario.setup().run(&mut pipeline)?;

        let asserted = self.steps(&mut pipeline);
        let cleaned = self.scenario.cleanup().run(&mut pipeline);

        let report = asserted?;
        cleaned?;

        Ok(report)
    }

    fn steps(&self, pipeline: &mut ConnectedPipeline) -> Result<Report, Unverifiable> {
        let mut report = Report::default();

        for step in self.scenario.steps() {
            step.actions().run(pipeline)?;

            for build in step.builds() {
                build.run(pipeline)?;
            }

            for assertion in step.assertions() {
                assertion.run(pipeline, step.name(), &mut report)?;
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
    WithoutTopicPrefix,
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
            Self::WithoutTopicPrefix => {
                write!(
                    formatter,
                    "the source names no topic prefix to read its change logs by"
                )
            }
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
