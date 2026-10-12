use chrono::Utc;

use lithify_config::infra::{Sink, Source};
use lithify_config::{ArtifactKind, Schedule};
use lithify_core::artifact::ArtifactBuilder;
use lithify_core::build_log::{BuildLog, Outcome, Run, Status};
use lithify_core::sink::WarehouseClient;
use lithify_core::sql::SqlString;

use crate::pipeline::{Capture, Warehouse};
use crate::unrunnable::Unrunnable;

pub trait Settlement {
    type Output;

    fn settle<K, B>(self, client: &K, builders: &[B]) -> Result<Self::Output, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
        B: ArtifactBuilder;
}

pub struct DryRun;

impl Settlement for DryRun {
    type Output = Vec<SqlString>;

    fn settle<K, B>(self, _: &K, builders: &[B]) -> Result<Self::Output, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
        B: ArtifactBuilder,
    {
        Ok(builders
            .iter()
            .map(ArtifactBuilder::create)
            .collect::<Result<Vec<_>, _>>()?
            .concat())
    }
}

pub struct Build {
    namespace: String,
    schedule: Schedule,
    source: String,
    capture: String,
}

impl Build {
    pub fn new(sink: &Sink<Warehouse>, source: &Source<Capture>) -> Result<Self, Unrunnable> {
        let artifacts = sink.artifacts();

        Ok(Self {
            namespace: artifacts.namespace(ArtifactKind::Snapshot).to_owned(),
            schedule: artifacts.schedule(),
            source: source.name().to_owned(),
            capture: serde_json::to_string(source.capture())?,
        })
    }

    fn run<K: WarehouseClient>(&self, client: &K) -> Result<Run, K::Error> {
        Ok(Run::new(
            client.next_run_id(&self.namespace)?,
            &ArtifactKind::Snapshot.to_string(),
            &self.schedule.to_string(),
            &self.source,
            &self.capture,
            env!("CARGO_PKG_VERSION"),
        ))
    }

    fn outcome<K>(client: &K, builder: &impl ArtifactBuilder) -> Outcome
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        let status = match client.build::<Unrunnable>(builder) {
            Ok(()) => Status::Succeeded,
            Err(unbuilt) => Status::Failed(unbuilt.to_string()),
        };

        Outcome::new(status, Utc::now())
    }
}

impl Settlement for Build {
    type Output = Vec<BuildLog>;

    fn settle<K, B>(self, client: &K, builders: &[B]) -> Result<Self::Output, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
        B: ArtifactBuilder,
    {
        client.create_build_log::<Unrunnable>(&self.namespace)?;
        let run = self.run(client)?;

        builders
            .iter()
            .map(|builder| {
                let log = builder.build_log(&run, Self::outcome(client, builder));
                client.append_build_log(&log)?;

                Ok(log)
            })
            .collect()
    }
}
