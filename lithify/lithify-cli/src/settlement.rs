use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use ulid::Ulid;

use lithify_config::ArtifactKind;
use lithify_config::infra::{Sink, Source};
use lithify_core::artifact::ArtifactBuilder;
use lithify_core::build_log::{BuildLog, Outcome, Run, Status};
use lithify_core::sink::WarehouseClient;
use lithify_core::sql::SqlString;
use lithify_core::window::Window;

use crate::journal::Journal;
use crate::pipeline::{CaptureConfig, WarehouseConfig};
use crate::unrunnable::Unrunnable;

pub trait Settlement {
    type Output;

    fn settle<K, B>(&self, client: &K, builders: &[B]) -> Result<Self::Output, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
        B: ArtifactBuilder;

    fn report(
        &self,
        window: &Window,
        output: Self::Output,
        journal: &mut Journal,
    ) -> Result<Option<Vec<BuildLog>>, Unrunnable>;
}

pub struct DryRun {
    into: PathBuf,
    sink: String,
}

impl DryRun {
    pub fn new(into: &Path, sink: &str) -> Self {
        Self {
            into: into.to_owned(),
            sink: sink.to_owned(),
        }
    }
}

impl Settlement for DryRun {
    type Output = Vec<SqlString>;

    fn settle<K, B>(&self, client: &K, builders: &[B]) -> Result<Self::Output, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
        B: ArtifactBuilder,
    {
        Ok(builders
            .iter()
            .map(|builder| builder.create(client.warehouse()))
            .collect::<Result<Vec<_>, _>>()?
            .concat())
    }

    fn report(
        &self,
        window: &Window,
        output: Self::Output,
        journal: &mut Journal,
    ) -> Result<Option<Vec<BuildLog>>, Unrunnable> {
        let path = self.into.join(format!(
            "{}_{}.sql",
            self.sink,
            window.from().milliseconds_since_epoch()
        ));

        std::fs::create_dir_all(&self.into)?;
        std::fs::write(
            &path,
            output
                .iter()
                .map(|statement| format!("{statement};\n"))
                .collect::<Vec<_>>()
                .join("\n"),
        )?;
        journal.note(&format!(
            "wrote {} statements to {}",
            output.len(),
            path.display()
        ));

        Ok(None)
    }
}

pub struct Build {
    namespace: String,
    run: Run,
}

impl Build {
    pub fn new(
        sink: &Sink<WarehouseConfig>,
        source: &Source<CaptureConfig>,
        now: DateTime<Utc>,
    ) -> Result<Self, Unrunnable> {
        let artifacts = sink.artifacts();

        Ok(Self {
            namespace: artifacts.namespace(ArtifactKind::Snapshot).to_owned(),
            run: Run::new(
                Ulid::from_datetime(now.into()),
                now,
                &ArtifactKind::Snapshot.to_string(),
                &artifacts.schedule().to_string(),
                source.name(),
                &source.capture().to_json()?,
                env!("CARGO_PKG_VERSION"),
            ),
        })
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

    fn settle<K, B>(&self, client: &K, builders: &[B]) -> Result<Self::Output, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
        B: ArtifactBuilder,
    {
        client.create_build_log::<Unrunnable>(&self.namespace)?;

        builders
            .iter()
            .map(|builder| {
                let log = builder.build_log(&self.run, Self::outcome(client, builder));
                client.append_build_log(&self.namespace, &log)?;

                Ok(log)
            })
            .collect()
    }

    fn report(
        &self,
        _: &Window,
        output: Self::Output,
        journal: &mut Journal,
    ) -> Result<Option<Vec<BuildLog>>, Unrunnable> {
        for log in &output {
            journal.built(log);
        }

        Ok(output
            .iter()
            .all(|log| !log.status().failed())
            .then_some(output))
    }
}
