use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use ulid::Ulid;

use lithify_config::infra::{Sink, Source};
use lithify_core::artifact::ArtifactBuilder;
use lithify_core::build_log::{BuildLog, Outcome, Run, Series, Status};
use lithify_core::sink::{Namespace, WarehouseClient};
use lithify_core::sql::SqlString;
use lithify_core::window::Window;

use crate::artifacts::{ArtifactConfig, DeclaredSnapshot};
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

    fn advancing(&self, namespace: &Namespace, series: &Series) -> Self;

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
    series: Option<Series>,
}

impl DryRun {
    pub fn new(into: &Path, sink: &str) -> Self {
        Self {
            into: into.to_owned(),
            sink: sink.to_owned(),
            series: None,
        }
    }

    fn named(&self, window: &Window) -> String {
        let opened = window.from().milliseconds_since_epoch();

        match &self.series {
            None => format!("{}_{opened}.sql", self.sink),
            Some(series) => format!(
                "{}_{}_{}_{opened}.sql",
                self.sink,
                series.artifact(),
                series.schedule().replace(':', "")
            ),
        }
    }
}

impl Settlement for DryRun {
    type Output = Vec<SqlString>;

    fn advancing(&self, _: &Namespace, series: &Series) -> Self {
        Self {
            into: self.into.clone(),
            sink: self.sink.clone(),
            series: Some(series.clone()),
        }
    }

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
        let path = self.into.join(self.named(window));

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
    namespace: Namespace,
    run: Run,
}

impl Build {
    pub fn new(
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        source: &Source<CaptureConfig>,
        now: DateTime<Utc>,
    ) -> Result<Self, Unrunnable> {
        let snapshot = DeclaredSnapshot::of(sink)?;

        Ok(Self {
            namespace: snapshot.namespace().clone(),
            run: Run::new(
                Ulid::from_datetime(now.into()),
                now,
                snapshot.series().artifact(),
                snapshot.series().schedule(),
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

    fn advancing(&self, namespace: &Namespace, series: &Series) -> Self {
        Self {
            namespace: namespace.clone(),
            run: self.run.advancing(series),
        }
    }

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
