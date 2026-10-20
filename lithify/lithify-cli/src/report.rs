use std::path::{Path, PathBuf};

use lithify_core::artifact::{ArtifactBuilder, UnbuildableArtifact};
use lithify_core::build_log::Series;
use lithify_core::runner::{Build, Rehearsal, Runner, Unsteppable};
use lithify_core::sink::{Namespace, WarehouseClient};
use lithify_core::sql::SqlString;
use lithify_core::window::Window;

use crate::journal::Journal;
use crate::unrunnable::Unrunnable;

pub trait Report: Runner {
    fn report(
        &self,
        series: &Series,
        window: &Window,
        output: Self::Output,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable>;
}

impl Report for Build {
    fn report(
        &self,
        _: &Series,
        _: &Window,
        output: Self::Output,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        for log in &output {
            journal.built(log);
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
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

    fn named(&self, series: &Series, window: &Window) -> String {
        format!(
            "{}_{}_{}_{}.sql",
            self.sink,
            series.artifact(),
            series.schedule().replace(':', ""),
            window.from().milliseconds_since_epoch()
        )
    }
}

impl Runner for DryRun {
    type Output = Vec<SqlString>;

    fn advancing(&self, _: &Namespace, _: &Series) -> Self {
        self.clone()
    }

    fn run<K, B, E>(&self, client: &K, window: &Window, builders: &[B]) -> Result<Self::Output, E>
    where
        K: WarehouseClient,
        B: ArtifactBuilder,
        E: From<K::Error> + From<UnbuildableArtifact> + From<Unsteppable>,
    {
        Rehearsal.run(client, window, builders)
    }
}

impl Report for DryRun {
    fn report(
        &self,
        series: &Series,
        window: &Window,
        output: Self::Output,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        let path = self.into.join(self.named(series, window));

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

        Ok(())
    }
}
