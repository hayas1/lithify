use std::path::{Path, PathBuf};

use lithify_core::artifact::UnbuildableArtifact;
use lithify_core::build_log::{BuildLog, Series, TableBuilt};
use lithify_core::runner::{Build, Runner};
use lithify_core::sink::{Namespace, WarehouseClient};
use lithify_core::window::Window;
use lithify_protocol::sink::bigquery::client::BigQueryExecution;
use lithify_protocol::sink::duckdb::client::DuckDbExecution;
use lithify_protocol::sink::execution::{Apply, DryRun};

use crate::journal::Journal;
use crate::unrunnable::Unrunnable;

pub trait Report: Runner {
    type Execution: DuckDbExecution + BigQueryExecution;

    fn execution(&self) -> Self::Execution;

    fn report(
        &self,
        series: &Series,
        window: &Window,
        output: Self::Output,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable>;
}

impl Report for Build {
    type Execution = Apply;

    fn execution(&self) -> Apply {
        Apply
    }

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

pub struct DryRunBuild {
    build: Build,
    dry_run: DryRun,
    into: PathBuf,
    sink: String,
}

impl DryRunBuild {
    pub fn new(build: Build, into: &Path, sink: &str) -> Self {
        Self {
            build,
            dry_run: DryRun::default(),
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

impl Runner for DryRunBuild {
    type Output = Vec<BuildLog>;

    fn advancing(&self, namespace: &Namespace, series: &Series) -> Self {
        Self {
            build: self.build.advancing(namespace, series),
            dry_run: self.dry_run.clone(),
            into: self.into.clone(),
            sink: self.sink.clone(),
        }
    }

    fn run<K, E>(&self, client: &K, built: Vec<TableBuilt>) -> Result<Self::Output, E>
    where
        K: WarehouseClient,
        E: From<K::Error> + From<UnbuildableArtifact>,
    {
        self.build.run(client, built)
    }
}

impl Report for DryRunBuild {
    type Execution = DryRun;

    fn execution(&self) -> DryRun {
        self.dry_run.clone()
    }

    fn report(
        &self,
        series: &Series,
        window: &Window,
        output: Self::Output,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        let statements = self.dry_run.take();
        let path = self.into.join(self.named(series, window));

        std::fs::create_dir_all(&self.into)?;
        std::fs::write(
            &path,
            statements
                .iter()
                .map(|statement| format!("{statement};\n"))
                .collect::<Vec<_>>()
                .join("\n"),
        )?;
        journal.note(&format!(
            "wrote {} statements to {}",
            statements.len(),
            path.display()
        ));

        self.build.report(series, window, output, journal)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use ulid::Ulid;

    use lithify_core::build_log::{Outcome, Run, Status};
    use lithify_core::model::TableId;
    use lithify_core::sink::TablePath;
    use lithify_core::sql::SqlString;
    use lithify_core::ts::Ts;

    use super::*;

    fn run() -> Run {
        Run::new(
            Ulid::nil(),
            DateTime::UNIX_EPOCH,
            "snapshot",
            "daily",
            "primary",
            "{}",
            "0.1.0",
        )
    }

    fn window() -> Window {
        Window::new(
            Ts::try_from(1789862400000).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        )
    }

    fn failed() -> BuildLog {
        BuildLog::new(
            &run(),
            Some(TableId::new("shop", "people")),
            Some(TableId::new("shop", "people")),
            None,
            Some(TablePath::new("mysql_shop", "people_20260920")),
            window(),
            Outcome::new(Status::Failed("no era".to_owned()), DateTime::UNIX_EPOCH),
        )
    }

    #[test]
    fn a_dry_run_writes_what_its_step_recorded_and_reports_a_table_that_failed_as_a_build_does() {
        let into = std::env::temp_dir().join(format!("lithify-dry-run-{}", Ulid::generate()));
        let dry_run = DryRunBuild::new(
            Build::new(Namespace::from("mysql_shop"), run(), || {
                DateTime::UNIX_EPOCH
            }),
            &into,
            "rustfs",
        );
        dry_run.execution().record(&SqlString::from("SELECT 1"));
        let mut journal = Journal::default();

        dry_run
            .report(
                &Series::new("snapshot", "daily"),
                &window(),
                vec![failed()],
                &mut journal,
            )
            .unwrap();
        let written =
            std::fs::read_to_string(into.join("rustfs_snapshot_daily_1789862400000.sql")).unwrap();
        std::fs::remove_dir_all(&into).unwrap();

        assert_eq!(written, "SELECT 1;\n");
        assert!(!journal.complete());
    }
}
