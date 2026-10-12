use std::process::ExitCode;

use crate::pipeline::ConnectedPipeline;
use crate::row::Rows;
use crate::run::Unverifiable;
use crate::scenario::Assertion;

impl Assertion {
    pub fn run(
        &self,
        pipeline: &mut ConnectedPipeline,
        step: &str,
        report: &mut Report,
    ) -> Result<(), Unverifiable> {
        match self {
            Self::Snapshot(snapshot) => {
                let (source, sink, watch) = pipeline.ends(snapshot.source(), snapshot.sink())?;
                let model = sink.table_model(source, snapshot.table(), watch)?;
                let held = source.rows(&model)?;
                let built = sink.snapshot_rows(&model, watch)?;

                report.compared(
                    step,
                    &format!(
                        "the snapshot of {} in {}",
                        snapshot.table(),
                        snapshot.sink()
                    ),
                    snapshot.source(),
                    held,
                    built,
                );

                Ok(())
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct Report {
    lines: Vec<String>,
    failed: bool,
}

impl Report {
    pub fn print(&self) -> ExitCode {
        for line in &self.lines {
            println!("{line}");
        }

        match self.failed {
            true => ExitCode::FAILURE,
            false => ExitCode::SUCCESS,
        }
    }

    pub fn compared(&mut self, step: &str, target: &str, source: &str, held: Rows, built: Rows) {
        let missing = held.missing_from(&built);
        let extra = built.missing_from(&held);

        if missing.is_empty() && extra.is_empty() {
            self.lines.push(format!(
                "{step}: {target} holds what {source} holds in {} rows",
                held.count()
            ));

            return;
        }

        self.failed = true;
        self.lines
            .push(format!("{step}: {target} differs from {source}"));

        for row in missing {
            self.lines.push(format!("  only {source} holds {row}"));
        }

        for row in extra {
            self.lines.push(format!("  only {target} holds {row}"));
        }
    }
}
