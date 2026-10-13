use std::path::PathBuf;
use std::process::ExitCode;

use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};

use lithify_cli::credential::{Credential, Secret};
use lithify_cli::journal::Journal;
use lithify_cli::run::Run;
use lithify_cli::unrunnable::Unrunnable;

#[derive(Parser)]
#[command(
    name = "lithify",
    about = "Settle the change log of a pipeline into the artifacts of its warehouses"
)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(
        about = "Build every window of a pipeline's change log that closed since it was last built"
    )]
    Run(RunArguments),
    #[command(
        about = "List what should have been built by now but was not, and fail if anything is listed"
    )]
    Check(CheckArguments),
}

impl Command {
    fn execute(self) -> ExitCode {
        let journal = match self {
            Self::Run(arguments) => arguments.journal(),
            Self::Check(arguments) => arguments.journal(),
        };

        match journal {
            Ok(journal) => {
                for entry in journal.entries() {
                    println!("{entry}");
                }

                match journal.complete() {
                    true => ExitCode::SUCCESS,
                    false => ExitCode::FAILURE,
                }
            }
            Err(unrunnable) => {
                eprintln!("{unrunnable}");

                ExitCode::FAILURE
            }
        }
    }
}

#[derive(clap::Args)]
struct PipelineArguments {
    #[arg(
        value_name = "DIRECTORY",
        help = "The directory holding the infra.toml and table.toml of one pipeline"
    )]
    directory: PathBuf,
    #[arg(
        long,
        env = "S3_ACCESS_KEY_ID",
        hide_env_values = true,
        value_name = "KEY",
        help = "The key the object storage holding the change log is read with"
    )]
    access_key_id: String,
    #[arg(
        long,
        env = "S3_SECRET_ACCESS_KEY",
        hide_env_values = true,
        value_name = "SECRET",
        help = "The secret the object storage holding the change log is read with"
    )]
    secret_access_key: Secret,
    #[arg(
        long,
        value_name = "NAME",
        help = "The sink to read, out of the ones the pipeline is sunk into"
    )]
    sink: Option<String>,
}

impl PipelineArguments {
    fn run(self, onto: Option<DateTime<Utc>>) -> Run {
        Run::new(
            self.directory,
            Credential::new(self.access_key_id, self.secret_access_key),
            self.sink,
            onto,
            Utc::now(),
        )
    }
}

#[derive(clap::Args)]
struct RunArguments {
    #[command(flatten)]
    pipeline: PipelineArguments,
    #[arg(
        long,
        value_name = "DIRECTORY",
        help = "Write the statements each window would run into this directory, one file per sink and window, instead of running them"
    )]
    dry_run: Option<PathBuf>,
    #[arg(
        long,
        value_name = "INSTANT",
        help = "The instant the window before the first one to build opened, naming the snapshots to fold onto instead of the ones the build log recorded"
    )]
    onto: Option<DateTime<Utc>>,
}

impl RunArguments {
    fn journal(self) -> Result<Journal, Unrunnable> {
        let run = self.pipeline.run(self.onto);

        match self.dry_run {
            Some(into) => run.plan(&into),
            None => run.build(),
        }
    }
}

#[derive(clap::Args)]
struct CheckArguments {
    #[command(flatten)]
    pipeline: PipelineArguments,
}

impl CheckArguments {
    fn journal(self) -> Result<Journal, Unrunnable> {
        self.pipeline.run(None).check()
    }
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    Arguments::parse().command.execute()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_arguments(extra: &[&str]) -> RunArguments {
        let required = [
            "lithify",
            "run",
            "mysql",
            "--access-key-id",
            "rustfsadmin",
            "--secret-access-key",
            "rustfsadmin",
        ];

        match Arguments::parse_from(required.iter().chain(extra)).command {
            Command::Run(arguments) => arguments,
            Command::Check(_) => unreachable!("run was named"),
        }
    }

    #[test]
    fn a_dry_run_names_the_directory_the_plans_are_written_into() {
        let planned = run_arguments(&["--dry-run", "plans"]);

        assert_eq!(planned.dry_run, Some(PathBuf::from("plans")));
        assert_eq!(run_arguments(&[]).dry_run, None);
    }

    #[test]
    fn a_run_folds_onto_the_snapshot_of_the_window_it_names() {
        let folded = run_arguments(&["--onto", "2026-09-19T00:00:00Z"]);

        assert_eq!(
            folded.onto,
            Some(
                DateTime::parse_from_rfc3339("2026-09-19T00:00:00Z")
                    .unwrap()
                    .to_utc()
            )
        );
        assert_eq!(run_arguments(&[]).onto, None);
    }

    #[test]
    fn a_run_is_given_no_window_to_build() {
        let windowed = Arguments::try_parse_from([
            "lithify",
            "run",
            "mysql",
            "--from",
            "2026-09-20T00:00:00Z",
            "--access-key-id",
            "rustfsadmin",
            "--secret-access-key",
            "rustfsadmin",
        ]);

        assert!(windowed.is_err());
    }

    #[test]
    fn a_check_reads_the_pipeline_a_run_would() {
        let checked = Arguments::parse_from([
            "lithify",
            "check",
            "mysql",
            "--access-key-id",
            "rustfsadmin",
            "--secret-access-key",
            "rustfsadmin",
            "--sink",
            "rustfs",
        ]);

        let Command::Check(arguments) = checked.command else {
            panic!("check was named");
        };
        assert_eq!(arguments.pipeline.directory, PathBuf::from("mysql"));
        assert_eq!(arguments.pipeline.sink.as_deref(), Some("rustfs"));
    }

    #[test]
    fn a_pipeline_given_without_naming_what_to_do_with_it_is_rejected() {
        let unnamed = Arguments::try_parse_from([
            "lithify",
            "mysql",
            "--access-key-id",
            "rustfsadmin",
            "--secret-access-key",
            "rustfsadmin",
        ]);

        assert!(unnamed.is_err());
    }
}
