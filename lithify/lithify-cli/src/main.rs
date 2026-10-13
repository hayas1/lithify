use std::path::PathBuf;
use std::process::ExitCode;

use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};

use lithify_cli::course::Course;
use lithify_cli::credential::{Credential, Secret};
use lithify_cli::journal::Journal;
use lithify_cli::run::Run;
use lithify_cli::unrunnable::Unrunnable;
use lithify_core::position::RecordedAt;
use lithify_core::window::Window;

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
        about = "Build every window of a pipeline's change log that closed since the build log says it was last built"
    )]
    CatchUp(CatchUpArguments),
    #[command(
        about = "Build the window of a pipeline's change log that is named, whatever the build log says"
    )]
    Build(BuildArguments),
    #[command(
        about = "List what should have been built by now but was not, and fail if anything is listed"
    )]
    Check(CheckArguments),
}

impl Command {
    fn execute(self) -> ExitCode {
        let journal = match self {
            Self::CatchUp(arguments) => arguments.journal(),
            Self::Build(arguments) => arguments.journal(),
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
    fn run(self) -> Run {
        Run::new(
            self.directory,
            Credential::new(self.access_key_id, self.secret_access_key),
            self.sink,
            Utc::now(),
        )
    }
}

#[derive(clap::Args)]
struct CatchUpArguments {
    #[command(flatten)]
    pipeline: PipelineArguments,
}

impl CatchUpArguments {
    fn journal(self) -> Result<Journal, Unrunnable> {
        self.pipeline.run().build(&Course::CatchUp)
    }
}

#[derive(clap::Args)]
struct BuildArguments {
    #[command(flatten)]
    pipeline: PipelineArguments,
    #[arg(
        long,
        value_name = "INSTANT",
        help = "The instant the window to build opens"
    )]
    from: DateTime<Utc>,
    #[arg(
        long,
        value_name = "INSTANT",
        help = "The instant the window to build closes"
    )]
    to: DateTime<Utc>,
    #[arg(
        long,
        value_name = "INSTANT",
        help = "The instant the window before the one to build opened, naming the snapshots to fold onto instead of building from nothing"
    )]
    onto: Option<DateTime<Utc>>,
    #[arg(
        long,
        value_name = "DIRECTORY",
        help = "Write the statements the window would run into this directory, one file per sink, instead of running them"
    )]
    dry_run: Option<PathBuf>,
}

impl BuildArguments {
    fn journal(self) -> Result<Journal, Unrunnable> {
        let course = Course::window(
            Window::new(RecordedAt::from(self.from), RecordedAt::from(self.to)),
            self.onto.map(RecordedAt::from),
        );
        let run = self.pipeline.run();

        match self.dry_run {
            Some(into) => run.plan(&course, &into),
            None => run.build(&course),
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
        self.pipeline.run().check()
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

    const PIPELINE: [&str; 5] = [
        "mysql",
        "--access-key-id",
        "rustfsadmin",
        "--secret-access-key",
        "rustfsadmin",
    ];

    fn parsed(subcommand: &str, extra: &[&str]) -> Result<Command, clap::Error> {
        Arguments::try_parse_from(["lithify", subcommand].iter().chain(&PIPELINE).chain(extra))
            .map(|arguments| arguments.command)
    }

    fn build_arguments(extra: &[&str]) -> BuildArguments {
        let window = [
            "--from",
            "2026-09-20T00:00:00Z",
            "--to",
            "2026-09-21T00:00:00Z",
        ];

        match parsed("build", &[&window[..], extra].concat()).unwrap() {
            Command::Build(arguments) => arguments,
            _ => unreachable!("build was named"),
        }
    }

    fn instant(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339).unwrap().to_utc()
    }

    #[test]
    fn a_catch_up_is_given_nothing_but_the_pipeline() {
        assert!(matches!(parsed("catch-up", &[]), Ok(Command::CatchUp(_))));
        assert!(parsed("catch-up", &["--from", "2026-09-20T00:00:00Z"]).is_err());
        assert!(parsed("catch-up", &["--onto", "2026-09-19T00:00:00Z"]).is_err());
        assert!(parsed("catch-up", &["--dry-run", "plans"]).is_err());
    }

    #[test]
    fn a_build_is_given_the_window_it_builds() {
        let built = build_arguments(&[]);

        assert_eq!(built.from, instant("2026-09-20T00:00:00Z"));
        assert_eq!(built.to, instant("2026-09-21T00:00:00Z"));
        assert_eq!(built.onto, None);
        assert_eq!(built.dry_run, None);
    }

    #[test]
    fn a_build_without_the_close_of_its_window_is_rejected() {
        assert!(parsed("build", &["--from", "2026-09-20T00:00:00Z"]).is_err());
    }

    #[test]
    fn a_build_folds_onto_the_snapshot_of_the_window_it_names() {
        let folded = build_arguments(&["--onto", "2026-09-19T00:00:00Z"]);

        assert_eq!(folded.onto, Some(instant("2026-09-19T00:00:00Z")));
    }

    #[test]
    fn a_dry_run_names_the_directory_the_plans_are_written_into() {
        let planned = build_arguments(&["--dry-run", "plans"]);

        assert_eq!(planned.dry_run, Some(PathBuf::from("plans")));
    }

    #[test]
    fn a_check_reads_the_pipeline_a_catch_up_would() {
        let Ok(Command::Check(arguments)) = parsed("check", &["--sink", "rustfs"]) else {
            panic!("check was named");
        };

        assert_eq!(arguments.pipeline.directory, PathBuf::from("mysql"));
        assert_eq!(arguments.pipeline.sink.as_deref(), Some("rustfs"));
    }

    #[test]
    fn a_pipeline_given_without_naming_what_to_do_with_it_is_rejected() {
        let unnamed = Arguments::try_parse_from(["lithify"].iter().chain(&PIPELINE));

        assert!(unnamed.is_err());
    }
}
