#[cfg(feature = "serve")]
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use chrono::{NaiveDate, NaiveTime, Utc};
use clap::{Parser, Subcommand};

#[cfg(feature = "serve")]
use lithify_cli::artifacts::ArtifactConfig;
use lithify_cli::artifacts::Declared;
use lithify_cli::command::{RunCommand, Target};
use lithify_cli::credential::Credentials;
use lithify_cli::journal::Journal;
#[cfg(feature = "serve")]
use lithify_cli::pipeline::{CaptureConfig, WarehouseConfig};
use lithify_cli::run::Run;
use lithify_cli::unrunnable::Unrunnable;
use lithify_protocol::sink::duckdb::store::{S3Credential, Secret};
#[cfg(feature = "serve")]
use lithify_ui::pipelines::Pipelines;
#[cfg(feature = "serve")]
use lithify_ui::server::Board;

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
        about = "Build the next window the build log leaves to build, or the one named, onto what the build log recorded before it"
    )]
    Build(BuildArguments),
    #[command(
        about = "List what should have been built by now but was not, and fail if anything is listed"
    )]
    Check(CheckArguments),
    #[cfg(feature = "serve")]
    #[command(
        about = "Serve the pages listing what was built and what the check finds wrong with it"
    )]
    Serve(ServeArguments),
}

impl Command {
    fn execute(self) -> ExitCode {
        let journal = match self {
            Self::CatchUp(arguments) => arguments.journal(),
            Self::Build(arguments) => arguments.journal(),
            Self::Check(arguments) => arguments.journal(),
            #[cfg(feature = "serve")]
            Self::Serve(arguments) => arguments.journal(),
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
struct CredentialArguments {
    #[arg(
        long,
        env = "S3_ACCESS_KEY_ID",
        hide_env_values = true,
        value_name = "KEY",
        requires = "secret_access_key",
        help = "The key the object storage holding the change log is read with"
    )]
    access_key_id: Option<String>,
    #[arg(
        long,
        env = "S3_SECRET_ACCESS_KEY",
        hide_env_values = true,
        value_name = "SECRET",
        requires = "access_key_id",
        help = "The secret the object storage holding the change log is read with"
    )]
    secret_access_key: Option<Secret>,
}

impl CredentialArguments {
    fn credentials(&self) -> Credentials {
        Credentials::new(
            self.access_key_id
                .clone()
                .zip(self.secret_access_key.clone())
                .map(|(key_id, secret)| S3Credential::new(key_id, secret)),
        )
    }
}

#[derive(clap::Args)]
struct PipelineArguments {
    #[arg(
        value_name = "DIRECTORY",
        help = "The directory holding the infra.toml and table.toml of one pipeline"
    )]
    directory: PathBuf,
    #[command(flatten)]
    credential: CredentialArguments,
    #[arg(
        long,
        value_name = "NAME",
        help = "The sink to read, out of the ones the pipeline is sunk into"
    )]
    sink: Option<String>,
}

impl PipelineArguments {
    fn run(&self) -> Run {
        Run::new(
            self.directory.clone(),
            self.credential.credentials(),
            self.sink.clone(),
            Utc::now(),
        )
    }
}

#[derive(clap::Args)]
struct TargetArguments {
    #[arg(
        long,
        value_name = "ARTIFACT",
        value_parser = |artifact: &str| Declared::KINDS.into_iter().find(|kind| *kind == artifact).ok_or("snapshot, instant, scd1 or scd2"),
        help = "The artifact to build, out of every one the sink places when none is named"
    )]
    artifact: Option<&'static str>,
    #[arg(
        long,
        value_name = "HH:MM",
        requires = "artifact",
        help = "The instant of the day the instant to build is cut at, out of every one the sink places when none is named"
    )]
    at: Option<NaiveTime>,
}

impl TargetArguments {
    fn target(&self) -> Target {
        Target::new(self.artifact, self.at)
    }
}

#[derive(clap::Args)]
struct CatchUpArguments {
    #[command(flatten)]
    pipeline: PipelineArguments,
    #[command(flatten)]
    target: TargetArguments,
    #[arg(
        long,
        value_name = "DATE",
        help = "The last day to build through, rather than the latest window that has closed"
    )]
    to: Option<NaiveDate>,
}

impl CatchUpArguments {
    fn journal(self) -> Result<Journal, Unrunnable> {
        self.pipeline.run().build(&RunCommand::CatchUp {
            target: self.target.target(),
            to: self.to,
        })
    }
}

#[derive(clap::Args)]
struct BuildArguments {
    #[command(flatten)]
    pipeline: PipelineArguments,
    #[command(flatten)]
    target: TargetArguments,
    #[arg(
        long,
        value_name = "DIRECTORY",
        help = "Write the statements the step would run into this directory, one file per sink, instead of running them"
    )]
    dry_run: Option<PathBuf>,
}

impl BuildArguments {
    fn journal(self) -> Result<Journal, Unrunnable> {
        let command = RunCommand::Build(self.target.target());
        let run = self.pipeline.run();

        match self.dry_run {
            Some(into) => run.dry_run(&command, &into),
            None => run.build(&command),
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

#[cfg(feature = "serve")]
#[derive(clap::Args)]
struct ServeArguments {
    #[arg(
        value_name = "DIRECTORY",
        help = "The directory of one pipeline, or the directory holding the directories of several"
    )]
    directory: PathBuf,
    #[command(flatten)]
    credential: CredentialArguments,
    #[arg(
        long,
        value_name = "ADDRESS",
        default_value = "127.0.0.1:3000",
        help = "The address the pages are served on"
    )]
    address: SocketAddr,
}

#[cfg(feature = "serve")]
impl ServeArguments {
    fn journal(self) -> Result<Journal, Unrunnable> {
        Board::new(
            Pipelines::<CaptureConfig, WarehouseConfig, ArtifactConfig>::new(
                self.directory,
                self.credential.credentials(),
            ),
        )
        .serve(self.address)?;

        Ok(Journal::default())
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
        match parsed("build", extra).unwrap() {
            Command::Build(arguments) => arguments,
            _ => unreachable!("build was named"),
        }
    }

    #[test]
    fn a_pipeline_is_read_without_a_credential_unless_one_is_given_whole() {
        assert!(Arguments::try_parse_from(["lithify", "catch-up", "mysql"]).is_ok());
        assert!(
            Arguments::try_parse_from([
                "lithify",
                "catch-up",
                "mysql",
                "--access-key-id",
                "rustfsadmin"
            ])
            .is_err()
        );
    }

    #[test]
    fn a_catch_up_is_given_the_pipeline_and_the_date_of_the_last_shard_it_builds() {
        let Ok(Command::CatchUp(latest)) = parsed("catch-up", &[]) else {
            panic!("catch-up was named");
        };
        let Ok(Command::CatchUp(past)) = parsed("catch-up", &["--to", "2026-09-20"]) else {
            panic!("catch-up was named");
        };

        assert_eq!(latest.to, None);
        assert_eq!(latest.target.artifact, None);
        assert_eq!(past.to, Some("2026-09-20".parse::<NaiveDate>().unwrap()));
        assert!(parsed("catch-up", &["--from", "2026-09-20T00:00:00Z"]).is_err());
        assert!(parsed("catch-up", &["--onto", "2026-09-19T00:00:00Z"]).is_err());
        assert!(parsed("catch-up", &["--dry-run", "dry-run"]).is_err());
        let Ok(Command::CatchUp(cut)) =
            parsed("catch-up", &["--artifact", "instant", "--at", "06:00"])
        else {
            panic!("catch-up was named");
        };
        assert_eq!(cut.target.artifact, Some("instant"));
        assert_eq!(cut.target.at, NaiveTime::from_hms_opt(6, 0, 0));
    }

    #[test]
    fn a_build_named_nothing_takes_the_step_the_build_log_says_is_next() {
        let Ok(Command::Build(next)) = parsed("build", &[]) else {
            panic!("build was named");
        };

        assert_eq!(next.target.artifact, None);
        assert_eq!(next.dry_run, None);
        assert!(parsed("build", &["--date", "2026-09-20"]).is_err());
    }

    #[test]
    fn a_build_is_named_the_artifact_and_the_instant_it_is_cut_at() {
        let cut = build_arguments(&["--artifact", "instant", "--at", "12:00"]);

        assert_eq!(cut.target.artifact, Some("instant"));
        assert_eq!(cut.target.at, NaiveTime::from_hms_opt(12, 0, 0));
        assert!(parsed("build", &["--artifact", "masked"]).is_err());
        assert!(parsed("build", &["--at", "12:00"]).is_err());
    }

    #[test]
    fn a_dry_run_names_the_directory_the_statements_are_written_into() {
        let dry_run = build_arguments(&["--dry-run", "dry-run"]);

        assert_eq!(dry_run.dry_run, Some(PathBuf::from("dry-run")));
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

    #[cfg(feature = "serve")]
    #[test]
    fn a_board_is_served_on_the_loopback_unless_told_otherwise() {
        let Ok(Command::Serve(served)) = parsed("serve", &[]) else {
            panic!("serve was named");
        };
        let Ok(Command::Serve(addressed)) = parsed("serve", &["--address", "0.0.0.0:8080"]) else {
            panic!("serve was named");
        };

        assert_eq!(served.address, "127.0.0.1:3000".parse().unwrap());
        assert_eq!(addressed.address, "0.0.0.0:8080".parse().unwrap());
    }

    #[cfg(feature = "serve")]
    #[test]
    fn a_board_reads_every_sink_rather_than_one_it_is_told_of() {
        assert!(parsed("serve", &["--sink", "rustfs"]).is_err());
    }
}
