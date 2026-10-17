#[cfg(feature = "serve")]
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use chrono::{NaiveDate, Utc};
use clap::{Parser, Subcommand};

use lithify_cli::course::Course;
use lithify_cli::credential::Credentials;
use lithify_cli::journal::Journal;
#[cfg(feature = "serve")]
use lithify_cli::pipeline::{Capture, WarehouseConfig};
use lithify_cli::plan::Tables;
use lithify_cli::run::Run;
use lithify_cli::unrunnable::Unrunnable;
use lithify_core::model::TableId;
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
        about = "Build the shard of a pipeline's snapshots that is named, onto the one the build log recorded before it"
    )]
    Build(BuildArguments),
    #[command(
        about = "List what should have been built by now but was not, and fail if anything is listed"
    )]
    Check(CheckArguments),
    #[cfg(feature = "serve")]
    #[command(
        about = "Serve the pages listing what was built, and assembling the builds to rebuild it with"
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
        value_name = "DATE",
        help = "The date the window of the shard to build opens on, folding onto the shard the build log recorded closing then"
    )]
    snapshot: NaiveDate,
    #[arg(
        long,
        value_name = "SCHEMA.TABLE",
        value_parser = |table: &str| TableId::try_from(table),
        help = "A table to build the shard of, out of every table the window holds when none is named"
    )]
    table: Vec<TableId>,
    #[arg(
        long,
        value_name = "DIRECTORY",
        help = "Write the statements the shard would run into this directory, one file per sink, instead of running them"
    )]
    dry_run: Option<PathBuf>,
}

impl BuildArguments {
    fn journal(self) -> Result<Journal, Unrunnable> {
        let course = Course::Shard {
            date: self.snapshot,
            tables: match self.table.is_empty() {
                true => Tables::Every,
                false => Tables::Named(self.table.into_iter().collect()),
            },
        };
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
        Board::new(Pipelines::<Capture, WarehouseConfig>::new(
            self.directory,
            self.credential.credentials(),
        ))
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
        match parsed(
            "build",
            &[&["--snapshot", "2026-09-20"][..], extra].concat(),
        )
        .unwrap()
        {
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
    fn a_catch_up_is_given_nothing_but_the_pipeline() {
        assert!(matches!(parsed("catch-up", &[]), Ok(Command::CatchUp(_))));
        assert!(parsed("catch-up", &["--from", "2026-09-20T00:00:00Z"]).is_err());
        assert!(parsed("catch-up", &["--onto", "2026-09-19T00:00:00Z"]).is_err());
        assert!(parsed("catch-up", &["--dry-run", "plans"]).is_err());
    }

    #[test]
    fn a_build_is_given_the_shard_it_builds_every_table_of() {
        let built = build_arguments(&[]);

        assert_eq!(built.snapshot, "2026-09-20".parse::<NaiveDate>().unwrap());
        assert_eq!(built.table, []);
        assert_eq!(built.dry_run, None);
    }

    #[test]
    fn a_build_is_narrowed_to_the_tables_it_names() {
        let narrowed = build_arguments(&["--table", "shop.people", "--table", "shop.pets"]);

        assert_eq!(
            narrowed.table,
            [TableId::new("shop", "people"), TableId::new("shop", "pets")]
        );
    }

    #[test]
    fn a_build_without_its_shard_is_rejected() {
        assert!(parsed("build", &[]).is_err());
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
    fn a_board_surveys_every_sink_rather_than_one_it_is_told_of() {
        assert!(parsed("serve", &["--sink", "rustfs"]).is_err());
    }

    #[cfg(feature = "serve")]
    #[test]
    fn a_build_the_board_assembles_is_one_the_command_line_takes() {
        let assembled =
            lithify_ui::command::BuildCommand::new("mysql", "2026-09-20".parse().unwrap())
                .sink("rustfs")
                .table(TableId::new("shop", "pets"))
                .dry_run("plans");

        let Command::Build(built) = Arguments::try_parse_from(
            assembled
                .args()
                .iter()
                .map(String::as_str)
                .chain(PIPELINE[1..].iter().copied()),
        )
        .unwrap()
        .command
        else {
            panic!("build was assembled");
        };

        assert_eq!(built.pipeline.directory, PathBuf::from("mysql"));
        assert_eq!(built.pipeline.sink.as_deref(), Some("rustfs"));
        assert_eq!(built.snapshot, "2026-09-20".parse::<NaiveDate>().unwrap());
        assert_eq!(built.table, [TableId::new("shop", "pets")]);
        assert_eq!(built.dry_run, Some(PathBuf::from("plans")));
    }
}
