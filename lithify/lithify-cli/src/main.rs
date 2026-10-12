use std::path::PathBuf;
use std::process::ExitCode;

use chrono::{DateTime, Utc};
use clap::Parser;

use lithify_core::position::RecordedAt;
use lithify_core::window::Window;

use lithify_cli::credential::{Credential, Secret};
use lithify_cli::journal::Journal;
use lithify_cli::run::Run;
use lithify_cli::unreplayable::Unreplayable;

#[derive(Parser)]
#[command(
    name = "lithify",
    about = "Report the schema changes a window of a pipeline's change log holds, or plan the snapshots it would build"
)]
struct Arguments {
    #[arg(
        value_name = "DIRECTORY",
        help = "The directory holding the infra.toml and table.toml of one pipeline"
    )]
    directory: PathBuf,
    #[arg(
        long,
        value_name = "INSTANT",
        help = "The instant the window opens, as RFC 3339"
    )]
    from: DateTime<Utc>,
    #[arg(
        long,
        value_name = "INSTANT",
        help = "The instant the window closes, as RFC 3339"
    )]
    to: DateTime<Utc>,
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
    #[arg(
        long,
        value_name = "DIRECTORY",
        help = "Write the statements a snapshot of each table would run into this directory, one file per sink, instead of reporting the schema changes"
    )]
    dry_run: Option<PathBuf>,
    // TODO: read the window to fold onto from a history of the runs that built snapshots, once
    // the command builds them instead of only planning them.
    #[arg(
        long,
        value_name = "INSTANT",
        help = "The instant the window before this one opened, naming the snapshots to fold onto; without it the window folds onto what the capture read"
    )]
    onto: Option<DateTime<Utc>>,
}

impl Arguments {
    fn window(&self) -> Window {
        Window::new(RecordedAt::from(self.from), RecordedAt::from(self.to))
    }

    fn journal(mut self) -> Result<Journal, Unreplayable> {
        let dry_run = self.dry_run.take();
        let run = Run::from(self);

        match dry_run {
            Some(into) => run.plan(&into),
            None => run.report(),
        }
    }
}

impl From<Arguments> for Run {
    fn from(arguments: Arguments) -> Self {
        let window = arguments.window();

        Self::new(
            arguments.directory,
            window,
            Credential::new(arguments.access_key_id, arguments.secret_access_key),
            arguments.sink,
            arguments.onto,
        )
    }
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let arguments = Arguments::parse();

    match arguments.journal() {
        Ok(journal) => {
            for entry in journal.entries() {
                println!("{entry}");
            }

            match journal.complete() {
                true => ExitCode::SUCCESS,
                false => ExitCode::FAILURE,
            }
        }
        Err(unreplayable) => {
            eprintln!("{unreplayable}");

            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(from: &str, to: &str) -> Arguments {
        Arguments::parse_from([
            "lithify",
            "mysql",
            "--from",
            from,
            "--to",
            to,
            "--access-key-id",
            "minioadmin",
            "--secret-access-key",
            "minioadmin",
        ])
    }

    #[test]
    fn a_dry_run_names_the_directory_the_plans_are_written_into() {
        let planned = Arguments::parse_from([
            "lithify",
            "mysql",
            "--from",
            "2026-09-20T00:00:00Z",
            "--to",
            "2026-09-21T00:00:00Z",
            "--access-key-id",
            "minioadmin",
            "--secret-access-key",
            "minioadmin",
            "--dry-run",
            "plans",
        ]);

        assert_eq!(planned.dry_run, Some(PathBuf::from("plans")));
        assert_eq!(
            arguments("2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z").dry_run,
            None
        );
    }

    #[test]
    fn a_run_folds_onto_the_snapshot_of_the_window_it_names() {
        let folded = Arguments::parse_from([
            "lithify",
            "mysql",
            "--from",
            "2026-09-20T00:00:00Z",
            "--to",
            "2026-09-21T00:00:00Z",
            "--access-key-id",
            "minioadmin",
            "--secret-access-key",
            "minioadmin",
            "--onto",
            "2026-09-19T00:00:00Z",
        ]);

        assert_eq!(
            folded.onto,
            Some(
                DateTime::parse_from_rfc3339("2026-09-19T00:00:00Z")
                    .unwrap()
                    .to_utc()
            )
        );
        assert_eq!(
            arguments("2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z").onto,
            None
        );
    }

    #[test]
    fn a_run_reads_the_window_it_is_given() {
        let window = arguments("2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z").window();

        assert_eq!(window.from(), &RecordedAt::from(1789862400000));
        assert_eq!(window.to(), &RecordedAt::from(1789948800000));
    }

    #[test]
    fn a_run_reads_a_window_given_in_another_zone_as_the_instant_it_names() {
        let window = arguments("2026-09-20T09:00:00+09:00", "2026-09-21T09:00:00+09:00").window();

        assert_eq!(
            window,
            arguments("2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z").window()
        );
    }

    #[test]
    fn a_window_that_is_not_an_instant_is_rejected() {
        let unreadable = Arguments::try_parse_from([
            "lithify",
            "mysql",
            "--from",
            "yesterday afternoon",
            "--to",
            "2026-09-21T00:00:00Z",
            "--access-key-id",
            "minioadmin",
            "--secret-access-key",
            "minioadmin",
        ]);

        assert!(unreadable.is_err());
    }
}
