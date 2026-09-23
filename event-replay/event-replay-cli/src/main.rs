use std::path::PathBuf;
use std::process::ExitCode;

use chrono::{DateTime, Utc};
use clap::Parser;

use event_replay_core::position::RecordedAt;
use event_replay_core::window::Window;

use event_replay_cli::credential::{Credential, Secret};
use event_replay_cli::run::Run;

#[derive(Parser)]
#[command(
    name = "event-replay",
    about = "Report the schema changes a window of a pipeline's change log holds"
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
}

impl Arguments {
    fn window(&self) -> Window {
        Window::new(RecordedAt::from(self.from), RecordedAt::from(self.to))
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
        )
    }
}

fn main() -> ExitCode {
    let arguments = Arguments::parse();

    match Run::from(arguments).report() {
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
            "event-replay",
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
            "event-replay",
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
