mod pipeline;
mod row;
mod run;
mod runnable;
mod scenario;
mod sink;
mod source;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;

use lithify_cli::credential::Credentials;
use lithify_protocol::sink::duckdb::store::{S3Credential, Secret};

use crate::run::{Run, Unverifiable};
use crate::runnable::Report;

#[derive(Parser)]
#[command(
    name = "lithify-e2e",
    about = "Run a scenario against a pipeline and assert on what its sinks built"
)]
struct Arguments {
    #[arg(
        value_name = "SCENARIO",
        help = "The toml file the scenario is written in"
    )]
    scenario: PathBuf,
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
        default_value = "180",
        value_name = "SECONDS",
        help = "How long to wait for the capture to land what a step applied"
    )]
    patience: u64,
}

impl Arguments {
    fn run(&self) -> Result<Run, Unverifiable> {
        let written = std::fs::read_to_string(&self.scenario).map_err(Unverifiable::Missing)?;

        Ok(Run::new(
            written.parse()?,
            self.scenario.parent().unwrap_or(Path::new(".")),
            self.credentials(),
            self.patience,
        ))
    }

    fn credentials(&self) -> Credentials {
        Credentials::new(Some(S3Credential::new(
            self.access_key_id.clone(),
            self.secret_access_key.clone(),
        )))
    }

    async fn report(&self) -> Result<Report, Unverifiable> {
        self.run()?.report().await
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = Arguments::parse();

    match arguments.report().await {
        Ok(report) => report.print(),
        Err(unverifiable) => {
            eprintln!("{}: {unverifiable}", arguments.scenario.display());

            ExitCode::FAILURE
        }
    }
}
