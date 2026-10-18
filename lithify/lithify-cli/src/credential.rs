use lithify_protocol::sink::duckdb::store::S3Credential;

use crate::unrunnable::Unrunnable;

pub struct Credentials {
    duckdb: Option<S3Credential>,
    // bigquery: Option<GcpCredential>, // TODO: read from the command line rather than infra.toml
}

impl Credentials {
    pub fn new(duckdb: Option<S3Credential>) -> Self {
        Self { duckdb }
    }

    pub fn duckdb(&self) -> Result<&S3Credential, Unrunnable> {
        self.duckdb
            .as_ref()
            .ok_or(Unrunnable::Uncredentialed("duckdb"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_duckdb_sink_read_without_a_credential_says_what_it_misses() {
        assert!(matches!(
            Credentials::new(None).duckdb(),
            Err(Unrunnable::Uncredentialed(_))
        ));
    }
}
