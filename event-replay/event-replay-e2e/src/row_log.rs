use duckdb::types::FromSql;

use event_replay_cli::pipeline::Capture;
use event_replay_core::source::{ColumnPath, RowChangeLog, SchemaChangeLog};
use event_replay_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbSchemaLogFn};
use event_replay_protocol::source::debezium::Debezium;
use event_replay_protocol::source::debezium::config::Dialect;
use event_replay_protocol::{MySql, Postgres};

pub trait RowLogFn {
    type Output;

    fn call<L: RowChangeLog>(self, log: L) -> Self::Output;
}

pub trait DuckDbRowCapture {
    fn with_duckdb_row_log<F: RowLogFn>(&self, f: F) -> F::Output;
}

impl DuckDbRowCapture for Capture {
    fn with_duckdb_row_log<F: RowLogFn>(&self, f: F) -> F::Output {
        let envelope = self
            .with_duckdb_schema_log(Envelope)
            .map(|envelope| envelope.to_string());
        let envelope = envelope.as_deref();
        let Capture::Debezium(debezium) = self;

        match debezium.dialect() {
            Dialect::MySql => f.call(Debezium::within(envelope, MySql)),
            Dialect::Postgres => f.call(Debezium::within(envelope, Postgres)),
        }
    }
}

struct Envelope;

impl DuckDbSchemaLogFn for Envelope {
    type Output = Option<ColumnPath>;

    fn call_duckdb<L>(self, log: L) -> Self::Output
    where
        L: SchemaChangeLog,
        L::Record: FromSql,
    {
        log.envelope()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Probe;

    impl RowLogFn for Probe {
        type Output = (&'static str, Option<ColumnPath>);

        fn call<L: RowChangeLog>(self, log: L) -> Self::Output {
            (std::any::type_name::<L>(), log.envelope())
        }
    }

    fn capture(dialect: &str) -> Capture {
        toml::from_str(&format!("[debezium]\ndialect = \"{dialect}\"")).unwrap()
    }

    #[test]
    fn a_row_log_fn_is_called_with_the_log_of_the_dialect_under_the_envelope_of_duckdb() {
        assert_eq!(
            capture("mysql").with_duckdb_row_log(Probe),
            (
                std::any::type_name::<Debezium<MySql>>(),
                Some(ColumnPath::from("value"))
            )
        );
        assert_eq!(
            capture("postgres").with_duckdb_row_log(Probe),
            (
                std::any::type_name::<Debezium<Postgres>>(),
                Some(ColumnPath::from("value"))
            )
        );
    }
}
