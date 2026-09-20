use event_replay_core::position::{Dialect, Position};
use event_replay_core::schema::MalformedEvent;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PostgresPosition {
    lsn: u64,
}

impl PostgresPosition {
    pub fn lsn(&self) -> u64 {
        self.lsn
    }
}

impl Position for PostgresPosition {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Postgres;

impl Dialect for Postgres {
    type Position = PostgresPosition;

    fn position(&self, source: Value) -> Result<Self::Position, MalformedEvent> {
        source
            .get("lsn")
            .and_then(Value::as_u64)
            .map(|lsn| PostgresPosition { lsn })
            .ok_or(MalformedEvent::UnknownDialect(source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_replay_core::position::Dialect;
    use serde_json::json;

    fn position(lsn: u64) -> PostgresPosition {
        Postgres.position(json!({"lsn": lsn})).unwrap()
    }

    #[test]
    fn positions_order_by_log_sequence_number() {
        assert!(position(26738384) < position(26738385));
    }

    #[test]
    fn a_source_without_a_log_sequence_number_is_rejected() {
        let source = json!({"file": "binlog.000002", "pos": 448});

        assert_eq!(
            Postgres.position(source.clone()).unwrap_err(),
            MalformedEvent::UnknownDialect(source)
        );
    }

    #[test]
    fn a_schema_change_is_positioned_by_its_log_sequence_number() {
        let change = Postgres
            .schema_change(json!({
                "source": {"lsn": 26738384i64, "txId": 770},
                "ts_ms": 1789800000000i64,
                "ddl": "",
                "tableChanges": []
            }))
            .unwrap();

        assert_eq!(change.position(), &position(26738384));
    }
}
