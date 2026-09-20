use event_replay_core::position::{Dialect, Position};
use event_replay_core::schema::MalformedEvent;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MySqlPosition {
    file: String,
    pos: u64,
    transaction: Option<String>,
}

impl MySqlPosition {
    pub fn file(&self) -> &str {
        &self.file
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }

    pub fn transaction(&self) -> Option<&str> {
        self.transaction.as_deref()
    }
}

impl Position for MySqlPosition {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MySql;

impl Dialect for MySql {
    type Position = MySqlPosition;

    fn position(&self, source: Value) -> Result<Self::Position, MalformedEvent> {
        let file = source.get("file").and_then(Value::as_str);
        let pos = source.get("pos").and_then(Value::as_u64);

        match (file, pos) {
            (Some(file), Some(pos)) => Ok(MySqlPosition {
                file: file.to_owned(),
                pos,
                transaction: source
                    .get("gtid")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            }),
            _ => Err(MalformedEvent::UnknownDialect(source)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_replay_core::position::Dialect;
    use serde_json::json;

    fn position(file: &str, pos: u64) -> MySqlPosition {
        MySql.position(json!({"file": file, "pos": pos})).unwrap()
    }

    #[test]
    fn positions_order_by_binlog_file_then_offset() {
        assert!(position("binlog.000002", 448) < position("binlog.000002", 449));
        assert!(position("binlog.000002", 999) < position("binlog.000003", 4));
    }

    #[test]
    fn a_position_carries_the_transaction_identifier_when_the_server_assigns_one() {
        let position = MySql
            .position(json!({"file": "binlog.000002", "pos": 448, "gtid": "f07c93b3:15"}))
            .unwrap();

        assert_eq!(position.transaction(), Some("f07c93b3:15"));
        assert_eq!(self::position("binlog.000002", 448).transaction(), None);
    }

    #[test]
    fn a_source_without_a_binlog_offset_is_rejected() {
        let source = json!({"lsn": 26738384});

        assert_eq!(
            MySql.position(source.clone()).unwrap_err(),
            MalformedEvent::UnknownDialect(source)
        );
    }

    #[test]
    fn a_schema_change_is_positioned_by_its_binlog_offset() {
        let change = MySql
            .schema_change(json!({
                "source": {"file": "binlog.000002", "pos": 448},
                "ts_ms": 1789800000000i64,
                "ddl": "ALTER TABLE `customers` ADD COLUMN `nickname` varchar(255)",
                "tableChanges": []
            }))
            .unwrap();

        assert_eq!(change.position(), &position("binlog.000002", 448));
    }
}
