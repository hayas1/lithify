use std::fmt;

use crate::model::{TableId, TableModel};
use crate::ts::{Processing, Ts};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableChange {
    Created(TableModel),
    Altered(TableModel),
    Dropped(TableId),
}

impl TableChange {
    pub fn id(&self) -> &TableId {
        match self {
            Self::Created(table) | Self::Altered(table) => table.id(),
            Self::Dropped(id) => id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hint {
    TableRename {
        from: TableId,
        to: TableId,
    },
    ColumnRename {
        table: TableId,
        from: String,
        to: String,
    },
}

impl Hint {
    pub fn ids(&self) -> Vec<&TableId> {
        match self {
            Self::TableRename { from, to } => vec![from, to],
            Self::ColumnRename { table, .. } => vec![table],
        }
    }

    pub fn as_table_rename(&self) -> Option<(&TableId, &TableId)> {
        match self {
            Self::TableRename { from, to } => Some((from, to)),
            Self::ColumnRename { .. } => None,
        }
    }

    pub fn as_column_rename(&self) -> Option<(&TableId, &str, &str)> {
        match self {
            Self::TableRename { .. } => None,
            Self::ColumnRename { table, from, to } => Some((table, from, to)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaChange<P> {
    position: P,
    processing_time: Ts<Processing>,
    statement: String,
    tables: Vec<TableChange>,
    hints: Vec<Hint>,
    captured: bool,
}

impl<P> SchemaChange<P> {
    pub fn position(&self) -> &P {
        &self.position
    }

    pub fn processing_time(&self) -> &Ts<Processing> {
        &self.processing_time
    }

    pub fn statement(&self) -> &str {
        &self.statement
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableChange> {
        self.tables.iter()
    }

    pub fn hints(&self) -> impl Iterator<Item = &Hint> {
        self.hints.iter()
    }

    pub fn column_renames<'a>(
        &'a self,
        table: &'a TableId,
    ) -> impl Iterator<Item = (&'a str, &'a str)> {
        let renamed_to = move |id: &TableId| {
            self.hints()
                .filter_map(Hint::as_table_rename)
                .any(|(from, to)| from == id && to == table)
        };

        self.hints()
            .filter_map(Hint::as_column_rename)
            .filter(move |(renamed, ..)| *renamed == table || renamed_to(renamed))
            .map(|(_, from, to)| (from, to))
    }

    pub fn new(
        position: P,
        processing_time: Ts<Processing>,
        statement: &str,
        tables: Vec<TableChange>,
        hints: Vec<Hint>,
    ) -> Self {
        Self {
            position,
            processing_time,
            statement: statement.to_owned(),
            tables,
            hints,
            captured: false,
        }
    }

    pub fn captured(self) -> Self {
        Self {
            captured: true,
            ..self
        }
    }

    pub fn is_captured(&self) -> bool {
        self.captured
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MalformedEvent {
    Undecodable(String),
    UnknownTableChange(String),
    UnknownTableId(String),
}

impl fmt::Display for MalformedEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Undecodable(problem) => write!(formatter, "undecodable event: {problem}"),
            Self::UnknownTableChange(kind) => write!(formatter, "unknown table change {kind}"),
            Self::UnknownTableId(id) => write!(formatter, "unknown table id {id}"),
        }
    }
}

impl std::error::Error for MalformedEvent {}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::model::{ColumnModel, ColumnType};

    fn customers() -> TableModel {
        TableModel::new(
            TableId::new("shop", "customers"),
            vec![ColumnModel::new(
                "id",
                "BIGINT",
                ColumnType::Int64,
                false,
                None,
            )],
            vec!["id".to_owned()],
        )
    }

    #[test]
    fn a_table_created_or_altered_is_named_by_its_definition() {
        assert_eq!(
            TableChange::Created(customers()).id(),
            &TableId::new("shop", "customers")
        );
        assert_eq!(
            TableChange::Altered(customers()).id(),
            &TableId::new("shop", "customers")
        );
    }

    #[test]
    fn a_dropped_table_is_named_by_its_id_alone() {
        assert_eq!(
            TableChange::Dropped(TableId::new("shop", "customers")).id(),
            &TableId::new("shop", "customers")
        );
    }

    #[test]
    fn a_schema_change_is_taken_at_the_source_unless_marked_captured() {
        let change = SchemaChange::new(1, Ts::try_from(1).unwrap(), "", Vec::new(), Vec::new());

        assert!(!change.is_captured());
        assert!(change.captured().is_captured());
    }
}
