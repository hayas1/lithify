use std::fmt;

use crate::model::{TableId, TableModel};
use crate::position::RecordedAt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableChange {
    id: TableId,
    table: Option<TableModel>,
    initialization: bool,
}

impl TableChange {
    pub fn initialized(table: TableModel) -> Self {
        Self {
            id: table.id().clone(),
            table: Some(table),
            initialization: true,
        }
    }

    pub fn altered(table: TableModel) -> Self {
        Self {
            id: table.id().clone(),
            table: Some(table),
            initialization: false,
        }
    }

    pub fn dropped(id: TableId) -> Self {
        Self {
            id,
            table: None,
            initialization: false,
        }
    }

    pub fn id(&self) -> &TableId {
        &self.id
    }

    pub fn table(&self) -> Option<&TableModel> {
        self.table.as_ref()
    }

    pub fn is_initialization(&self) -> bool {
        self.initialization
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
    recorded_at: RecordedAt,
    statement: String,
    tables: Vec<TableChange>,
    hints: Vec<Hint>,
    captured: bool,
}

impl<P> SchemaChange<P> {
    pub fn position(&self) -> &P {
        &self.position
    }

    pub fn recorded_at(&self) -> &RecordedAt {
        &self.recorded_at
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
        recorded_at: RecordedAt,
        statement: &str,
        tables: Vec<TableChange>,
        hints: Vec<Hint>,
    ) -> Self {
        Self {
            position,
            recorded_at,
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
    fn an_initialized_table_carries_its_definition_and_is_an_initialization() {
        let change = TableChange::initialized(customers());

        assert_eq!(change.id(), &TableId::new("shop", "customers"));
        assert_eq!(change.table(), Some(&customers()));
        assert!(change.is_initialization());
    }

    #[test]
    fn an_altered_table_is_defined_without_being_initialized() {
        let change = TableChange::altered(customers());

        assert_eq!(change.id(), &TableId::new("shop", "customers"));
        assert_eq!(change.table(), Some(&customers()));
        assert!(!change.is_initialization());
    }

    #[test]
    fn a_dropped_table_is_left_without_a_definition() {
        let change = TableChange::dropped(TableId::new("shop", "customers"));

        assert_eq!(change.id(), &TableId::new("shop", "customers"));
        assert_eq!(change.table(), None);
        assert!(!change.is_initialization());
    }

    #[test]
    fn a_schema_change_is_taken_at_the_source_unless_marked_captured() {
        let change = SchemaChange::new(1, RecordedAt::from(1), "", Vec::new(), Vec::new());

        assert!(!change.is_captured());
        assert!(change.captured().is_captured());
    }
}
