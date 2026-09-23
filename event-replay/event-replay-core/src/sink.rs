use std::fmt;

use crate::model::ColumnType;
use crate::source::{ColumnPath, SchemaChangeLog};
use crate::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedColumn {
    column_type: ColumnType,
}

impl UnsupportedColumn {
    pub fn new(column_type: &ColumnType) -> Self {
        Self {
            column_type: column_type.clone(),
        }
    }

    pub fn column_type(&self) -> &ColumnType {
        &self.column_type
    }
}

impl fmt::Display for UnsupportedColumn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "the warehouse has no column for {:?}",
            self.column_type
        )
    }
}

impl std::error::Error for UnsupportedColumn {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TablePath {
    namespace: String,
    name: String,
}

impl TablePath {
    pub fn new(namespace: &str, name: &str) -> Self {
        Self {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for TablePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}", self.namespace, self.name)
    }
}

pub trait Warehouse {
    type Config;

    fn identifier(&self, name: &str) -> String;

    fn table_ref(&self, table: &TablePath) -> String;

    fn column_type(&self, column_type: &ColumnType) -> Result<String, UnsupportedColumn>;

    fn read(&self, expression: &str, column_type: &ColumnType)
    -> Result<String, UnsupportedColumn>;

    fn path(&self, path: &ColumnPath) -> String {
        path.parts()
            .map(|part| self.identifier(part))
            .collect::<Vec<_>>()
            .join(".")
    }

    fn select_schema_changes(
        &self,
        log: &impl SchemaChangeLog,
        table: &TablePath,
        window: &Window,
    ) -> String {
        let envelope = log
            .envelope()
            .map(|envelope| self.path(&envelope))
            .unwrap_or_else(|| "*".to_owned());
        let recorded_at = self.path(&log.recorded_at());
        let ordering = log
            .ordering()
            .iter()
            .map(|path| self.path(path))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "SELECT {envelope}\nFROM {}\nWHERE {recorded_at} >= {} AND {recorded_at} < {}\nORDER BY {ordering}",
            self.table_ref(table),
            window.from().milliseconds_since_epoch(),
            window.to().milliseconds_since_epoch(),
        )
    }
}
