use std::borrow::Borrow;
use std::fmt;

use crate::event::MalformedEvent;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableId {
    schema: String,
    table: String,
}

impl TableId {
    pub fn new(schema: &str, table: &str) -> Self {
        Self {
            schema: schema.to_owned(),
            table: table.to_owned(),
        }
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn table(&self) -> &str {
        &self.table
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}", self.schema, self.table)
    }
}

impl TryFrom<&str> for TableId {
    type Error = MalformedEvent;

    fn try_from(quoted: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = quoted
            .split('.')
            .map(|part| part.trim_matches('"'))
            .collect();

        match parts.as_slice() {
            [schema, table] => Ok(Self::new(schema, table)),
            _ => Err(MalformedEvent::UnknownTableId(quoted.to_owned())),
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnName(String);

impl ColumnName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ColumnName {
    fn from(name: &str) -> Self {
        Self(name.to_owned())
    }
}

impl From<String> for ColumnName {
    fn from(name: String) -> Self {
        Self(name)
    }
}

impl Borrow<str> for ColumnName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ColumnName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, formatter)
    }
}

impl fmt::Display for ColumnName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl PartialEq<str> for ColumnName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for ColumnName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal { precision: u32, scale: u32 },
    Text,
    Bytes,
    Date,
    Time,
    Timestamp,
    ZonedTimestamp,
    Json,
    Array(Box<ColumnType>),
    Unsupported(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnModel {
    name: ColumnName,
    type_name: String,
    column_type: ColumnType,
    optional: bool,
    default_value: Option<String>,
    comment: Option<String>,
}

impl ColumnModel {
    pub fn new(
        name: &str,
        type_name: &str,
        column_type: ColumnType,
        optional: bool,
        default_value: Option<&str>,
    ) -> Self {
        Self {
            name: ColumnName::from(name),
            type_name: type_name.to_owned(),
            column_type,
            optional,
            default_value: default_value.map(str::to_owned),
            comment: None,
        }
    }

    pub fn with_comment(self, comment: Option<&str>) -> Self {
        Self {
            comment: comment.map(str::to_owned),
            ..self
        }
    }

    pub fn name(&self) -> &ColumnName {
        &self.name
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    pub fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    pub fn is_optional(&self) -> bool {
        self.optional
    }

    pub fn default_value(&self) -> Option<&str> {
        self.default_value.as_deref()
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }
}

// An empty key stands for a table without one, and every artifact checks for it with
// `is_empty`. Building it only through `new(Vec<ColumnName>) -> Option<Self>`, as `NonZero`
// is, with `TableModel` holding `Option<PrimaryKeyColumn>`, would leave a table without a key
// one way to be told and let query builders take a key without checking it again; not done
// while the check stays at the entry of each artifact.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrimaryKeyColumn(Vec<ColumnName>);

impl PrimaryKeyColumn {
    pub fn columns(&self) -> impl Iterator<Item = &ColumnName> {
        self.0.iter()
    }

    pub fn contains(&self, column: &ColumnName) -> bool {
        self.0.contains(column)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<const N: usize> From<[&str; N]> for PrimaryKeyColumn {
    fn from(columns: [&str; N]) -> Self {
        columns.into_iter().map(ColumnName::from).collect()
    }
}

impl FromIterator<ColumnName> for PrimaryKeyColumn {
    fn from_iter<I: IntoIterator<Item = ColumnName>>(columns: I) -> Self {
        Self(columns.into_iter().collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableModel {
    id: TableId,
    columns: Vec<ColumnModel>,
    primary_key: PrimaryKeyColumn,
}

impl TableModel {
    pub fn new(id: TableId, columns: Vec<ColumnModel>, primary_key: PrimaryKeyColumn) -> Self {
        Self {
            id,
            columns,
            primary_key,
        }
    }

    pub fn id(&self) -> &TableId {
        &self.id
    }

    pub fn columns(&self) -> impl Iterator<Item = &ColumnModel> {
        self.columns.iter()
    }

    pub fn column(&self, name: &ColumnName) -> Option<&ColumnModel> {
        self.columns.iter().find(|column| column.name() == name)
    }

    pub fn primary_key(&self) -> &PrimaryKeyColumn {
        &self.primary_key
    }

    pub fn with_id(&self, id: &TableId) -> Self {
        Self {
            id: id.clone(),
            columns: self.columns.clone(),
            primary_key: self.primary_key.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, type_name: &str) -> ColumnModel {
        ColumnModel::new(name, type_name, ColumnType::Text, true, None)
    }

    fn customers() -> TableModel {
        TableModel::new(
            TableId::new("shop", "customers"),
            vec![
                column("id", "BIGINT"),
                column("name", "VARCHAR"),
                column("email", "VARCHAR"),
            ],
            ["id"].into(),
        )
    }

    fn names(table: &TableModel) -> Vec<&ColumnName> {
        table.columns().map(ColumnModel::name).collect()
    }

    #[test]
    fn a_table_under_another_id_keeps_its_columns_and_key() {
        let moved = customers().with_id(&TableId::new("shop", "clients"));

        assert_eq!(moved.id(), &TableId::new("shop", "clients"));
        assert_eq!(names(&moved), ["id", "name", "email"]);
        assert_eq!(moved.primary_key().columns().collect::<Vec<_>>(), ["id"]);
    }
}
