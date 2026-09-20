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
    name: String,
    type_name: String,
    column_type: ColumnType,
    position: u32,
    optional: bool,
    default_value: Option<String>,
}

impl ColumnModel {
    pub fn new(
        name: &str,
        type_name: &str,
        column_type: ColumnType,
        position: u32,
        optional: bool,
        default_value: Option<&str>,
    ) -> Self {
        Self {
            name: name.to_owned(),
            type_name: type_name.to_owned(),
            column_type,
            position,
            optional,
            default_value: default_value.map(str::to_owned),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    pub fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    pub fn position(&self) -> u32 {
        self.position
    }

    pub fn is_optional(&self) -> bool {
        self.optional
    }

    pub fn default_value(&self) -> Option<&str> {
        self.default_value.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableModel {
    id: TableId,
    columns: Vec<ColumnModel>,
    primary_key: Vec<String>,
}

impl TableModel {
    pub fn new(id: TableId, columns: Vec<ColumnModel>, primary_key: Vec<String>) -> Self {
        let mut columns = columns;
        columns.sort_by_key(ColumnModel::position);

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

    pub fn column(&self, name: &str) -> Option<&ColumnModel> {
        self.columns.iter().find(|column| column.name() == name)
    }

    pub fn primary_key(&self) -> impl Iterator<Item = &str> {
        self.primary_key.iter().map(String::as_str)
    }
}
