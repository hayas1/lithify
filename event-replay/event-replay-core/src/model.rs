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
    optional: bool,
    default_value: Option<String>,
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
            name: name.to_owned(),
            type_name: type_name.to_owned(),
            column_type,
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

    pub fn without_column(&self, name: &str) -> Self {
        Self {
            id: self.id.clone(),
            columns: self
                .columns()
                .filter(|held| held.name() != name)
                .cloned()
                .collect(),
            primary_key: self.primary_key.clone(),
        }
    }

    pub fn with_column(&self, column: &ColumnModel) -> Self {
        let appended = self.column(column.name()).is_none().then_some(column);

        Self {
            id: self.id.clone(),
            columns: self
                .columns()
                .map(|held| match held.name() == column.name() {
                    true => column,
                    false => held,
                })
                .chain(appended)
                .cloned()
                .collect(),
            primary_key: self.primary_key.clone(),
        }
    }

    pub fn keyed_by<'a>(&self, primary_key: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            id: self.id.clone(),
            columns: self.columns.clone(),
            primary_key: primary_key.into_iter().map(str::to_owned).collect(),
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
            vec!["id".to_owned()],
        )
    }

    fn names(table: &TableModel) -> Vec<&str> {
        table.columns().map(ColumnModel::name).collect()
    }

    #[test]
    fn a_table_holds_its_columns_in_the_order_they_were_given() {
        let table = TableModel::new(
            TableId::new("shop", "customers"),
            vec![column("name", "VARCHAR"), column("id", "BIGINT")],
            vec!["id".to_owned()],
        );

        assert_eq!(names(&table), ["name", "id"]);
    }

    #[test]
    fn a_table_without_a_column_keeps_the_rest_in_their_order() {
        assert_eq!(names(&customers().without_column("name")), ["id", "email"]);
    }

    #[test]
    fn a_table_without_a_column_it_never_had_stays_as_it_was() {
        assert_eq!(customers().without_column("nickname"), customers());
    }

    #[test]
    fn a_table_with_a_column_of_a_name_it_has_replaces_that_column_in_place() {
        let retyped = customers().with_column(&column("name", "TEXT"));

        assert_eq!(names(&retyped), ["id", "name", "email"]);
        assert_eq!(retyped.column("name").unwrap().type_name(), "TEXT");
    }

    #[test]
    fn a_table_with_a_new_column_holds_it_last() {
        let added = customers().with_column(&column("nickname", "VARCHAR"));

        assert_eq!(names(&added), ["id", "name", "email", "nickname"]);
    }

    #[test]
    fn a_table_keyed_by_other_columns_keeps_its_columns() {
        let rekeyed = customers().keyed_by(["id", "email"]);

        assert_eq!(rekeyed.primary_key().collect::<Vec<_>>(), ["id", "email"]);
        assert_eq!(names(&rekeyed), ["id", "name", "email"]);
    }
}
