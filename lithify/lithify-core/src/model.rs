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

    pub fn named(&self, name: &str) -> Self {
        Self {
            name: name.to_owned(),
            ..self.clone()
        }
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

    pub fn with_column_renamed(&self, previous: &str, name: &str) -> Self {
        let rename = |held: &str| match held == previous {
            true => name.to_owned(),
            false => held.to_owned(),
        };

        Self {
            id: self.id.clone(),
            columns: self
                .columns()
                .map(|held| held.named(&rename(held.name())))
                .collect(),
            primary_key: self.primary_key().map(rename).collect(),
        }
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
    fn a_table_with_a_column_renamed_holds_it_in_place_under_its_new_name() {
        let renamed = customers().with_column_renamed("name", "full_name");

        assert_eq!(names(&renamed), ["id", "full_name", "email"]);
        assert_eq!(renamed.column("full_name").unwrap().type_name(), "VARCHAR");
    }

    #[test]
    fn a_table_with_a_key_column_renamed_stays_keyed_by_it() {
        let renamed = customers().with_column_renamed("id", "customer_id");

        assert_eq!(renamed.primary_key().collect::<Vec<_>>(), ["customer_id"]);
    }

    #[test]
    fn a_table_under_another_id_keeps_its_columns_and_key() {
        let moved = customers().with_id(&TableId::new("shop", "clients"));

        assert_eq!(moved.id(), &TableId::new("shop", "clients"));
        assert_eq!(names(&moved), ["id", "name", "email"]);
        assert_eq!(moved.primary_key().collect::<Vec<_>>(), ["id"]);
    }

    #[test]
    fn a_table_with_a_column_it_never_had_renamed_stays_as_it_was() {
        assert_eq!(
            customers().with_column_renamed("nickname", "alias"),
            customers()
        );
    }
}
