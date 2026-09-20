use std::fmt;

use serde_json::Value;

use crate::position::RecordedAt;

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
pub struct Column {
    name: String,
    type_name: String,
    position: u32,
    optional: bool,
    default_value: Option<String>,
    length: Option<u32>,
    scale: Option<u32>,
}

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
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

    pub fn length(&self) -> Option<u32> {
        self.length
    }

    pub fn scale(&self) -> Option<u32> {
        self.scale
    }

    fn read(column: &Value) -> Result<Self, MalformedEvent> {
        let text = |field: &str| column.get(field).and_then(Value::as_str);
        let number = |field: &str| column.get(field).and_then(Value::as_u64).map(|n| n as u32);
        let missing = |field: &str| MalformedEvent::MissingField(field.to_owned());

        Ok(Self {
            name: text("name")
                .ok_or_else(|| missing("columns[].name"))?
                .to_owned(),
            type_name: text("typeName")
                .ok_or_else(|| missing("columns[].typeName"))?
                .to_owned(),
            position: number("position").ok_or_else(|| missing("columns[].position"))?,
            optional: column
                .get("optional")
                .and_then(Value::as_bool)
                .ok_or_else(|| missing("columns[].optional"))?,
            default_value: text("defaultValueExpression").map(str::to_owned),
            length: number("length"),
            scale: number("scale"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    id: TableId,
    columns: Vec<Column>,
    primary_key: Vec<String>,
}

impl Table {
    pub fn id(&self) -> &TableId {
        &self.id
    }

    pub fn columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter()
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|column| column.name() == name)
    }

    pub fn primary_key(&self) -> impl Iterator<Item = &str> {
        self.primary_key.iter().map(String::as_str)
    }

    fn read(id: TableId, table: &Value) -> Result<Self, MalformedEvent> {
        let columns = table
            .get("columns")
            .and_then(Value::as_array)
            .ok_or_else(|| MalformedEvent::MissingField("table.columns".to_owned()))?;
        let mut columns: Vec<Column> =
            columns.iter().map(Column::read).collect::<Result<_, _>>()?;
        columns.sort_by_key(Column::position);

        Ok(Self {
            id,
            columns,
            primary_key: table
                .get("primaryKeyColumnNames")
                .and_then(Value::as_array)
                .map(|names| {
                    names
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableChange {
    Created(Table),
    Altered(Table),
    Dropped(TableId),
}

impl TableChange {
    pub fn id(&self) -> &TableId {
        match self {
            Self::Created(table) | Self::Altered(table) => table.id(),
            Self::Dropped(id) => id,
        }
    }

    fn read(change: &Value) -> Result<Self, MalformedEvent> {
        let kind = change
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| MalformedEvent::MissingField("tableChanges[].type".to_owned()))?;
        let id = change
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| MalformedEvent::MissingField("tableChanges[].id".to_owned()))?;
        let id = TableId::try_from(id)?;
        let table = || {
            change
                .get("table")
                .ok_or_else(|| MalformedEvent::MissingField("tableChanges[].table".to_owned()))
                .and_then(|table| Table::read(id.clone(), table))
        };

        match kind {
            "CREATE" => Ok(Self::Created(table()?)),
            "ALTER" => Ok(Self::Altered(table()?)),
            "DROP" => Ok(Self::Dropped(id)),
            unknown => Err(MalformedEvent::UnknownTableChange(unknown.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaChange<P> {
    position: P,
    recorded_at: RecordedAt,
    statement: String,
    tables: Vec<TableChange>,
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

    pub(crate) fn read(
        envelope: Value,
        position: impl FnOnce(Value) -> Result<P, MalformedEvent>,
    ) -> Result<Self, MalformedEvent> {
        let mut envelope = match envelope {
            Value::Object(fields) => fields,
            other => return Err(MalformedEvent::NotAnEvent(other)),
        };
        let mut take = |field: &str| {
            envelope
                .remove(field)
                .ok_or_else(|| MalformedEvent::MissingField(field.to_owned()))
        };

        let position = position(take("source")?)?;
        let recorded_at = RecordedAt::from(
            take("ts_ms")?
                .as_i64()
                .ok_or_else(|| MalformedEvent::MissingField("ts_ms".to_owned()))?,
        );
        let statement = take("ddl")?
            .as_str()
            .ok_or_else(|| MalformedEvent::MissingField("ddl".to_owned()))?
            .to_owned();
        let tables = take("tableChanges")?
            .as_array()
            .ok_or_else(|| MalformedEvent::MissingField("tableChanges".to_owned()))?
            .iter()
            .map(TableChange::read)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            position,
            recorded_at,
            statement,
            tables,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MalformedEvent {
    MissingField(String),
    UnknownDialect(Value),
    UnknownTableChange(String),
    UnknownTableId(String),
    NotAnEvent(Value),
}

impl fmt::Display for MalformedEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField(field) => write!(formatter, "missing field {field}"),
            Self::UnknownDialect(source) => write!(formatter, "unknown dialect in source {source}"),
            Self::UnknownTableChange(kind) => write!(formatter, "unknown table change {kind}"),
            Self::UnknownTableId(id) => write!(formatter, "unknown table id {id}"),
            Self::NotAnEvent(value) => write!(formatter, "not an event: {value}"),
        }
    }
}

impl std::error::Error for MalformedEvent {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{Log, Offset};
    use crate::position::Dialect;
    use serde_json::{Value, json};

    fn envelope(table_changes: Value) -> Value {
        json!({
            "source": {"offset": 448},
            "ts_ms": 1789800000000i64,
            "databaseName": "shop",
            "ddl": "CREATE TABLE `customers` (`id` bigint NOT NULL)",
            "tableChanges": table_changes,
        })
    }

    fn created_customers() -> Value {
        json!([{
            "type": "CREATE",
            "id": "\"shop\".\"customers\"",
            "table": {
                "primaryKeyColumnNames": ["id"],
                "columns": [
                    {"name": "id", "typeName": "BIGINT", "position": 1, "optional": false,
                     "defaultValueExpression": null, "length": null, "scale": null},
                    {"name": "name", "typeName": "VARCHAR", "position": 2, "optional": false,
                     "defaultValueExpression": null, "length": 255, "scale": null}
                ]
            }
        }])
    }

    fn schema_change(table_changes: Value) -> SchemaChange<Offset> {
        Log.schema_change(envelope(table_changes)).unwrap()
    }

    #[test]
    fn a_schema_change_is_positioned_and_timed_like_any_other_event() {
        let change = schema_change(created_customers());

        assert_eq!(change.position(), &Offset::from(448));
        assert_eq!(change.recorded_at(), &RecordedAt::from(1789800000000i64));
    }

    #[test]
    fn a_created_table_carries_its_columns_in_position_order() {
        let change = schema_change(created_customers());

        let TableChange::Created(table) = change.tables().next().unwrap() else {
            panic!("expected a created table");
        };
        assert_eq!(table.id(), &TableId::new("shop", "customers"));
        assert_eq!(
            table.columns().map(Column::name).collect::<Vec<_>>(),
            ["id", "name"]
        );
        assert_eq!(table.primary_key().collect::<Vec<_>>(), ["id"]);
    }

    #[test]
    fn a_column_carries_what_a_snapshot_query_needs_to_declare_it() {
        let change = schema_change(created_customers());

        let TableChange::Created(table) = change.tables().next().unwrap() else {
            panic!("expected a created table");
        };
        let name = table.column("name").unwrap();
        assert_eq!(name.type_name(), "VARCHAR");
        assert_eq!(name.position(), 2);
        assert!(!name.is_optional());
        assert_eq!(name.default_value(), None);
    }

    #[test]
    fn an_altered_table_carries_the_table_as_it_now_stands() {
        let mut changes = created_customers();
        changes[0]["type"] = json!("ALTER");

        let change = schema_change(changes);

        let TableChange::Altered(table) = change.tables().next().unwrap() else {
            panic!("expected an altered table");
        };
        assert_eq!(table.id(), &TableId::new("shop", "customers"));
    }

    #[test]
    fn a_dropped_table_carries_only_its_identifier() {
        let change = schema_change(json!([{"type": "DROP", "id": "\"shop\".\"customers\""}]));

        let TableChange::Dropped(id) = change.tables().next().unwrap() else {
            panic!("expected a dropped table");
        };
        assert_eq!(id, &TableId::new("shop", "customers"));
    }

    #[test]
    fn a_statement_that_changes_no_table_carries_no_table_change() {
        let change = schema_change(json!([]));

        assert_eq!(change.tables().count(), 0);
        assert_eq!(
            change.statement(),
            "CREATE TABLE `customers` (`id` bigint NOT NULL)"
        );
    }

    #[test]
    fn an_unknown_table_change_is_rejected() {
        let changes = json!([{"type": "TRUNCATE", "id": "\"shop\".\"customers\""}]);

        assert_eq!(
            Log.schema_change(envelope(changes)).unwrap_err(),
            MalformedEvent::UnknownTableChange("TRUNCATE".to_owned())
        );
    }
}
