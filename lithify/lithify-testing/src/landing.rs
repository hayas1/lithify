use std::collections::BTreeMap;

use lithify_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use lithify_core::sink::{ChangeLogSql, WarehouseSql};
use lithify_core::sql::SqlString;
use lithify_protocol::DuckDbWarehouse;

use crate::replica::{CellValue, ColumnDdl, Ddl, Dml, Mutation, Placed, Replica, ReplicaDump, Row};

pub struct Landing<'a> {
    replica: &'a Replica,
}

impl<'a> Landing<'a> {
    pub const TOPIC_PREFIX: &'static str = "mysql";

    pub fn of(replica: &'a Replica) -> Self {
        Self { replica }
    }

    pub fn create(&self, warehouse: &DuckDbWarehouse) -> SqlString {
        let mut dump = ReplicaDump::default();
        let mut schema_changes = Vec::new();
        let mut topics: BTreeMap<String, Topic> = BTreeMap::new();

        for (position, (at, mutation)) in self.replica.mutations().iter().enumerate() {
            let landed = Landed {
                position: position + 1,
                at: at.milliseconds_since_epoch(),
            };
            match mutation {
                Mutation::Dml(dml) => {
                    let before = dump.row(dml.table(), dml.row()).cloned();
                    dump.apply(mutation);
                    let after = dump.row(dml.table(), dml.row()).cloned();
                    let table = dump
                        .table(dml.table())
                        .unwrap_or_else(|| panic!("{} is changed while it stands", dml.table()));
                    topics
                        .entry(Self::topic(dml.table()))
                        .or_default()
                        .push(table, landed.row_change(dml, before, after));
                }
                Mutation::Ddl(ddl) => {
                    dump.apply(mutation);
                    schema_changes.push(landed.schema_change(ddl, &dump));
                }
            }
        }

        let mut statements = vec![format!(
            "{};\nCREATE TABLE {} AS SELECT * FROM (VALUES\n{}\n) AS t(value)",
            warehouse.create_namespace(warehouse.change_log_namespace()),
            warehouse.change_log_ref(Self::TOPIC_PREFIX),
            schema_changes.join(",\n")
        )];
        for (topic, landed) in &topics {
            statements.extend(landed.create(&warehouse.change_log_ref(topic)));
        }

        SqlString::from(statements.join(";\n"))
    }

    fn topic(table: &TableId) -> String {
        format!(
            "{}.{}.{}",
            Self::TOPIC_PREFIX,
            table.schema(),
            table.table()
        )
    }

    fn literal(text: &str) -> String {
        format!("'{}'", text.replace('\'', "''"))
    }

    fn nullable(text: Option<&str>, sql_type: &str) -> String {
        match text {
            Some(text) => format!("{}::{sql_type}", Self::literal(text)),
            None => format!("NULL::{sql_type}"),
        }
    }
}

struct Landed {
    position: usize,
    at: i64,
}

impl Landed {
    fn row_change(&self, dml: &Dml, before: Option<Row>, after: Option<Row>) -> RowChange {
        RowChange {
            position: self.position,
            at: self.at,
            op: match dml {
                Dml::Insert { .. } => "c",
                Dml::Update { .. } => "u",
                Dml::Delete { .. } => "d",
            },
            before,
            after,
        }
    }

    fn schema_change(&self, ddl: &Ddl, dump: &ReplicaDump) -> String {
        let (kind, id) = match ddl {
            Ddl::Create(table) => ("CREATE", table.id()),
            Ddl::Drop(table) => ("DROP", table),
            Ddl::Rename { to, .. } => ("ALTER", to),
            Ddl::Alter { table, renamed, .. } => ("ALTER", renamed.as_ref().unwrap_or(table)),
        };
        let table = dump.table(id).map_or("NULL".to_owned(), Self::definition);

        format!(
            "({{'source': {{'file': 'binlog.000001', 'pos': {}::BIGINT, 'gtid': NULL::VARCHAR, 'ts_ms': {at}::BIGINT}}, 'ts_ms': {at}::BIGINT, 'ddl': {}, 'tableChanges': [{{'type': '{kind}', 'id': {}, 'table': {table}}}]}})",
            self.position,
            Landing::literal(&Statement(ddl).to_string()),
            Landing::literal(&format!("\"{}\".\"{}\"", id.schema(), id.table())),
            at = self.at,
        )
    }

    fn definition(table: &TableModel) -> String {
        let key = table
            .primary_key()
            .columns()
            .map(|column| Landing::literal(column.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        let columns = table
            .columns()
            .enumerate()
            .map(|(index, column)| {
                format!(
                    "{{'name': {}, 'typeName': {}, 'position': {}::BIGINT, 'optional': {}, 'defaultValueExpression': {}, 'length': NULL::BIGINT, 'scale': NULL::BIGINT, 'comment': {}}}",
                    Landing::literal(column.name().as_str()),
                    Landing::literal(column.type_name()),
                    index + 1,
                    column.is_optional(),
                    Landing::nullable(column.default_value(), "VARCHAR"),
                    Landing::nullable(column.comment(), "VARCHAR"),
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!("{{'primaryKeyColumnNames': [{key}]::VARCHAR[], 'columns': [{columns}]}}")
    }
}

struct RowChange {
    position: usize,
    at: i64,
    op: &'static str,
    before: Option<Row>,
    after: Option<Row>,
}

#[derive(Default)]
struct Topic {
    shape: Vec<ColumnModel>,
    changes: Vec<RowChange>,
}

impl Topic {
    fn push(&mut self, table: &TableModel, change: RowChange) {
        self.widen(table);
        self.changes.push(change);
    }

    fn widen(&mut self, table: &TableModel) {
        for column in table.columns() {
            match self
                .shape
                .iter_mut()
                .find(|held| held.name() == column.name())
            {
                Some(held) if Self::sql_type(held) != Self::sql_type(column) => {
                    *held = ColumnModel::new(
                        column.name().as_str(),
                        "VARCHAR",
                        ColumnType::Text,
                        true,
                        None,
                    );
                }
                Some(_) => {}
                None => self.shape.push(column.clone()),
            }
        }
    }

    fn create(&self, table: &SqlString) -> Vec<String> {
        let image = format!(
            "STRUCT({})",
            self.shape
                .iter()
                .map(|column| format!("\"{}\" {}", column.name(), Self::sql_type(column)))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut statements = vec![format!(
            "CREATE TABLE {table} (value STRUCT(op VARCHAR, ts_ms BIGINT, source STRUCT(file VARCHAR, pos BIGINT, ts_ms BIGINT), before {image}, after {image}))"
        )];
        if !self.changes.is_empty() {
            let values = self
                .changes
                .iter()
                .map(|change| {
                    format!(
                        "({{'op': '{}', 'ts_ms': {at}::BIGINT, 'source': {{'file': 'binlog.000001', 'pos': {}::BIGINT, 'ts_ms': {at}::BIGINT}}, 'before': {}, 'after': {}}})",
                        change.op,
                        change.position,
                        self.image(change.before.as_ref(), &image),
                        self.image(change.after.as_ref(), &image),
                        at = change.at,
                    )
                })
                .collect::<Vec<_>>()
                .join(",\n");
            statements.push(format!("INSERT INTO {table} VALUES\n{values}"));
        }

        statements
    }

    fn image(&self, row: Option<&Row>, image: &str) -> String {
        let Some(row) = row else {
            return format!("NULL::{image}");
        };
        let cells = self
            .shape
            .iter()
            .map(|column| {
                format!(
                    "{}: {}",
                    Landing::literal(column.name().as_str()),
                    Cell(
                        row.get(column.name()).unwrap_or(&CellValue::Null),
                        Self::sql_type(column)
                    )
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!("{{{cells}}}::{image}")
    }

    fn sql_type(column: &ColumnModel) -> &'static str {
        match column.column_type() {
            ColumnType::Int16 => "SMALLINT",
            ColumnType::Int32 => "INTEGER",
            ColumnType::Int64 => "BIGINT",
            _ => "VARCHAR",
        }
    }
}

struct Cell<'a>(&'a CellValue, &'a str);

impl std::fmt::Display for Cell<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(value, sql_type) = self;
        match value {
            CellValue::Null => write!(formatter, "NULL::{sql_type}"),
            CellValue::Int(number) => write!(formatter, "{number}::{sql_type}"),
            CellValue::Text(text) => write!(formatter, "{}::{sql_type}", Landing::literal(text)),
        }
    }
}

struct Statement<'a>(&'a Ddl);

impl std::fmt::Display for Statement<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Ddl::Create(table) => {
                let columns = table
                    .columns()
                    .map(|column| format!("{} {}", column.name(), column.type_name()))
                    .chain([format!(
                        "PRIMARY KEY ({})",
                        table
                            .primary_key()
                            .columns()
                            .map(|column| column.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )])
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(formatter, "CREATE TABLE {} ({columns})", table.id().table())
            }
            Ddl::Drop(table) => write!(formatter, "DROP TABLE {}", table.table()),
            Ddl::Rename { from, to } => {
                write!(formatter, "RENAME TABLE {} TO {}", from.table(), to.table())
            }
            Ddl::Alter {
                table,
                renamed,
                changes,
            } => {
                let clauses = changes
                    .iter()
                    .map(|change| Clause(change).to_string())
                    .chain(renamed.iter().map(|to| format!("RENAME TO {}", to.table())))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(formatter, "ALTER TABLE {} {clauses}", table.table())
            }
        }
    }
}

struct Clause<'a>(&'a ColumnDdl);

impl std::fmt::Display for Clause<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            ColumnDdl::Add { column, placed } => write!(
                formatter,
                "ADD COLUMN {} {}{}",
                column.name(),
                column.type_name(),
                Placement(placed)
            ),
            ColumnDdl::Drop(name) => write!(formatter, "DROP COLUMN {name}"),
            ColumnDdl::Rename { from, to } => write!(formatter, "RENAME COLUMN {from} TO {to}"),
            ColumnDdl::Change {
                from,
                column,
                placed,
            } if from == column.name() => write!(
                formatter,
                "MODIFY COLUMN {} {}{}",
                column.name(),
                column.type_name(),
                Placement(placed)
            ),
            ColumnDdl::Change {
                from,
                column,
                placed,
            } => write!(
                formatter,
                "CHANGE COLUMN {from} {} {}{}",
                column.name(),
                column.type_name(),
                Placement(placed)
            ),
            ColumnDdl::Rekey(key) => write!(
                formatter,
                "DROP PRIMARY KEY, ADD PRIMARY KEY ({})",
                key.columns()
                    .map(|column| column.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

struct Placement<'a>(&'a Placed);

impl std::fmt::Display for Placement<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Placed::Kept | Placed::Last => Ok(()),
            Placed::First => write!(formatter, " FIRST"),
            Placed::After(column) => write!(formatter, " AFTER {column}"),
        }
    }
}
