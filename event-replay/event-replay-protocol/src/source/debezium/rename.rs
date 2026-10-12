use sqlparser::ast::{AlterTable, AlterTableOperation, ObjectName, RenameTableNameKind, Statement};
use sqlparser::parser::Parser;

use event_replay_core::event::TableChange;
use event_replay_core::model::TableId;

pub struct RenameReader<S> {
    dialect: S,
}

impl<S: sqlparser::dialect::Dialect> RenameReader<S> {
    pub fn new(dialect: S) -> Self {
        Self { dialect }
    }

    pub fn read(&self, ddl: &str, schema: &str) -> Vec<TableChange> {
        Parser::parse_sql(&self.dialect, ddl)
            .unwrap_or_default()
            .iter()
            .flat_map(|statement| Self::statement(statement, schema))
            .collect()
    }

    fn statement(statement: &Statement, schema: &str) -> Vec<TableChange> {
        match statement {
            Statement::RenameTable(renames) => renames
                .iter()
                .map(|rename| TableChange::Renamed {
                    from: Self::id(&rename.old_name, schema),
                    to: Self::id(&rename.new_name, schema),
                })
                .collect(),
            Statement::AlterTable(alter) => Self::alter(alter, schema),
            _ => Vec::new(),
        }
    }

    fn alter(alter: &AlterTable, schema: &str) -> Vec<TableChange> {
        let table = Self::id(&alter.name, schema);
        let columns = alter
            .operations
            .iter()
            .filter_map(|operation| Self::column(operation, &table));
        let tables = alter
            .operations
            .iter()
            .filter_map(|operation| Self::table(operation, &table, schema));

        columns.chain(tables).collect()
    }

    fn column(operation: &AlterTableOperation, table: &TableId) -> Option<TableChange> {
        let (from, to) = match operation {
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => (old_column_name, new_column_name),
            AlterTableOperation::ChangeColumn {
                old_name, new_name, ..
            } => (old_name, new_name),
            _ => return None,
        };

        (from != to).then(|| TableChange::ColumnRenamed {
            table: table.clone(),
            from: from.value.clone(),
            to: to.value.clone(),
        })
    }

    fn table(
        operation: &AlterTableOperation,
        table: &TableId,
        schema: &str,
    ) -> Option<TableChange> {
        match operation {
            AlterTableOperation::RenameTable {
                table_name: RenameTableNameKind::To(name) | RenameTableNameKind::As(name),
            } => Some(TableChange::Renamed {
                from: table.clone(),
                to: Self::id(name, schema),
            }),
            _ => None,
        }
    }

    fn id(name: &ObjectName, schema: &str) -> TableId {
        let mut parts = name
            .0
            .iter()
            .filter_map(|part| part.as_ident())
            .map(|ident| ident.value.as_str())
            .rev();
        let table = parts.next().unwrap_or_default();

        TableId::new(parts.next().unwrap_or(schema), table)
    }
}
