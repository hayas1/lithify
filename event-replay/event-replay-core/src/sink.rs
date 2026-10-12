use std::fmt;

use crate::artifact::{Artifact, UnbuildableArtifact};
use crate::model::{ColumnModel, ColumnType};
use crate::source::{ColumnPath, SchemaChangeLog};
use crate::sql::SqlString;
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

    fn identifier(&self, name: &str) -> SqlString;

    fn namespace_ref(&self, namespace: &str) -> SqlString {
        self.identifier(namespace)
    }

    fn table_ref(&self, table: &TablePath) -> SqlString;

    fn change_log_ref(&self, table: &TablePath) -> SqlString {
        self.table_ref(table)
    }

    fn column_type(&self, column_type: &ColumnType) -> Result<SqlString, UnsupportedColumn>;

    fn read(
        &self,
        expression: &SqlString,
        column_type: &ColumnType,
    ) -> Result<SqlString, UnsupportedColumn>;

    fn cast_or_null(
        &self,
        expression: &SqlString,
        column_type: &ColumnType,
    ) -> Result<SqlString, UnsupportedColumn>;

    fn fill(&self, column: &ColumnModel) -> Result<SqlString, UnsupportedColumn> {
        match column.default_value() {
            Some(default) => self.cast_or_null(&self.literal(default), column.column_type()),
            None => Ok(SqlString::from(format!(
                "CAST(NULL AS {})",
                self.column_type(column.column_type())?
            ))),
        }
    }

    fn literal(&self, text: &str) -> SqlString {
        SqlString::from(format!("'{}'", text.replace('\'', "''")))
    }

    fn create_namespace(&self, namespace: &str) -> SqlString {
        SqlString::from(format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            self.namespace_ref(namespace)
        ))
    }

    fn path(&self, path: &ColumnPath) -> SqlString {
        SqlString::from(
            path.parts()
                .map(|part| self.identifier(part).to_string())
                .collect::<Vec<_>>()
                .join("."),
        )
    }

    fn select_schema_changes(
        &self,
        log: &impl SchemaChangeLog,
        table: &TablePath,
        window: &Window,
    ) -> SqlString {
        let envelope = log
            .envelope()
            .map(|envelope| self.path(&envelope))
            .unwrap_or_else(|| SqlString::from("*"));
        let recorded_at = self.path(&log.recorded_at());
        let ordering = log
            .ordering()
            .iter()
            .map(|path| self.path(path).to_string())
            .collect::<Vec<_>>()
            .join(", ");

        SqlString::from(format!(
            "SELECT {envelope}\nFROM {}\nWHERE {recorded_at} >= {} AND {recorded_at} < {}\nORDER BY {ordering}",
            self.change_log_ref(table),
            window.from().milliseconds_since_epoch(),
            window.to().milliseconds_since_epoch(),
        ))
    }
}

pub trait WarehouseClient {
    type Warehouse: Warehouse;
    type Error: std::error::Error;

    fn warehouse(&self) -> &Self::Warehouse;

    // TODO: a client may come to be asked to create_table and the like rather than to
    // execute any statement, so that what a warehouse builds is named in this trait
    // instead of being spelled out as SQL by every artifact.
    fn execute(&self, statement: &SqlString) -> Result<(), Self::Error>;

    fn build<E>(&self, artifact: &impl Artifact) -> Result<(), E>
    where
        E: From<Self::Error> + From<UnbuildableArtifact>,
    {
        for statement in artifact.statements()? {
            self.execute(&statement)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    struct Engine;

    impl Warehouse for Engine {
        type Config = ();

        fn identifier(&self, name: &str) -> SqlString {
            SqlString::from(format!("\"{name}\""))
        }

        fn table_ref(&self, table: &TablePath) -> SqlString {
            SqlString::from(format!("{}.{}", table.namespace(), table.name()))
        }

        fn column_type(&self, column_type: &ColumnType) -> Result<SqlString, UnsupportedColumn> {
            Err(UnsupportedColumn::new(column_type))
        }

        fn read(
            &self,
            _: &SqlString,
            column_type: &ColumnType,
        ) -> Result<SqlString, UnsupportedColumn> {
            Err(UnsupportedColumn::new(column_type))
        }

        fn cast_or_null(
            &self,
            _: &SqlString,
            column_type: &ColumnType,
        ) -> Result<SqlString, UnsupportedColumn> {
            Err(UnsupportedColumn::new(column_type))
        }
    }

    #[derive(Default)]
    struct Recorder {
        executed: RefCell<Vec<String>>,
    }

    impl WarehouseClient for Recorder {
        type Warehouse = Engine;
        type Error = std::fmt::Error;

        fn warehouse(&self) -> &Self::Warehouse {
            &Engine
        }

        fn execute(&self, statement: &SqlString) -> Result<(), Self::Error> {
            self.executed.borrow_mut().push(statement.to_string());

            Ok(())
        }
    }

    struct Namespaced;

    impl Artifact for Namespaced {
        fn statements(&self) -> Result<Vec<SqlString>, UnbuildableArtifact> {
            Ok(vec![
                Engine.create_namespace("replay"),
                SqlString::from("CREATE TABLE replay.people (id BIGINT)"),
            ])
        }
    }

    #[derive(Debug)]
    enum Unbuilt {
        Unexecuted,
        Unbuildable,
    }

    impl From<std::fmt::Error> for Unbuilt {
        fn from(_: std::fmt::Error) -> Self {
            Self::Unexecuted
        }
    }

    impl From<UnbuildableArtifact> for Unbuilt {
        fn from(_: UnbuildableArtifact) -> Self {
            Self::Unbuildable
        }
    }

    #[test]
    fn a_client_builds_an_artifact_by_running_its_statements_in_order() {
        let client = Recorder::default();

        client.build::<Unbuilt>(&Namespaced).unwrap();

        assert_eq!(
            client.executed.into_inner(),
            [
                "CREATE SCHEMA IF NOT EXISTS \"replay\"",
                "CREATE TABLE replay.people (id BIGINT)"
            ]
        );
    }

    #[test]
    fn a_change_log_is_read_where_its_table_is_unless_the_warehouse_reads_it_elsewhere() {
        let table = TablePath::new("cdc", "people");

        assert_eq!(Engine.change_log_ref(&table), Engine.table_ref(&table));
    }
}
