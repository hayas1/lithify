use std::fmt;

use crate::artifact::{ArtifactBuilder, UnbuildableArtifact};
use crate::build_log::{BuildLog, Latest};
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

    fn change_log_namespace(&self) -> &str;

    fn change_log(&self, topic: &str) -> TablePath {
        TablePath::new(self.change_log_namespace(), topic)
    }

    fn change_log_ref(&self, topic: &str) -> SqlString {
        self.table_ref(&self.change_log(topic))
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

    fn timestamp_millis(&self, milliseconds: i64) -> SqlString;

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

    fn unix_millis(&self, expression: &SqlString) -> SqlString;

    fn create_build_log(&self, namespace: &str) -> Result<SqlString, UnsupportedColumn> {
        let columns = std::iter::once(&(BuildLog::ID, ColumnType::Int64))
            .chain(BuildLog::COLUMNS.iter())
            .map(|(name, column_type)| {
                Ok(format!(
                    "{} {}",
                    self.identifier(name),
                    self.column_type(column_type)?
                ))
            })
            .collect::<Result<Vec<_>, UnsupportedColumn>>()?;

        Ok(SqlString::from(format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            self.table_ref(&BuildLog::path(namespace)),
            columns.join(", ")
        )))
    }

    fn append_build_log(&self, log: &BuildLog) -> SqlString {
        let id = self.identifier(BuildLog::ID);
        let columns = BuildLog::COLUMNS
            .iter()
            .map(|(name, _)| self.identifier(name))
            .collect::<Vec<_>>();
        let table = self.table_ref(&BuildLog::path(log.namespace()));

        SqlString::from(format!(
            "INSERT INTO {table} ({id}, {})\nSELECT COALESCE(MAX({id}), 0) + 1, {}\nFROM {table}",
            columns.join(", "),
            log.cells(self).join(", ")
        ))
    }

    fn select_latest_build_logs(&self, namespace: &str, latest: &Latest) -> SqlString {
        let columns = BuildLog::COLUMNS
            .iter()
            .map(|(name, column_type)| {
                let column = self.identifier(name);

                match column_type {
                    ColumnType::ZonedTimestamp => {
                        format!("{} AS {column}", self.unix_millis(&column))
                    }
                    _ => column.to_string(),
                }
            })
            .collect::<Vec<_>>();
        let partition = ["artifact", "schedule", "table_id"]
            .map(|name| self.identifier(name))
            .join(", ");

        SqlString::from(format!(
            "SELECT {}\nFROM {}\nWHERE {}\nQUALIFY ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {} DESC) = 1\nORDER BY {partition}",
            columns.join(", "),
            self.table_ref(&BuildLog::path(namespace)),
            latest.condition(self),
            self.identifier(BuildLog::ID),
        ))
    }

    fn select_schema_changes(&self, log: &impl SchemaChangeLog, window: &Window) -> SqlString {
        let envelope = log
            .envelope()
            .map(|envelope| self.path(&envelope))
            .unwrap_or_else(|| SqlString::from("*"));
        let recorded_at = self.path(&log.recorded_at());
        let ordering = log
            .sequence()
            .iter()
            .map(|path| self.path(path).to_string())
            .collect::<Vec<_>>()
            .join(", ");

        SqlString::from(format!(
            "SELECT {envelope}\nFROM {}\nWHERE {recorded_at} >= {} AND {recorded_at} < {}\nORDER BY {ordering}",
            self.change_log_ref(&log.ddl_topic()),
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

    fn latest_build_logs(
        &self,
        namespace: &str,
        latest: &Latest,
    ) -> Result<Vec<BuildLog>, Self::Error>;

    fn create_build_log<E>(&self, namespace: &str) -> Result<(), E>
    where
        E: From<Self::Error> + From<UnbuildableArtifact>,
    {
        let warehouse = self.warehouse();

        self.execute(&warehouse.create_namespace(namespace))?;
        self.execute(
            &warehouse
                .create_build_log(namespace)
                .map_err(UnbuildableArtifact::from)?,
        )?;

        Ok(())
    }

    fn append_build_log(&self, log: &BuildLog) -> Result<(), Self::Error> {
        self.execute(&self.warehouse().append_build_log(log))
    }

    fn build<E>(&self, builder: &impl ArtifactBuilder) -> Result<(), E>
    where
        E: From<Self::Error> + From<UnbuildableArtifact>,
    {
        for statement in builder.create()? {
            self.execute(&statement)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::build_log::{Outcome, Run};
    use crate::model::TableId;
    use crate::position::RecordedAt;

    struct Engine;

    impl Warehouse for Engine {
        type Config = ();

        fn timestamp_millis(&self, milliseconds: i64) -> SqlString {
            SqlString::from(milliseconds.to_string())
        }

        fn unix_millis(&self, expression: &SqlString) -> SqlString {
            expression.clone()
        }

        fn change_log_namespace(&self) -> &str {
            "cdc"
        }

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

        fn latest_build_logs(&self, _: &str, _: &Latest) -> Result<Vec<BuildLog>, Self::Error> {
            Ok(vec![])
        }
    }

    struct Namespaced(TableId);

    impl ArtifactBuilder for Namespaced {
        fn table(&self) -> &TableId {
            &self.0
        }

        fn select(&self) -> Result<SqlString, UnbuildableArtifact> {
            Ok(SqlString::from("SELECT 1 AS id"))
        }

        fn create(&self) -> Result<Vec<SqlString>, UnbuildableArtifact> {
            Ok(vec![
                Engine.create_namespace("shop"),
                SqlString::from("CREATE TABLE shop.people (id BIGINT)"),
            ])
        }

        fn build_log(&self, run: &Run, outcome: Outcome) -> BuildLog {
            BuildLog::new(
                run,
                self.0.clone(),
                None,
                TablePath::new("shop", "people"),
                Window::until(RecordedAt::from(1)),
                outcome,
            )
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

        client
            .build::<Unbuilt>(&Namespaced(TableId::new("shop", "people")))
            .unwrap();

        assert_eq!(
            client.executed.into_inner(),
            [
                "CREATE SCHEMA IF NOT EXISTS \"shop\"",
                "CREATE TABLE shop.people (id BIGINT)"
            ]
        );
    }

    #[test]
    fn a_change_log_is_read_from_the_topic_it_landed_on_in_the_namespace_of_the_warehouse() {
        assert_eq!(
            Engine.change_log_ref("mysql.shop.people"),
            Engine.table_ref(&TablePath::new("cdc", "mysql.shop.people"))
        );
    }
}
