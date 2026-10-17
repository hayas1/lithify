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

pub trait WarehouseSql {
    fn identifier(&self, name: &str) -> SqlString;

    fn namespace_ref(&self, namespace: &str) -> SqlString {
        self.identifier(namespace)
    }

    fn table_ref(&self, table: &TablePath) -> SqlString;

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

    fn select_existence(&self, table: &TablePath) -> SqlString;

    fn create_table(&self, table: &TablePath, columns: &[(SqlString, SqlString)]) -> SqlString {
        let columns: Vec<_> = columns
            .iter()
            .map(|(name, column_type)| format!("{name} {column_type}"))
            .collect();

        SqlString::from(format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            self.table_ref(table),
            columns.join(", ")
        ))
    }

    fn create_table_as(&self, table: &TablePath, select: &SqlString) -> SqlString {
        SqlString::from(format!(
            "CREATE OR REPLACE TABLE {} AS\n{select}",
            self.table_ref(table)
        ))
    }

    fn insert(&self, table: &TablePath, columns: &[SqlString], select: &SqlString) -> SqlString {
        let columns: Vec<_> = columns.iter().map(SqlString::to_string).collect();

        SqlString::from(format!(
            "INSERT INTO {} ({})\n{select}",
            self.table_ref(table),
            columns.join(", ")
        ))
    }
}

pub trait ChangeLogSql: WarehouseSql {
    fn change_log_namespace(&self) -> &str;

    fn change_log(&self, topic: &str) -> TablePath {
        TablePath::new(self.change_log_namespace(), topic)
    }

    fn change_log_ref(&self, topic: &str) -> SqlString {
        self.table_ref(&self.change_log(topic))
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

pub trait Warehouse: ChangeLogSql + OpenWarehouse {}

impl<W: ChangeLogSql + OpenWarehouse> Warehouse for W {}

pub trait WarehouseFn {
    type Output;

    fn call<K: WarehouseClient>(self, client: &K) -> Self::Output;
}

pub trait OpenWarehouse {
    type Config;
    type Client: WarehouseClient;
    type Credential;
    type Error: std::error::Error;

    fn open(
        config: &Self::Config,
        credential: &Self::Credential,
        change_log: &str,
    ) -> Result<Self::Client, Self::Error>;
}

pub trait SinkWarehouse {
    type Credential;
    type Error: std::error::Error;

    fn with_client<F: WarehouseFn>(
        &self,
        change_log: &str,
        credential: &Self::Credential,
        f: F,
    ) -> Result<F::Output, Self::Error>;
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
            &BuildLog::create_table(warehouse, namespace).map_err(UnbuildableArtifact::from)?,
        )?;

        Ok(())
    }

    fn exists(&self, table: &TablePath) -> Result<bool, Self::Error>;

    fn append_build_log(&self, namespace: &str, log: &BuildLog) -> Result<(), Self::Error> {
        self.execute(&log.insert(self.warehouse(), namespace))
    }

    fn build<E>(&self, builder: &impl ArtifactBuilder) -> Result<(), E>
    where
        E: From<Self::Error> + From<UnbuildableArtifact>,
    {
        for statement in builder.create(self.warehouse())? {
            self.execute(&statement)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_log::{Outcome, Run};
    use crate::model::TableId;
    use crate::position::RecordedAt;
    use crate::testing::{Engine, Recorder};

    struct Namespaced(TableId);

    impl ArtifactBuilder for Namespaced {
        fn table(&self) -> &TableId {
            &self.0
        }

        fn select(&self) -> Result<SqlString, UnbuildableArtifact> {
            Ok(SqlString::from("SELECT 1 AS id"))
        }

        fn create<W: Warehouse>(
            &self,
            warehouse: &W,
        ) -> Result<Vec<SqlString>, UnbuildableArtifact> {
            Ok(vec![
                warehouse.create_namespace("shop"),
                SqlString::from("CREATE TABLE shop.people (id BIGINT)"),
            ])
        }

        fn build_log(&self, run: &Run, outcome: Outcome) -> BuildLog {
            BuildLog::new(
                run,
                Some(self.0.clone()),
                Some(self.0.clone()),
                None,
                Some(TablePath::new("shop", "people")),
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
        let client = Recorder::new(Engine::default());

        client
            .build::<Unbuilt>(&Namespaced(TableId::new("shop", "people")))
            .unwrap();

        assert_eq!(
            client.executed(),
            [
                "CREATE SCHEMA IF NOT EXISTS \"shop\"",
                "CREATE TABLE shop.people (id BIGINT)"
            ]
        );
    }

    #[test]
    fn a_table_is_created_whole_from_what_a_select_reads_unless_the_warehouse_says_otherwise() {
        let people = TablePath::new("shop", "people");

        assert_eq!(
            Engine::default().create_table_as(&people, &SqlString::from("SELECT 1 AS id")),
            "CREATE OR REPLACE TABLE \"shop\".\"people\" AS\nSELECT 1 AS id"
        );
        assert_eq!(
            Engine::default().create_table(
                &people,
                &[(SqlString::from("\"id\""), SqlString::from("BIGINT"))]
            ),
            "CREATE TABLE IF NOT EXISTS \"shop\".\"people\" (\"id\" BIGINT)"
        );
        assert_eq!(
            Engine::default().insert(
                &people,
                &[SqlString::from("\"id\"")],
                &SqlString::from("SELECT 1")
            ),
            "INSERT INTO \"shop\".\"people\" (\"id\")\nSELECT 1"
        );
    }

    #[test]
    fn a_change_log_is_read_from_the_topic_it_landed_on_in_the_namespace_of_the_warehouse() {
        assert_eq!(
            Engine::default().change_log_ref("mysql.shop.people"),
            Engine::default().table_ref(&TablePath::new("mysql_shop_cdc", "mysql.shop.people"))
        );
    }
}
