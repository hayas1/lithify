use std::fmt;

use crate::artifact::UnbuildableArtifact;
use crate::build_log::{
    BuildLog, BuildLogQuery, BuildLogRow, Series, Status, TableBuild, TableBuilt,
    UnreadableBuildLog,
};
use crate::model::{ColumnModel, ColumnName, ColumnType};
use crate::source::{ColumnPath, SchemaChangeLog};
use crate::sql::SqlString;
use crate::ts::Ts;
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
pub struct Namespace(Vec<String>);

impl Namespace {
    pub fn parts(&self) -> impl DoubleEndedIterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }
}

impl From<&str> for Namespace {
    fn from(path: &str) -> Self {
        Self(path.split('.').map(str::to_owned).collect())
    }
}

impl From<Vec<String>> for Namespace {
    fn from(parts: Vec<String>) -> Self {
        Self(parts)
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.join("."))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TablePath {
    namespace: Namespace,
    name: String,
}

impl TablePath {
    pub fn new(namespace: impl Into<Namespace>, name: &str) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.to_owned(),
        }
    }

    pub fn namespace(&self) -> &Namespace {
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

    fn namespace_ref(&self, namespace: &Namespace) -> SqlString {
        SqlString::from(
            namespace
                .parts()
                .map(|part| self.identifier(part).to_string())
                .collect::<Vec<_>>()
                .join("."),
        )
    }

    fn table_ref(&self, table: &TablePath) -> SqlString;

    fn column_ref(&self, column: &ColumnName) -> SqlString {
        self.identifier(column.as_str())
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

    fn create_namespace(&self, namespace: &Namespace) -> SqlString {
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

    fn create_view(&self, view: &TablePath, select: &SqlString) -> SqlString {
        SqlString::from(format!(
            "CREATE OR REPLACE VIEW {} AS\n{select}",
            self.table_ref(view)
        ))
    }
}

pub trait ChangeLogSql: WarehouseSql {
    fn change_log_namespace(&self) -> &Namespace;

    fn change_log(&self, topic: &str) -> TablePath {
        TablePath::new(self.change_log_namespace().clone(), topic)
    }

    fn change_log_ref(&self, topic: &str) -> SqlString {
        self.table_ref(&self.change_log(topic))
    }

    fn select_schema_changes(&self, log: &impl SchemaChangeLog, window: &Window) -> SqlString {
        let envelope = log
            .envelope()
            .map(|envelope| self.path(&envelope))
            .unwrap_or_else(|| SqlString::from("*"));
        let event_time = self.path(&log.event_time());
        let ordering = log
            .sequence()
            .iter()
            .map(|path| self.path(path).to_string())
            .collect::<Vec<_>>()
            .join(", ");

        SqlString::from(format!(
            "SELECT {envelope}\nFROM {}\nWHERE {event_time} >= {} AND {event_time} < {}\nORDER BY {ordering}",
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

    async fn call<K: WarehouseClient>(self, client: &K) -> Self::Output;
}

pub trait OpenWarehouse {
    type Config;
    type Location;
    type Client: WarehouseClient;
    type Credential;
    type Error: std::error::Error;

    async fn open(
        config: &Self::Config,
        credential: &Self::Credential,
        change_log: &Self::Location,
    ) -> Result<Self::Client, Self::Error>;
}

pub trait SinkWarehouse {
    type Location: Clone + fmt::Debug + PartialEq + Eq + Into<Namespace>;
    type Credential;
    type Error: std::error::Error;

    fn reaches(&self, location: &Self::Location) -> bool;

    async fn with_client<F: WarehouseFn>(
        &self,
        change_log: &Self::Location,
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
    async fn execute(&self, statement: &SqlString) -> Result<(), Self::Error>;

    async fn build_log_rows(
        &self,
        namespace: &Namespace,
        query: &BuildLogQuery,
    ) -> Result<Vec<BuildLogRow>, Self::Error>;

    async fn build_logs<E>(
        &self,
        namespace: &Namespace,
        series: &Series,
        query: &BuildLogQuery,
    ) -> Result<Vec<BuildLog>, E>
    where
        E: From<Self::Error> + From<UnreadableBuildLog>,
    {
        if !self.exists(&BuildLog::path(namespace)).await? {
            return Ok(Vec::new());
        }

        self.build_log_rows(namespace, query)
            .await?
            .into_iter()
            .filter(|row| row.advances(series))
            .map(|row| Ok(BuildLog::try_from(row)?))
            .collect()
    }

    async fn create_build_log<E>(&self, namespace: &Namespace) -> Result<(), E>
    where
        E: From<Self::Error> + From<UnbuildableArtifact>,
    {
        let warehouse = self.warehouse();

        self.execute(&warehouse.create_namespace(namespace)).await?;
        self.execute(
            &BuildLog::create_table(warehouse, namespace).map_err(UnbuildableArtifact::from)?,
        )
        .await?;

        Ok(())
    }

    async fn exists(&self, table: &TablePath) -> Result<bool, Self::Error>;

    async fn append_build_logs(
        &self,
        namespace: &Namespace,
        logs: &[BuildLog],
    ) -> Result<(), Self::Error> {
        match BuildLog::insert(logs, self.warehouse(), namespace) {
            Some(insert) => self.execute(&insert).await,
            None => Ok(()),
        }
    }

    async fn build(&self, build: TableBuild) -> TableBuilt {
        let status = match build.refusal() {
            Some(refused) => Status::Failed(refused.to_string()),
            None => match self
                .execute_all(build.statements().unwrap_or_default())
                .await
            {
                Ok(()) => Status::Succeeded,
                Err(failed) => Status::Failed(failed.to_string()),
            },
        };

        build.built(status, Ts::now())
    }

    async fn execute_all(&self, statements: &[SqlString]) -> Result<(), Self::Error> {
        for statement in statements {
            self.execute(statement).await?;
        }

        Ok(())
    }
}
