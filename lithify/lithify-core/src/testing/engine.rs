use std::collections::BTreeMap;
use std::convert::Infallible;

use crate::model::ColumnType;
use crate::sink::{
    ChangeLogSql, Namespace, OpenWarehouse, TablePath, UnsupportedColumn, WarehouseSql,
};
use crate::sql::SqlString;
use crate::testing::Recorder;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Engine {
    change_log: Namespace,
    change_logs: BTreeMap<String, SqlString>,
}

impl Default for Engine {
    fn default() -> Self {
        Self {
            change_log: Namespace::from("mysql_shop_cdc"),
            change_logs: BTreeMap::new(),
        }
    }
}

impl Engine {
    pub fn reading_change_log(mut self, topic: &str, from: &str) -> Self {
        self.change_logs
            .insert(topic.to_owned(), SqlString::from(from));

        self
    }
}

impl WarehouseSql for Engine {
    fn identifier(&self, name: &str) -> SqlString {
        SqlString::from(format!("\"{name}\""))
    }

    fn table_ref(&self, table: &TablePath) -> SqlString {
        SqlString::from(format!(
            "{}.{}",
            self.namespace_ref(table.namespace()),
            self.identifier(table.name())
        ))
    }

    fn column_type(&self, column_type: &ColumnType) -> Result<SqlString, UnsupportedColumn> {
        match column_type {
            ColumnType::Int64 => Ok(SqlString::from("BIGINT")),
            ColumnType::Text => Ok(SqlString::from("TEXT")),
            unsupported => Err(UnsupportedColumn::new(unsupported)),
        }
    }

    fn read(
        &self,
        expression: &SqlString,
        column_type: &ColumnType,
    ) -> Result<SqlString, UnsupportedColumn> {
        Ok(SqlString::from(format!(
            "CAST({expression} AS {})",
            self.column_type(column_type)?
        )))
    }

    fn cast_or_null(
        &self,
        expression: &SqlString,
        column_type: &ColumnType,
    ) -> Result<SqlString, UnsupportedColumn> {
        Ok(SqlString::from(format!(
            "TRY_CAST({expression} AS {})",
            self.column_type(column_type)?
        )))
    }

    fn timestamp_millis(&self, milliseconds: i64) -> SqlString {
        SqlString::from(milliseconds.to_string())
    }

    fn unix_millis(&self, expression: &SqlString) -> SqlString {
        expression.clone()
    }

    fn select_existence(&self, table: &TablePath) -> SqlString {
        SqlString::from(format!("SELECT COUNT(*) FROM {table}"))
    }
}

impl ChangeLogSql for Engine {
    fn change_log_namespace(&self) -> &Namespace {
        &self.change_log
    }

    fn change_log_ref(&self, topic: &str) -> SqlString {
        match self.change_logs.get(topic) {
            Some(from) => from.clone(),
            None => self.table_ref(&self.change_log(topic)),
        }
    }
}

impl OpenWarehouse for Engine {
    type Config = ();
    type Location = Namespace;
    type Client = Recorder<Engine>;
    type Credential = ();
    type Error = Infallible;

    async fn open(_: &(), _: &(), _: &Namespace) -> Result<Recorder<Engine>, Infallible> {
        Ok(Recorder::new(Engine::default()))
    }
}
