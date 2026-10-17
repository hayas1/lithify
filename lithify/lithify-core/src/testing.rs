use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;

use crate::build_log::{BuildLog, Latest};
use crate::model::ColumnType;
use crate::sink::{
    ChangeLogSql, OpenWarehouse, TablePath, UnsupportedColumn, Warehouse, WarehouseClient,
    WarehouseSql,
};
use crate::sql::SqlString;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Engine {
    change_logs: BTreeMap<String, SqlString>,
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
            self.identifier(table.namespace()),
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
    fn change_log_namespace(&self) -> &str {
        "mysql_shop_cdc"
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
    type Client = Recorder<Engine>;
    type Credential = ();
    type Error = Infallible;

    fn open(_: &(), _: &(), _: &str) -> Result<Recorder<Engine>, Infallible> {
        Ok(Recorder::new(Engine::default()))
    }
}

pub struct Recorder<W> {
    warehouse: W,
    logged: Vec<BuildLog>,
    present: BTreeSet<TablePath>,
    executed: RefCell<Vec<String>>,
}

impl<W> Recorder<W> {
    pub fn new(warehouse: W) -> Self {
        Self {
            warehouse,
            logged: Vec::new(),
            present: BTreeSet::new(),
            executed: RefCell::default(),
        }
    }

    pub fn logging(mut self, namespace: &str, logs: &[BuildLog]) -> Self {
        self.logged.extend_from_slice(logs);
        self.present.insert(BuildLog::path(namespace));

        self
    }

    pub fn holding(mut self, tables: &[TablePath]) -> Self {
        self.present.extend(tables.iter().cloned());

        self
    }

    pub fn executed(&self) -> Vec<String> {
        self.executed.borrow().clone()
    }
}

impl<W: Warehouse> WarehouseClient for Recorder<W> {
    type Warehouse = W;
    type Error = std::fmt::Error;

    fn warehouse(&self) -> &W {
        &self.warehouse
    }

    fn execute(&self, statement: &SqlString) -> Result<(), Self::Error> {
        self.executed.borrow_mut().push(statement.to_string());

        Ok(())
    }

    fn latest_build_logs(&self, _: &str, latest: &Latest) -> Result<Vec<BuildLog>, Self::Error> {
        let logged = self.logged.iter().cloned();

        Ok(match latest {
            Latest::Attempted => logged.collect(),
            Latest::Succeeded => logged.filter(|log| !log.status().failed()).collect(),
            Latest::SucceededUntil(to) => logged
                .filter(|log| !log.status().failed() && log.window().to() == to)
                .collect(),
        })
    }

    fn exists(&self, table: &TablePath) -> Result<bool, Self::Error> {
        Ok(self.present.contains(table))
    }
}
