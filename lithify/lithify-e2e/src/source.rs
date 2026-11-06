use std::fmt::{self, Display, Formatter};

use mysql::prelude::Queryable;
use mysql::{Pool, Value};

use lithify_core::model::TableModel;

use crate::row::{Cell, Rows};

pub trait Source {
    async fn execute(&self, statement: &str) -> Result<(), Unreachable>;

    async fn rows(&self, table: &TableModel) -> Result<Rows, Unreachable>;
}

impl Source for Pool {
    async fn execute(&self, statement: &str) -> Result<(), Unreachable> {
        Ok(self.get_conn()?.query_drop(statement)?)
    }

    async fn rows(&self, table: &TableModel) -> Result<Rows, Unreachable> {
        let columns: Vec<String> = table
            .columns()
            .map(|column| format!("`{}`", column.name()))
            .collect();
        let read: Vec<mysql::Row> = self.get_conn()?.query(format!(
            "SELECT {} FROM `{}`.`{}`",
            columns.join(", "),
            table.id().schema(),
            table.id().table()
        ))?;

        Ok(read
            .iter()
            .map(|row| {
                (0..row.len())
                    .map(|at| Cell::from(row.as_ref(at).unwrap_or(&Value::NULL)))
                    .collect()
            })
            .collect())
    }
}

pub enum SourceClient {
    MySql(Pool),
}

impl SourceClient {
    pub async fn connect(url: &str) -> Result<Self, Unreachable> {
        match url.split_once("://").map(|(scheme, _)| scheme) {
            Some("mysql") => Ok(Self::MySql(Pool::new(url)?)),
            scheme => Err(Unreachable::UnknownScheme(
                scheme.unwrap_or_default().to_owned(),
            )),
        }
    }

    pub async fn execute(&self, statement: &str) -> Result<(), Unreachable> {
        match self {
            Self::MySql(client) => client.execute(statement).await,
        }
    }

    pub async fn rows(&self, table: &TableModel) -> Result<Rows, Unreachable> {
        match self {
            Self::MySql(client) => client.rows(table).await,
        }
    }
}

impl From<&Value> for Cell {
    fn from(value: &Value) -> Self {
        match value {
            Value::NULL => Self::Missing,
            Value::Bytes(bytes) => Self::Read(String::from_utf8_lossy(bytes).into_owned()),
            Value::Int(read) => Self::Read(read.to_string()),
            Value::UInt(read) => Self::Read(read.to_string()),
            Value::Float(read) => Self::Read(read.to_string()),
            Value::Double(read) => Self::Read(read.to_string()),
            Value::Date(..) | Value::Time(..) => {
                Self::Read(value.as_sql(true).trim_matches('\'').to_owned())
            }
        }
    }
}

#[derive(Debug)]
pub enum Unreachable {
    Unusable(mysql::Error),
    UnknownScheme(String),
}

impl Display for Unreachable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unusable(cause) => write!(formatter, "{cause}"),
            Self::UnknownScheme(scheme) => {
                write!(formatter, "no source is reached through {scheme:?}")
            }
        }
    }
}

impl std::error::Error for Unreachable {}

impl From<mysql::Error> for Unreachable {
    fn from(cause: mysql::Error) -> Self {
        Self::Unusable(cause)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_source_is_reached_by_the_client_its_scheme_names() {
        assert!(matches!(
            SourceClient::connect("oracle://system@oracle:1521/shop").await,
            Err(Unreachable::UnknownScheme(scheme)) if scheme == "oracle"
        ));
    }
}
