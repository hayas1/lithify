use std::fmt::{self, Display, Formatter};

use mysql::prelude::Queryable;
use mysql::{Pool, Value};

use event_replay_core::model::TableModel;

use crate::row::{Cell, Rows};

pub trait Source {
    fn execute(&mut self, statement: &str) -> Result<(), Unreachable>;

    fn rows(&mut self, table: &TableModel) -> Result<Rows, Unreachable>;
}

impl dyn Source {
    pub fn connect(url: &str) -> Result<Box<Self>, Unreachable> {
        match url.split_once("://").map(|(scheme, _)| scheme) {
            Some("mysql") => Ok(Box::new(MySql::connect(url)?)),
            scheme => Err(Unreachable::UnknownScheme(
                scheme.unwrap_or_default().to_owned(),
            )),
        }
    }
}

pub struct MySql {
    pool: Pool,
}

impl MySql {
    pub fn connect(url: &str) -> Result<Self, Unreachable> {
        Ok(Self {
            pool: Pool::new(url)?,
        })
    }

    fn select(table: &TableModel) -> String {
        let columns: Vec<String> = table
            .columns()
            .map(|column| format!("`{}`", column.name()))
            .collect();

        format!(
            "SELECT {} FROM `{}`.`{}`",
            columns.join(", "),
            table.id().schema(),
            table.id().table()
        )
    }
}

impl Source for MySql {
    fn execute(&mut self, statement: &str) -> Result<(), Unreachable> {
        Ok(self.pool.get_conn()?.query_drop(statement)?)
    }

    fn rows(&mut self, table: &TableModel) -> Result<Rows, Unreachable> {
        let read: Vec<mysql::Row> = self.pool.get_conn()?.query(Self::select(table))?;

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

    #[test]
    fn a_source_is_reached_by_the_client_its_scheme_names() {
        assert!(matches!(
            <dyn Source>::connect("oracle://system@oracle:1521/shop"),
            Err(Unreachable::UnknownScheme(scheme)) if scheme == "oracle"
        ));
    }
}
