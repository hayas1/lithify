use std::fmt;

use crate::model::TableId;
use crate::sink::{UnsupportedColumn, Warehouse};
use crate::sql::Sql;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnbuildableArtifact {
    WithoutPrimaryKey(TableId),
    PrimaryKeyChanged(TableId),
    WithoutEra(TableId),
    UnsupportedColumn(UnsupportedColumn),
}

impl fmt::Display for UnbuildableArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithoutPrimaryKey(table) => {
                write!(formatter, "{table} has no primary key to replay by")
            }
            Self::PrimaryKeyChanged(table) => {
                write!(
                    formatter,
                    "{table} changed its primary key within the window"
                )
            }
            Self::WithoutEra(table) => {
                write!(formatter, "{table} lived through no era of the window")
            }
            Self::UnsupportedColumn(unsupported) => unsupported.fmt(formatter),
        }
    }
}

impl std::error::Error for UnbuildableArtifact {}

impl From<UnsupportedColumn> for UnbuildableArtifact {
    fn from(unsupported: UnsupportedColumn) -> Self {
        Self::UnsupportedColumn(unsupported)
    }
}

pub trait Artifact {
    fn statements(&self, warehouse: &impl Warehouse) -> Result<Vec<Sql>, UnbuildableArtifact>;
}

impl<A: Artifact> Artifact for Vec<A> {
    fn statements(&self, warehouse: &impl Warehouse) -> Result<Vec<Sql>, UnbuildableArtifact> {
        let mut statements = Vec::new();

        for artifact in self {
            statements.extend(artifact.statements(warehouse)?);
        }

        Ok(statements)
    }
}
