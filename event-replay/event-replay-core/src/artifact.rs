use std::fmt;

use crate::model::TableId;
use crate::sink::{UnsupportedColumn, Warehouse};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnbuildableArtifact {
    WithoutPrimaryKey(TableId),
    UnsupportedColumn(UnsupportedColumn),
}

impl fmt::Display for UnbuildableArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithoutPrimaryKey(table) => {
                write!(formatter, "{table} has no primary key to replay by")
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
    fn statements(&self, warehouse: &impl Warehouse) -> Result<Vec<String>, UnbuildableArtifact>;
}
