use std::convert::Infallible;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::build::Base;
use crate::listener::{ReplayEnd, SchemaChangeListener};
use crate::model::TableId;
use crate::sink::{TablePath, UnsupportedColumn, Warehouse};
use crate::source::RowChangeLog;
use crate::sql::SqlString;
use crate::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnbuildableArtifact {
    WithoutPrimaryKey(TableId),
    PrimaryKeyChanged(TableId),
    WithoutEra(TableId),
    WithoutBase(TableId),
    AcrossEras(TableId),
    UnsupportedColumn(UnsupportedColumn),
}

impl fmt::Display for UnbuildableArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithoutPrimaryKey(table) => {
                write!(
                    formatter,
                    "{table} has no primary key to fold its changes by"
                )
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
            Self::WithoutBase(table) => write!(
                formatter,
                "{table} stood before its window opened but nothing built of it closes then to fold onto"
            ),
            Self::AcrossEras(table) => write!(
                formatter,
                "{table} changed its schema within the window, which SCD type 2 does not carry its versions across yet"
            ),
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

// TODO: restrict a name to the characters every place it reaches takes, such as `[a-z0-9_]`, as
// it names a build log column, a configuration key and a dry run file alike.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct ArtifactName(String);

impl ArtifactName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ArtifactName {
    fn from(name: &str) -> Self {
        Self(name.to_owned())
    }
}

impl From<String> for ArtifactName {
    fn from(name: String) -> Self {
        Self(name)
    }
}

impl std::str::FromStr for ArtifactName {
    type Err = Infallible;

    fn from_str(name: &str) -> Result<Self, Infallible> {
        Ok(Self::from(name))
    }
}

impl fmt::Display for ArtifactName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub trait Artifact {
    const KIND: &'static str;

    type From: Artifact;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>: ArtifactBuilder<L::Position, Context = W>
    where
        Self: 'a;

    fn table_name(&self, table: &TableId, window: &Window) -> String;

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        log: &'a L,
        window: &'a Window,
        base: Base,
    ) -> Self::Builder<'a, L, W>;
}

pub trait ArtifactBuilder<P>: SchemaChangeListener<P, Error = UnbuildableArtifact> {
    fn select(
        &self,
        warehouse: &Self::Context,
        end: &ReplayEnd<UnbuildableArtifact>,
    ) -> Result<SqlString, UnbuildableArtifact>;

    fn create(
        &self,
        warehouse: &Self::Context,
        end: &ReplayEnd<UnbuildableArtifact>,
        target: &TablePath,
    ) -> Result<Vec<SqlString>, UnbuildableArtifact>;
}
