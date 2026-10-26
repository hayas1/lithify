use std::convert::Infallible;
use std::fmt;

use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use crate::build_log::{BuildLog, Series, TableBuild};
use crate::listener::{Finish, ReplayEnd, SchemaChangeListener};
use crate::model::TableId;
use crate::schedule::Timeline;
use crate::sink::{Namespace, TablePath, UnsupportedColumn, Warehouse};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    namespace: Namespace,
    copies: Vec<Namespace>,
}

impl Placement {
    pub fn new(namespace: Namespace, copies: Vec<Namespace>) -> Self {
        Self { namespace, copies }
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub fn copies(&self) -> &[Namespace] {
        &self.copies
    }
}

impl From<Namespace> for Placement {
    fn from(namespace: Namespace) -> Self {
        Self::new(namespace, Vec::new())
    }
}

pub trait Artifact {
    const KIND: &'static str;

    type From: Artifact;

    type Config;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>: ArtifactBuilder<L::Position, W>
    where
        Self: 'a;

    fn declared(
        name: &ArtifactName,
        config: &Self::Config,
        placement: &Placement,
        zone: Tz,
    ) -> Self;

    fn namespace(&self) -> &Namespace;

    fn series(&self) -> Series;

    fn window(&self, day: &Window, timeline: &Timeline) -> Option<Window>;

    fn base<'b>(
        &self,
        built: &'b [BuildLog],
        table: &TableId,
        window: &Window,
    ) -> Option<&'b BuildLog>;

    fn target(&self, table: &TableId, window: &Window) -> Result<TablePath, UnbuildableArtifact>;

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        log: &'a L,
        table: TableId,
        window: &'a Window,
        previous: Option<&BuildLog>,
    ) -> Self::Builder<'a, L, W>;
}

pub trait ArtifactBuilder<P, W>: SchemaChangeListener<P, Context = W, Error = UnbuildableArtifact>
    + Finish<P, Input = ReplayEnd<UnbuildableArtifact>, Output = TableBuild, FinishError = Infallible>
{
    fn select(
        &self,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
    ) -> Result<SqlString, UnbuildableArtifact>;

    fn create(
        &self,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
    ) -> Result<Vec<SqlString>, UnbuildableArtifact>;
}
