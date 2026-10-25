use std::convert::Infallible;
use std::fmt;

use crate::build_log::{BuildLog, Series, TableBuild};
use crate::listener::{Finish, ReplayEnd, SchemaChangeListener};
use crate::model::TableId;
use crate::schedule::Timeline;
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

pub trait Artifact {
    const KIND: &'static str;

    type From: Artifact;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>: ArtifactBuilder<L::Position, W>
    where
        Self: 'a;

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
