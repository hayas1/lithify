use std::fmt;

use crate::build_log::{BuildLog, Outcome, Run, Series};
use crate::listener::SchemaChangeListener;
use crate::model::{Mask, TableId};
use crate::sink::{TablePath, UnsupportedColumn, Warehouse};
use crate::source::RowChangeLog;
use crate::sql::SqlString;
use crate::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnbuildableArtifact {
    WithoutPrimaryKey(TableId),
    PrimaryKeyChanged(TableId),
    WithoutEra(TableId),
    UnsupportedColumn(UnsupportedColumn),
    UnsupportedMask {
        table: TableId,
        column: String,
        mask: Mask,
    },
    Unmasked {
        table: TableId,
        masked: String,
        renamed: String,
    },
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
            Self::UnsupportedColumn(unsupported) => unsupported.fmt(formatter),
            Self::UnsupportedMask {
                table,
                column,
                mask,
            } => write!(
                formatter,
                "{table} masks {column} with {mask:?}, which no artifact applies yet"
            ),
            Self::Unmasked {
                table,
                masked,
                renamed,
            } => write!(
                formatter,
                "{table} renamed the masked {masked} to {renamed}, which is not masked; mask {renamed} or the values {masked} held come out as they are"
            ),
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

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>: ArtifactBuilder
        + SchemaChangeListener<L::Position, Context = W, Error = UnbuildableArtifact>
    where
        Self: 'a;

    fn series(&self) -> Series;

    fn windows(&self, daily: &Window) -> Vec<Window>;

    fn open_ended(&self) -> bool {
        false
    }

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
    ) -> Result<Self::Builder<'a, L, W>, UnbuildableArtifact>;
}

pub trait ArtifactBuilder {
    fn table(&self) -> &TableId;

    fn missing_base(&self) -> Option<&TableId>;

    fn select(&self) -> Result<SqlString, UnbuildableArtifact>;

    fn create<W: Warehouse>(&self, warehouse: &W) -> Result<Vec<SqlString>, UnbuildableArtifact>;

    fn build_log(&self, run: &Run, outcome: Outcome) -> BuildLog;
}
