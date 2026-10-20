use std::fmt;

use crate::build_log::{BuildLog, Outcome, Run, Series};
use crate::listener::SchemaChangeListener;
use crate::model::TableId;
use crate::schedule::Timeline;
use crate::sink::{TablePath, UnsupportedColumn, Warehouse};
use crate::source::RowChangeLog;
use crate::sql::SqlString;
use crate::ts::{Event, Ts};
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

    type Open;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>: ArtifactBuilder
        + SchemaChangeListener<L::Position, Context = W, Error = UnbuildableArtifact>
    where
        Self: 'a;

    fn series(&self) -> Series;

    fn window(&self, interval: &Window, until: Ts<Event>) -> Option<Window>;

    fn open(&self, window: &Window) -> Self::Open;

    fn next(
        &self,
        timeline: &Timeline,
        built: Option<&Window>,
        _base: Option<Ts<Event>>,
    ) -> Option<Window> {
        let interval = match built {
            Some(built) => timeline.after(built)?,
            None => timeline.first()?,
        };

        self.window(&interval, timeline.until())
            .filter(|window| timeline.reached(window))
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
        open: Self::Open,
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
