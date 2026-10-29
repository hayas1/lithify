use crate::artifact::UnbuildableArtifact;
use crate::listener::Appearance;
use crate::model::TableId;
use crate::sink::TablePath;
use crate::sql::SqlString;
use crate::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Base {
    Built(TablePath),
    Missing,
    ChangeLog,
}

impl Base {
    pub fn appeared(&mut self, appearance: Appearance) {
        if appearance.is_logged_from_start() {
            *self = Self::ChangeLog;
        }
    }

    pub fn as_built(&self) -> Option<&TablePath> {
        match self {
            Self::Built(built) => Some(built),
            Self::Missing | Self::ChangeLog => None,
        }
    }
}

impl From<Option<TablePath>> for Base {
    fn from(built: Option<TablePath>) -> Self {
        built.map_or(Self::Missing, Self::Built)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableBuild {
    table_from: Option<TableId>,
    table_to: Option<TableId>,
    base: Base,
    target: Option<TablePath>,
    window: Window,
    making: Result<Vec<SqlString>, UnbuildableArtifact>,
}

impl TableBuild {
    pub fn new(
        table_from: Option<TableId>,
        table_to: Option<TableId>,
        base: Base,
        target: Option<TablePath>,
        window: Window,
        making: Result<Vec<SqlString>, UnbuildableArtifact>,
    ) -> Self {
        Self {
            table_from,
            table_to,
            base,
            target,
            window,
            making,
        }
    }

    pub fn table_from(&self) -> Option<&TableId> {
        self.table_from.as_ref()
    }

    pub fn table_to(&self) -> Option<&TableId> {
        self.table_to.as_ref()
    }

    pub fn base(&self) -> &Base {
        &self.base
    }

    pub fn target(&self) -> Option<&TablePath> {
        self.target.as_ref()
    }

    pub fn window(&self) -> &Window {
        &self.window
    }

    pub fn statements(&self) -> Result<&[SqlString], &UnbuildableArtifact> {
        self.making.as_deref()
    }

    pub fn refusal(&self) -> Option<UnbuildableArtifact> {
        match (&self.making, &self.base, &self.table_from) {
            (Err(refused), _, _) => Some(refused.clone()),
            (Ok(_), Base::Missing, Some(table)) => {
                Some(UnbuildableArtifact::WithoutBase(table.clone()))
            }
            (Ok(_), _, _) => None,
        }
    }
}
