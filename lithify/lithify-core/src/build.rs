use std::convert::Infallible;

use crate::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use crate::event::SchemaChange;
use crate::listener::{
    Finish, ReplayEnd, SchemaChangeListener, TableAltered, TableCreated, TableDropped, TableRenamed,
};
use crate::model::TableId;
use crate::sink::{Namespace, TablePath, Warehouse, WarehouseSql};
use crate::source::RowChangeLog;
use crate::sql::SqlString;
use crate::table_listeners::Encounter;
use crate::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Base {
    Built(TablePath),
    Missing,
    ChangeLog,
}

impl Base {
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

// One table as an artifact builds it: what every artifact records of the table alike, the name it
// opened the window under and what it is built onto, around the fold the artifact keeps of it.
pub struct TableBuilder<'a, A, B> {
    artifact: &'a A,
    namespace: &'a Namespace,
    window: &'a Window,
    table_from: Option<TableId>,
    base: Base,
    builder: B,
}

impl<'a, A, B> TableBuilder<'a, A, B> {
    pub fn opening<L, W>(
        artifact: &'a A,
        log: &'a L,
        namespace: &'a Namespace,
        window: &'a Window,
        table: &TableId,
        encounter: Encounter,
        built: Option<TablePath>,
    ) -> Self
    where
        A: Artifact<Builder<'a, L, W> = B>,
        L: RowChangeLog + 'a,
        W: Warehouse + 'a,
    {
        let base = encounter.base(built);

        Self {
            artifact,
            namespace,
            window,
            table_from: encounter.table_from(table),
            builder: artifact.builder::<L, W>(log, window, base.clone()),
            base,
        }
    }
}

impl<P, A, B> SchemaChangeListener<P> for TableBuilder<'_, A, B>
where
    B: SchemaChangeListener<P, Error = UnbuildableArtifact>,
{
    type Context = B::Context;
    type Error = UnbuildableArtifact;

    fn visit_statement(
        &mut self,
        context: &B::Context,
        change: &SchemaChange<P>,
    ) -> Result<(), UnbuildableArtifact> {
        self.builder.visit_statement(context, change)
    }

    fn leave_statement(
        &mut self,
        context: &B::Context,
        change: &SchemaChange<P>,
    ) -> Result<(), UnbuildableArtifact> {
        self.builder.leave_statement(context, change)
    }

    fn table_created(
        &mut self,
        context: &B::Context,
        event: &TableCreated<'_, P>,
    ) -> Result<(), UnbuildableArtifact> {
        self.builder.table_created(context, event)
    }

    fn table_dropped(
        &mut self,
        context: &B::Context,
        event: &TableDropped<'_, P>,
    ) -> Result<(), UnbuildableArtifact> {
        self.builder.table_dropped(context, event)
    }

    fn table_renamed(
        &mut self,
        context: &B::Context,
        event: &TableRenamed<'_, P>,
    ) -> Result<(), UnbuildableArtifact> {
        self.builder.table_renamed(context, event)
    }

    fn table_altered(
        &mut self,
        context: &B::Context,
        event: &TableAltered<'_, P>,
    ) -> Result<(), UnbuildableArtifact> {
        self.builder.table_altered(context, event)
    }
}

impl<A: Artifact, B> TableBuilder<'_, A, B> {
    fn made<P>(
        &self,
        warehouse: &B::Context,
        end: &ReplayEnd<UnbuildableArtifact>,
        target: &TablePath,
    ) -> Result<Vec<SqlString>, UnbuildableArtifact>
    where
        B: ArtifactBuilder<P>,
        B::Context: Warehouse,
    {
        Ok([warehouse.create_namespace(target.namespace())]
            .into_iter()
            .chain(self.builder.create(warehouse, end, target)?)
            .collect())
    }
}

impl<P, A, B> Finish<P> for TableBuilder<'_, A, B>
where
    A: Artifact,
    B: ArtifactBuilder<P>,
    B::Context: Warehouse,
{
    type Input = ReplayEnd<UnbuildableArtifact>;
    type Output = TableBuild;
    type FinishError = Infallible;

    fn finish(
        self,
        context: &B::Context,
        end: ReplayEnd<UnbuildableArtifact>,
    ) -> Result<TableBuild, Infallible> {
        let target = end.model().map(|table| {
            TablePath::new(
                self.namespace.clone(),
                &self.artifact.table_name(table.id(), self.window),
            )
        });
        let making = match &target {
            Some(target) => self.made(context, &end, target),
            None => end.refusal().cloned().map_or(Ok(Vec::new()), Err),
        };

        Ok(TableBuild::new(
            self.table_from,
            end.model().map(|table| table.id().clone()),
            self.base,
            target,
            self.window.clone(),
            making,
        ))
    }
}
