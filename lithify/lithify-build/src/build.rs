use std::convert::Infallible;
use std::marker::PhantomData;

use crate::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use crate::sink::{Namespace, TablePath, Warehouse};
use crate::source::RowChangeLog;
use crate::sql::SqlString;
use lithify_schema::event::SchemaChange;
use lithify_schema::listener::{
    Finish, ReplayEnd, SchemaChangeListener, TableAltered, TableCreated, TableDropped, TableRenamed,
};
use lithify_schema::model::TableId;
use lithify_schema::table_listeners::Encounter;
use lithify_schema::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Base {
    Built(TablePath),
    Missing,
    ChangeLog,
}

impl Base {
    pub fn new(encounter: &Encounter, built: Option<TablePath>) -> Self {
        match encounter.is_logged_from_start() {
            true => Self::ChangeLog,
            false => Self::from(built),
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

pub struct BuildContext<'a, W> {
    warehouse: &'a W,
    namespace: &'a Namespace,
}

impl<'a, W> BuildContext<'a, W> {
    pub fn new(warehouse: &'a W, namespace: &'a Namespace) -> Self {
        Self {
            warehouse,
            namespace,
        }
    }

    pub fn warehouse(&self) -> &'a W {
        self.warehouse
    }

    pub fn namespace(&self) -> &'a Namespace {
        self.namespace
    }
}

// One table as an artifact builds it: what every artifact records of the table alike, the name it
// opened the window under and what it is built onto, around the fold the artifact keeps of it.
pub struct TableBuilder<'a, A, L, B> {
    artifact: &'a A,
    log: PhantomData<&'a L>,
    window: &'a Window,
    table_from: Option<TableId>,
    base: Base,
    builder: B,
}

impl<'a, A, L, B> TableBuilder<'a, A, L, B> {
    pub fn opening<W>(
        artifact: &'a A,
        log: &'a L,
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
        let base = Base::new(&encounter, built);

        Self {
            artifact,
            log: PhantomData,
            window,
            table_from: encounter.table_from(table),
            builder: artifact.builder::<L, W>(log, window, base.clone()),
            base,
        }
    }
}

impl<'a, P, A, L, B> SchemaChangeListener<P> for TableBuilder<'a, A, L, B>
where
    B: SchemaChangeListener<P, Error = UnbuildableArtifact>,
    B::Context: 'a,
{
    type Context = BuildContext<'a, B::Context>;
    type Error = UnbuildableArtifact;

    fn visit_statement(
        &mut self,
        context: &BuildContext<'a, B::Context>,
        change: &SchemaChange<P>,
    ) -> Result<(), UnbuildableArtifact> {
        self.builder.visit_statement(context.warehouse(), change)
    }

    fn leave_statement(
        &mut self,
        context: &BuildContext<'a, B::Context>,
        change: &SchemaChange<P>,
    ) -> Result<(), UnbuildableArtifact> {
        self.builder.leave_statement(context.warehouse(), change)
    }

    fn table_created(
        &mut self,
        context: &BuildContext<'a, B::Context>,
        event: &TableCreated<'_, P>,
    ) -> Result<(), UnbuildableArtifact> {
        self.builder.table_created(context.warehouse(), event)
    }

    fn table_dropped(
        &mut self,
        context: &BuildContext<'a, B::Context>,
        event: &TableDropped<'_, P>,
    ) -> Result<(), UnbuildableArtifact> {
        self.builder.table_dropped(context.warehouse(), event)
    }

    fn table_renamed(
        &mut self,
        context: &BuildContext<'a, B::Context>,
        event: &TableRenamed<'_, P>,
    ) -> Result<(), UnbuildableArtifact> {
        self.builder.table_renamed(context.warehouse(), event)
    }

    fn table_altered(
        &mut self,
        context: &BuildContext<'a, B::Context>,
        event: &TableAltered<'_, P>,
    ) -> Result<(), UnbuildableArtifact> {
        self.builder.table_altered(context.warehouse(), event)
    }
}

impl<'a, A, L, W, B> TableBuilder<'a, A, L, B>
where
    A: Artifact<Builder<'a, L, W> = B>,
    L: RowChangeLog + 'a,
    W: Warehouse + 'a,
    B: ArtifactBuilder<L::Position, Context = W>,
{
    fn made(
        &self,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
        target: &TablePath,
    ) -> Result<Vec<SqlString>, UnbuildableArtifact> {
        Ok([warehouse.create_namespace(target.namespace())]
            .into_iter()
            .chain(
                self.artifact
                    .create::<L, W>(&self.builder, warehouse, end, target)?,
            )
            .collect())
    }
}

impl<'a, A, L, W, B> Finish<L::Position> for TableBuilder<'a, A, L, B>
where
    A: Artifact<Builder<'a, L, W> = B>,
    L: RowChangeLog + 'a,
    W: Warehouse + 'a,
    B: ArtifactBuilder<L::Position, Context = W>,
{
    type Input = ReplayEnd<UnbuildableArtifact>;
    type Output = TableBuild;
    type FinishError = Infallible;

    fn finish(
        self,
        context: &BuildContext<'a, W>,
        end: ReplayEnd<UnbuildableArtifact>,
    ) -> Result<TableBuild, Infallible> {
        let target = end.model().map(|table| {
            TablePath::new(
                context.namespace().clone(),
                &self.artifact.table_name(table.id(), self.window),
            )
        });
        let making = match &target {
            Some(target) => self.made(context.warehouse(), &end, target),
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

#[cfg(test)]
mod tests {
    use super::*;
    use lithify_schema::listener::Appearance;

    fn built() -> TablePath {
        TablePath::new("shelf", "people_before")
    }

    #[test]
    fn a_table_the_window_opened_with_is_built_onto_what_its_name_was_built_into() {
        assert_eq!(
            Base::new(&Encounter::Opened, Some(built())),
            Base::Built(built())
        );
    }

    #[test]
    fn a_table_discovered_within_the_window_is_built_onto_what_its_name_was_built_into() {
        let discovered = Encounter::Appeared(Appearance::Discovered);

        assert_eq!(Base::new(&discovered, Some(built())), Base::Built(built()));
        assert_eq!(Base::new(&discovered, None), Base::Missing);
    }

    #[test]
    fn a_table_created_within_the_window_is_built_onto_the_change_log() {
        let created = Encounter::Appeared(Appearance::Created);

        assert_eq!(Base::new(&created, Some(built())), Base::ChangeLog);
    }

    #[test]
    fn a_table_captured_within_the_window_is_built_onto_the_change_log() {
        let captured = Encounter::Appeared(Appearance::Captured);

        assert_eq!(Base::new(&captured, Some(built())), Base::ChangeLog);
    }
}
