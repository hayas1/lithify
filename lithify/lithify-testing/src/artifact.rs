use std::marker::PhantomData;

use lithify_core::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use lithify_core::build::Base;
use lithify_core::listener::{ReplayEnd, SchemaChangeListener};
use lithify_core::model::TableId;
use lithify_core::sink::{TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
use lithify_core::sql::SqlString;
use lithify_core::window::Window;

// Builds each table it is told of under the table's own name, with one statement saying what it
// was built onto and into, so a test reads what the core planned rather than SQL.
pub struct Mirror;

impl Artifact for Mirror {
    const KIND: &'static str = "mirror";

    type From = Self;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = Mirroring<W>;

    fn table_name(&self, table: &TableId, _: &Window) -> String {
        table.table().to_owned()
    }

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        _: &'a L,
        _: &'a Window,
        base: Base,
    ) -> Mirroring<W> {
        Mirroring {
            base,
            warehouse: PhantomData,
        }
    }
}

pub struct Mirroring<W> {
    base: Base,
    warehouse: PhantomData<fn(&W)>,
}

impl<P, W> SchemaChangeListener<P> for Mirroring<W> {
    type Context = W;
    type Error = UnbuildableArtifact;
}

impl<P, W> ArtifactBuilder<P> for Mirroring<W> {
    fn select(
        &self,
        _: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
    ) -> Result<SqlString, UnbuildableArtifact> {
        let onto = match &self.base {
            Base::Built(built) => built.to_string(),
            Base::Missing => "nothing".to_owned(),
            Base::ChangeLog => "the change log".to_owned(),
        };

        Ok(SqlString::from(format!("{} onto {onto}", end.table())))
    }

    fn create(
        &self,
        warehouse: &W,
        end: &ReplayEnd<UnbuildableArtifact>,
        target: &TablePath,
    ) -> Result<Vec<SqlString>, UnbuildableArtifact> {
        if let Some(refused) = end.refusal() {
            return Err(refused.clone());
        }
        let select = <Self as ArtifactBuilder<P>>::select(self, warehouse, end)?;

        Ok(vec![SqlString::from(format!("{select} into {target}"))])
    }
}
