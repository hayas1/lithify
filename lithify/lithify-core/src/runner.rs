use std::convert::Infallible;

use crate::artifact::Artifact;
use crate::build::{TableBuild, TableBuilder};
use crate::listener::{Finish, OutOfOrder};
use crate::model::TableId;
use crate::sink::{Namespace, TablePath, Warehouse};
use crate::source::{RowChangeLog, SchemaHistory};
use crate::table_listeners::TableListeners;
use crate::window::Window;

pub struct Replayer<'a, W, L> {
    warehouse: &'a W,
    log: &'a L,
}

impl<'a, W, L> Replayer<'a, W, L>
where
    W: Warehouse,
    L: RowChangeLog,
{
    pub fn new(warehouse: &'a W, log: &'a L) -> Self {
        Self { warehouse, log }
    }

    pub fn planned<A: Artifact, E>(
        &self,
        history: &SchemaHistory<L::Position>,
        artifact: &'a A,
        namespace: &'a Namespace,
        window: &'a Window,
        base: impl Fn(&TableId) -> Option<TablePath>,
        finishes: impl Fn(&TableId) -> bool,
    ) -> Result<Vec<TableBuild>, E>
    where
        E: From<OutOfOrder> + From<Infallible>,
    {
        let mut listeners = TableListeners::new(
            |table: &TableId, encounter| {
                TableBuilder::opening::<W>(
                    artifact,
                    self.log,
                    namespace,
                    window,
                    table,
                    encounter,
                    base(table),
                )
            },
            finishes,
        );
        let catalog = history.replay::<E, _>(self.warehouse, &mut listeners)?;

        Ok(listeners.finish(self.warehouse, catalog)?)
    }
}
