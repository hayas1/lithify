use std::convert::Infallible;

use crate::artifact::Artifact;
use crate::build::TableBuild;
use crate::listener::{Finish, OutOfOrder};
use crate::model::TableId;
use crate::sink::{TablePath, Warehouse};
use crate::source::{ChangeLogs, SchemaHistory};
use crate::table_listeners::TableListeners;
use crate::window::Window;

pub struct Replayer<'a, W, C> {
    warehouse: &'a W,
    logs: &'a C,
}

impl<'a, W, C> Replayer<'a, W, C>
where
    W: Warehouse,
    C: ChangeLogs,
{
    pub fn new(warehouse: &'a W, logs: &'a C) -> Self {
        Self { warehouse, logs }
    }

    pub fn planned<A: Artifact, E>(
        &self,
        history: &SchemaHistory<C::Position>,
        artifact: &'a A,
        window: &'a Window,
        base: impl Fn(&TableId) -> Option<TablePath>,
        finishes: impl Fn(&TableId) -> bool,
    ) -> Result<Vec<TableBuild>, E>
    where
        E: From<OutOfOrder> + From<Infallible>,
    {
        let mut listeners = TableListeners::new(
            |table: &TableId| {
                artifact.builder::<_, W>(
                    self.logs.row_changes(),
                    table.clone(),
                    window,
                    base(table),
                )
            },
            finishes,
        );
        let catalog = history.replay::<E, _>(self.warehouse, &mut listeners)?;

        Ok(listeners.finish(self.warehouse, catalog)?)
    }
}
