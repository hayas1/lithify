use std::convert::Infallible;

use crate::artifact::Artifact;
use crate::build::TableBuild;
use crate::listener::{Finish, OutOfOrder};
use crate::model::TableId;
use crate::sink::{TablePath, WarehouseClient};
use crate::source::{ChangeLogs, SchemaChangeStream};
use crate::table_listeners::TableListeners;
use crate::window::Window;

pub struct Replayer<'a, K, C> {
    client: &'a K,
    logs: &'a C,
}

impl<'a, K, C> Replayer<'a, K, C>
where
    C: ChangeLogs,
    K: SchemaChangeStream<C::Schema> + WarehouseClient,
{
    pub fn new(client: &'a K, logs: &'a C) -> Self {
        Self { client, logs }
    }

    pub async fn planned<A: Artifact, E>(
        &self,
        artifact: &'a A,
        window: &'a Window,
        base: impl Fn(&TableId) -> Option<TablePath>,
        finishes: impl Fn(&TableId) -> bool,
    ) -> Result<Vec<TableBuild>, E>
    where
        E: From<OutOfOrder> + From<Infallible>,
        <K as SchemaChangeStream<C::Schema>>::Error: Into<E>,
    {
        let mut listeners = TableListeners::new(
            |table: &TableId| {
                artifact.builder::<_, K::Warehouse>(
                    self.logs.row_changes(),
                    table.clone(),
                    window,
                    base(table),
                )
            },
            finishes,
        );
        let catalog = self
            .client
            .replay::<E, _>(
                self.client.warehouse(),
                self.logs.schema_changes(),
                window,
                &mut listeners,
            )
            .await?;

        Ok(listeners.finish(self.client.warehouse(), catalog)?)
    }
}
