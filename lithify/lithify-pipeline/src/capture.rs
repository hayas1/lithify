use lithify_core::sink::WarehouseClient;
use lithify_core::source::{ChangeLogs, SchemaChangeStream};

// The client stays a parameter of the traits rather than of their methods: a capture can hand only
// a client of a warehouse it knows logs that warehouse decodes, a bound only the impl for that
// warehouse can state.
pub trait ChangeLogsFn<K: WarehouseClient> {
    type Output;

    async fn call<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        K: SchemaChangeStream<C::SchemaChangeLog, Error = <K as WarehouseClient>::Error>;
}

pub trait Capture<K: WarehouseClient> {
    async fn with_change_logs<F: ChangeLogsFn<K>>(
        &self,
        warehouse: &K::Warehouse,
        f: F,
    ) -> F::Output;
}
