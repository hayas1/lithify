use chrono::{DateTime, NaiveDate, Utc};

use lithify_config::ArtifactKind;
use lithify_config::infra::{Sink, Source};
use lithify_core::position::RecordedAt;
use lithify_core::sink::WarehouseClient;

use crate::course::Itinerary;
use crate::pipeline::{Capture, Warehouse};
use crate::plan::Tables;
use crate::resumption::Resumption;
use crate::unrunnable::Unrunnable;

pub struct Shard<'a> {
    sink: &'a Sink<Warehouse>,
    source: &'a Source<Capture>,
    now: DateTime<Utc>,
}

impl<'a> Shard<'a> {
    pub fn new(sink: &'a Sink<Warehouse>, source: &'a Source<Capture>, now: DateTime<Utc>) -> Self {
        Self { sink, source, now }
    }

    pub fn itinerary<K>(
        &self,
        client: &K,
        date: NaiveDate,
        tables: &Tables,
    ) -> Result<Itinerary, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        let window = self
            .sink
            .artifacts()
            .schedule()
            .shard(self.source.since(), date)
            .ok_or(Unrunnable::Unsharded(date))?;
        if *window.to() > RecordedAt::from(self.now) {
            return Err(Unrunnable::Unclosed(window));
        }
        let previous = Resumption::at(*window.from(), self.source.since())
            .previous(client, self.namespace())?;

        Ok(Itinerary::new(vec![window], previous, tables.clone()))
    }

    fn namespace(&self) -> &str {
        self.sink.artifacts().namespace(ArtifactKind::Snapshot)
    }
}
