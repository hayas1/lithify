use chrono::{DateTime, NaiveDate, Utc};

use lithify_config::infra::{Sink, Source};
use lithify_core::sink::{Namespace, WarehouseClient};
use lithify_core::ts::Ts;
use lithify_core::window::Window;

use crate::artifacts::{ArtifactConfig, DeclaredSnapshot};
use crate::course::Itinerary;
use crate::pipeline::{CaptureConfig, WarehouseConfig};
use crate::plan::Tables;
use crate::resumption::Resumption;
use crate::unrunnable::Unrunnable;

pub struct Shard<'a> {
    sink: &'a Sink<WarehouseConfig, ArtifactConfig>,
    source: &'a Source<CaptureConfig>,
    now: DateTime<Utc>,
}

impl<'a> Shard<'a> {
    pub fn new(
        sink: &'a Sink<WarehouseConfig, ArtifactConfig>,
        source: &'a Source<CaptureConfig>,
        now: DateTime<Utc>,
    ) -> Self {
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
        let window = self.closed(date)?;
        let previous = Resumption::at(*window.from(), self.source.since())
            .previous(client, &self.namespace()?)?;

        Ok(Itinerary::new(vec![window], previous, tables.clone()))
    }

    pub fn closed(&self, date: NaiveDate) -> Result<Window, Unrunnable> {
        let window = DeclaredSnapshot::of(self.sink)?
            .schedule()
            .shard(self.source.since(), date)
            .ok_or(Unrunnable::Unsharded(date))?;

        match *window.to() > Ts::from(self.now) {
            true => Err(Unrunnable::Unclosed(window)),
            false => Ok(window),
        }
    }

    fn namespace(&self) -> Result<Namespace, Unrunnable> {
        Ok(DeclaredSnapshot::of(self.sink)?.namespace().clone())
    }
}
