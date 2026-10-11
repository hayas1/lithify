use chrono::{DateTime, Utc};
use duckdb::types::FromSql;
use google_cloud_bigquery::error::RowError;
use google_cloud_bigquery::query::Row;

use event_replay_config::Artifact;
use event_replay_config::infra::{Sink, Source};
use event_replay_core::artifact;
use event_replay_core::era::EraTracker;
use event_replay_core::sink::TablePath;
use event_replay_core::sink::WarehouseClient;
use event_replay_core::source::{ChangeLogs, SchemaChangeLog, SchemaChangeStream};
use event_replay_core::window::Window;
use event_replay_protocol::sink::bigquery::client::{BigQueryChangeLogsFn, BigQueryClient};
use event_replay_protocol::sink::duckdb::client::{DuckDbChangeLogsFn, DuckDbClient};
use event_replay_strategy::snapshot::{Base, Snapshot};

use crate::pipeline::{Capture, Warehouse};
use crate::unreplayable::Unreplayable;

pub struct SnapshotPlan<'a, K> {
    client: &'a K,
    sink: &'a Sink<Warehouse>,
    source: &'a Source<Capture>,
    schema_changes: &'a TablePath,
    window: &'a Window,
}

impl<'a, K> SnapshotPlan<'a, K> {
    pub fn new(
        client: &'a K,
        sink: &'a Sink<Warehouse>,
        source: &'a Source<Capture>,
        schema_changes: &'a TablePath,
        window: &'a Window,
    ) -> Self {
        Self {
            client,
            sink,
            source,
            schema_changes,
            window,
        }
    }

    fn statements<C>(&self, logs: C) -> Result<Vec<String>, Unreplayable>
    where
        C: ChangeLogs,
        K: SchemaChangeStream<C::Schema> + WarehouseClient,
        Unreplayable: From<<K as SchemaChangeStream<C::Schema>>::Error>,
    {
        let shard =
            DateTime::<Utc>::try_from(*self.window.from()).map_err(Unreplayable::Unshardable)?;
        let mut tracker = EraTracker::new(self.window.clone());
        let catalog = self.client.replay::<Unreplayable>(
            logs.schema_changes(),
            self.schema_changes,
            self.window,
            &mut tracker,
        )?;
        let snapshots = tracker
            .lineages(&catalog)
            .into_iter()
            .map(|lineage| {
                let table = lineage.table().id().clone();
                let changes = self
                    .sink
                    .row_changes(self.source, &table)
                    .ok_or_else(|| Unreplayable::WithoutTopic(self.source.name().to_owned()))?;

                Ok(Snapshot::new(
                    logs.row_changes(),
                    lineage,
                    changes,
                    self.sink.replay().target(Artifact::Snapshot, &table, shard),
                    Base::Captured,
                ))
            })
            .collect::<Result<Vec<_>, Unreplayable>>()?;

        Ok(artifact::Artifact::statements(
            &snapshots,
            self.client.warehouse(),
        )?)
    }
}

impl DuckDbChangeLogsFn for SnapshotPlan<'_, DuckDbClient> {
    type Output = Result<Vec<String>, Unreplayable>;

    fn call_duckdb<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: FromSql,
    {
        self.statements(logs)
    }
}

impl BigQueryChangeLogsFn for SnapshotPlan<'_, BigQueryClient> {
    type Output = Result<Vec<String>, Unreplayable>;

    fn call_bigquery<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: TryFrom<Row, Error = RowError>,
    {
        self.statements(logs)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use duckdb::Connection;

    use event_replay_config::Pipeline;
    use event_replay_core::position::RecordedAt;
    use event_replay_core::sink::WarehouseClient;
    use event_replay_protocol::DuckDbWarehouse;
    use event_replay_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbClient};

    use super::*;

    const CHANGE_LOGS: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value)";

    fn pipeline() -> Pipeline<Capture, Warehouse> {
        Pipeline::open(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/event-replay/mysql"),
        )
        .unwrap()
    }

    #[test]
    fn a_plan_holds_the_snapshot_of_every_table_the_schema_changes_left_standing() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "minio")
            .unwrap();
        let schema_changes = sink.schema_changes(source).unwrap();
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::default(),
        );
        client.execute(CHANGE_LOGS).unwrap();
        let window = Window::new(
            RecordedAt::from(1789862400000),
            RecordedAt::from(1789948800000),
        );

        let statements = source
            .capture()
            .with_duckdb_change_logs(SnapshotPlan::new(
                &client,
                sink,
                source,
                &schema_changes,
                &window,
            ))
            .unwrap();

        assert_eq!(statements[0], "CREATE SCHEMA IF NOT EXISTS \"mysql_shop\"");
        assert!(
            statements[1]
                .starts_with("CREATE OR REPLACE TABLE \"mysql_shop\".\"people_20260920\" AS")
        );
        assert!(statements[1].contains("FROM \"mysql_shop_cdc\".\"mysql.shop.people\""));
        assert_eq!(statements.len(), 2);
    }
}
