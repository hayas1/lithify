use chrono::{DateTime, Utc};
use duckdb::types::FromSql;
use google_cloud_bigquery::error::RowError;
use google_cloud_bigquery::query::Row;

use event_replay_config::infra::Sink;
use event_replay_core::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use event_replay_core::era::EraTracker;
use event_replay_core::model::TableId;
use event_replay_core::position::RecordedAt;
use event_replay_core::sink::TablePath;
use event_replay_core::sink::WarehouseClient;
use event_replay_core::source::{ChangeLogs, SchemaChangeLog, SchemaChangeStream};
use event_replay_core::sql::SqlString;
use event_replay_core::window::Window;
use event_replay_protocol::sink::bigquery::client::{BigQueryChangeLogsFn, BigQueryClient};
use event_replay_protocol::sink::duckdb::client::{DuckDbChangeLogsFn, DuckDbClient};
use event_replay_strategy::snapshot::Snapshot;

use crate::artifacts::Artifacts;
use crate::pipeline::Warehouse;
use crate::unreplayable::Unreplayable;

pub struct SnapshotPlan<'a, K> {
    client: &'a K,
    sink: &'a Sink<Warehouse>,
    window: &'a Window,
    onto: Option<DateTime<Utc>>,
}

impl<'a, K> SnapshotPlan<'a, K> {
    pub fn new(
        client: &'a K,
        sink: &'a Sink<Warehouse>,
        window: &'a Window,
        onto: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            client,
            sink,
            window,
            onto,
        }
    }

    fn base(
        &self,
        snapshot: &Snapshot,
        table: &TableId,
    ) -> Result<Option<TablePath>, UnbuildableArtifact> {
        self.onto
            .map(|onto| {
                snapshot.target(
                    table,
                    &Window::new(RecordedAt::from(onto), *self.window.from()),
                )
            })
            .transpose()
    }

    fn statements<C>(&self, logs: C) -> Result<Vec<SqlString>, Unreplayable>
    where
        C: ChangeLogs,
        K: SchemaChangeStream<C::Schema> + WarehouseClient,
        <K as SchemaChangeStream<C::Schema>>::Error: Into<Unreplayable>,
    {
        let snapshot = self.sink.snapshot();
        let mut tracker = EraTracker::new(self.window.clone(), |table: &TableId| {
            Ok::<_, UnbuildableArtifact>(snapshot.builder::<_, K::Warehouse>(
                logs.row_changes(),
                table.clone(),
                self.window,
                self.base(&snapshot, table)?,
            ))
        });
        let catalog = self.client.replay::<Unreplayable, _>(
            self.client.warehouse(),
            logs.schema_changes(),
            self.window,
            &mut tracker,
        )?;
        let builders = tracker.finish(self.client.warehouse(), &catalog)?;

        Ok(builders
            .iter()
            .map(ArtifactBuilder::create)
            .collect::<Result<Vec<_>, _>>()?
            .concat())
    }
}

impl DuckDbChangeLogsFn for SnapshotPlan<'_, DuckDbClient> {
    type Output = Result<Vec<SqlString>, Unreplayable>;

    fn call_duckdb<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: FromSql,
    {
        self.statements(logs)
    }
}

impl BigQueryChangeLogsFn for SnapshotPlan<'_, BigQueryClient> {
    type Output = Result<Vec<SqlString>, Unreplayable>;

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
    use crate::pipeline::Capture;

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
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new(sink.change_log()),
        );
        client.execute(&SqlString::from(CHANGE_LOGS)).unwrap();
        let window = Window::new(
            RecordedAt::from(1789862400000),
            RecordedAt::from(1789948800000),
        );

        let statements = source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                SnapshotPlan::new(&client, sink, &window, None),
            )
            .unwrap();

        assert_eq!(statements[0], "CREATE SCHEMA IF NOT EXISTS \"mysql_shop\"");
        assert!(
            statements[1]
                .as_str()
                .starts_with("CREATE OR REPLACE TABLE \"mysql_shop\".\"people_20260920\" AS")
        );
        assert!(
            statements[1]
                .as_str()
                .contains("FROM \"mysql_shop_cdc\".\"mysql.shop.people\"")
        );
        assert_eq!(statements.len(), 2);
    }

    #[test]
    fn a_plan_onto_an_earlier_window_folds_onto_the_snapshot_it_left() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "minio")
            .unwrap();
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new(sink.change_log()),
        );
        client.execute(&SqlString::from(CHANGE_LOGS)).unwrap();
        let window = Window::new(
            RecordedAt::from(1789862400000),
            RecordedAt::from(1789948800000),
        );
        let onto = DateTime::parse_from_rfc3339("2026-09-19T00:00:00Z")
            .unwrap()
            .to_utc();

        let statements = source
            .capture()
            .with_duckdb_change_logs(
                source.topic_prefix().unwrap(),
                SnapshotPlan::new(&client, sink, &window, Some(onto)),
            )
            .unwrap();

        assert!(
            statements[1]
                .as_str()
                .contains("  FROM \"mysql_shop\".\"people_20260919\"\n")
        );
    }
}
