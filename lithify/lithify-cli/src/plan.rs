use chrono::{DateTime, Utc};
use duckdb::types::FromSql;
use google_cloud_bigquery::error::RowError;
use google_cloud_bigquery::query::Row;

use lithify_config::infra::Sink;
use lithify_core::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use lithify_core::era::EraTracker;
use lithify_core::model::TableId;
use lithify_core::position::RecordedAt;
use lithify_core::sink::TablePath;
use lithify_core::sink::WarehouseClient;
use lithify_core::source::{ChangeLogs, SchemaChangeLog, SchemaChangeStream};
use lithify_core::sql::SqlString;
use lithify_core::window::Window;
use lithify_protocol::sink::bigquery::client::{BigQueryChangeLogsFn, BigQueryClient};
use lithify_protocol::sink::duckdb::client::{DuckDbChangeLogsFn, DuckDbClient};
use lithify_strategy::snapshot::Snapshot;

use crate::artifacts::Artifacts;
use crate::pipeline::Warehouse;
use crate::unrunnable::Unrunnable;

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

    fn statements<C>(&self, logs: C) -> Result<Vec<SqlString>, Unrunnable>
    where
        C: ChangeLogs,
        K: SchemaChangeStream<C::Schema> + WarehouseClient,
        <K as SchemaChangeStream<C::Schema>>::Error: Into<Unrunnable>,
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
        let catalog = self.client.replay::<Unrunnable, _>(
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
    type Output = Result<Vec<SqlString>, Unrunnable>;

    fn call_duckdb<C>(self, logs: C) -> Self::Output
    where
        C: ChangeLogs,
        <C::Schema as SchemaChangeLog>::Record: FromSql,
    {
        self.statements(logs)
    }
}

impl BigQueryChangeLogsFn for SnapshotPlan<'_, BigQueryClient> {
    type Output = Result<Vec<SqlString>, Unrunnable>;

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

    use lithify_config::Pipeline;
    use lithify_core::position::RecordedAt;
    use lithify_core::sink::WarehouseClient;
    use lithify_protocol::DuckDbWarehouse;
    use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbClient};

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
        Pipeline::open(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql"))
            .unwrap()
    }

    #[test]
    fn a_plan_holds_the_snapshot_of_every_table_the_schema_changes_left_standing() {
        let pipeline = pipeline();
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "rustfs")
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
            .find(|sink| sink.name() == "rustfs")
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
