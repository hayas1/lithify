use chrono::{DateTime, NaiveDate, NaiveTime, Utc};

use lithify_config::infra::{Sink, Source};
use lithify_core::runner::Planner;
use lithify_core::schedule::Timeline;
use lithify_core::stepping::Stepping;
use lithify_core::ts::Ts;

use crate::artifacts::{ArtifactConfig, Declared, DeclaredSnapshot, Lineup};
use crate::journal::Journal;
use crate::pipeline::{CaptureConfig, WarehouseConfig};
use crate::report::Report;
use crate::unrunnable::Unrunnable;

pub enum RunCommand {
    CatchUp {
        target: Target,
        to: Option<NaiveDate>,
    },
    Build(Target),
}

impl RunCommand {
    pub fn settle<P, R>(
        &self,
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        source: &Source<CaptureConfig>,
        now: DateTime<Utc>,
        planner: &P,
        runner: &R,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable>
    where
        P: Planner<Error = Unrunnable>,
        R: Report,
    {
        let lineup = Lineup::of(sink)?;
        let stepping = Stepping::new(
            planner,
            Timeline::new(
                DeclaredSnapshot::of(sink)?.schedule(),
                source.since()..Ts::from(now),
            ),
            source.capture().to_json()?,
        );

        match self {
            Self::CatchUp { target, to: None } => {
                lineup.catch_up(&stepping, target.named(), runner, journal)
            }
            Self::CatchUp {
                target,
                to: Some(date),
            } => lineup.catch_up(
                &stepping.up_to(*date, lineup.snapshot())?,
                target.named(),
                runner,
                journal,
            ),
            Self::Build(target) => target.build(&lineup, &stepping, runner, journal),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Target {
    artifact: Option<&'static str>,
    at: Option<NaiveTime>,
}

impl Target {
    pub fn new(artifact: Option<&'static str>, at: Option<NaiveTime>) -> Self {
        Self { artifact, at }
    }

    fn named(&self) -> impl Fn(&Declared) -> bool {
        move |declared| match self.artifact {
            Some(artifact) => declared.is(artifact, self.at),
            None => true,
        }
    }

    fn build<P, R>(
        &self,
        lineup: &Lineup,
        stepping: &Stepping<P>,
        runner: &R,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable>
    where
        P: Planner<Error = Unrunnable>,
        R: Report,
    {
        if lineup
            .step(stepping, self.named(), &Default::default(), runner, journal)?
            .is_none()
        {
            journal.note("nothing is left to build");
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use lithify_config::Pipeline;
    use lithify_core::sink::Namespace;
    use lithify_core::sql::SqlString;
    use lithify_protocol::sink::duckdb::client::DuckDbClient;

    use super::*;
    use lithify_core::ts::Ts;

    use crate::plan::Planning;
    use crate::plan::tests::{CAPTURE, PEOPLE, at, built_before, logged, pipeline, rows};
    use lithify_artifact::snapshot::Snapshot;
    use lithify_core::check::Expectation;
    use lithify_core::schedule::Schedule;
    use lithify_core::sink::WarehouseClient;

    const NOW: &str = "2026-09-22T05:00:00Z";

    const PETS_CREATED_ON_20260920: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400005::BIGINT}, 'ts_ms': 1789862400005::BIGINT,
            'ddl': 'CREATE TABLE pets (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"pets\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400001::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.pets\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400009::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400009::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 9::BIGINT}})
        ) AS t(value)";

    const PETS_DROPPED_ON_20260920: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE pets (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"pets\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400005::BIGINT}, 'ts_ms': 1789862400005::BIGINT,
            'ddl': 'DROP TABLE pets',
            'tableChanges': [{'type': 'DROP', 'id': '\"shop\".\"pets\"', 'table': NULL}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400001::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value)";

    const PETS_REPLACED_ON_20260920: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE pets (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"pets\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400005::BIGINT}, 'ts_ms': 1789862400005::BIGINT,
            'ddl': 'DROP TABLE pets',
            'tableChanges': [{'type': 'DROP', 'id': '\"shop\".\"pets\"', 'table': NULL}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 5::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400006::BIGINT}, 'ts_ms': 1789862400006::BIGINT,
            'ddl': 'CREATE TABLE pets (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"pets\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400001::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.pets\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400002::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400002::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 6::BIGINT}}),
          ({'op': 'c', 'ts_ms': 1789862400009::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 6::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400009::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 9::BIGINT}})
        ) AS t(value)";

    const PETS_DROPPED_ON_20260920_AND_CREATED_AGAIN_ON_20260921: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE pets (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"pets\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400005::BIGINT}, 'ts_ms': 1789862400005::BIGINT,
            'ddl': 'DROP TABLE pets',
            'tableChanges': [{'type': 'DROP', 'id': '\"shop\".\"pets\"', 'table': NULL}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 5::BIGINT, 'gtid': NULL, 'ts_ms': 1789948800006::BIGINT}, 'ts_ms': 1789948800006::BIGINT,
            'ddl': 'CREATE TABLE pets (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"pets\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400001::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.pets\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400002::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400002::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 6::BIGINT}}),
          ({'op': 'c', 'ts_ms': 1789948800009::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 6::BIGINT, 'gtid': NULL, 'ts_ms': 1789948800009::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 9::BIGINT}})
        ) AS t(value)";

    const PETS_REKEYED_ON_20260920: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE pets (id BIGINT, tag BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"pets\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
                {'name': 'tag', 'typeName': 'BIGINT', 'position': 2, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400005::BIGINT}, 'ts_ms': 1789862400005::BIGINT,
            'ddl': 'ALTER TABLE pets DROP PRIMARY KEY, ADD PRIMARY KEY (tag)',
            'tableChanges': [{'type': 'ALTER', 'id': '\"shop\".\"pets\"',
              'table': {'primaryKeyColumnNames': ['tag'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL},
                {'name': 'tag', 'typeName': 'BIGINT', 'position': 2, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400001::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.pets\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400009::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 5::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400009::BIGINT},
            'before': NULL::STRUCT(id BIGINT, tag BIGINT), 'after': {'id': 9::BIGINT, 'tag': 1::BIGINT}})
        ) AS t(value)";

    const PEOPLE_RENAMED_ON_20260920: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400005::BIGINT}, 'ts_ms': 1789862400005::BIGINT,
            'ddl': 'RENAME TABLE people TO persons',
            'tableChanges': [{'type': 'ALTER', 'id': '\"shop\".\"persons\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400001::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.persons\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789948800009::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 5::BIGINT, 'gtid': NULL, 'ts_ms': 1789948800009::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 9::BIGINT}})
        ) AS t(value)";

    fn up_to(date: &str) -> RunCommand {
        RunCommand::CatchUp {
            target: Target::default(),
            to: Some(date.parse().unwrap()),
        }
    }

    pub(crate) fn settle(
        pipeline: &Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
        client: &DuckDbClient,
        command: &RunCommand,
        now: &str,
    ) -> Result<Journal, Unrunnable> {
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let runner = DeclaredSnapshot::of(sink)
            .unwrap()
            .build(source, at(now))
            .unwrap();
        let mut journal = Journal::default();

        command.settle(
            sink,
            source,
            at(now),
            &Planning::new(
                client,
                source.capture(),
                source.topic_prefix().unwrap(),
                pipeline.tables(),
            ),
            &runner,
            &mut journal,
        )?;

        Ok(journal)
    }

    fn built(client: &DuckDbClient) -> Vec<(String, String)> {
        rows(
            client,
            "SELECT target, COALESCE(base, '') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id",
        )
    }

    fn folded(target: &str, base: &str) -> (String, String) {
        (target.to_owned(), base.to_owned())
    }

    #[test]
    fn a_shard_folds_onto_the_one_the_build_log_recorded_closing_at_its_opening() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(&pipeline, &client, &up_to("2026-09-20"), NOW).unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
            ]
        );
    }

    #[test]
    fn a_run_logs_the_instant_it_took_for_now_and_is_named_after_it() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            NOW,
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT DISTINCT strftime(started_at, '%Y-%m-%dT%H:%M:%SZ'), run_id FROM mysql_shop._lithify_log"
            )
            .into_iter()
            .map(|(started_at, run_id)| (
                started_at,
                ulid::Ulid::from_string(&run_id).unwrap().timestamp_ms()
            ))
            .collect::<Vec<_>>(),
            [(NOW.to_owned(), 1790053200000)]
        );
    }

    const PERSON_PROCESSED_PAST_THE_MIDNIGHT_ITS_EVENT_TIME_FALLS_BEFORE: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789948800001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789948799999::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value)";

    const PERSON_DELETED_AT_AN_EVENT_TIME_BEFORE_ITS_CREATION: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789948799100::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789948799000::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}}),
          ({'op': 'd', 'ts_ms': 1789948799200::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 1789948798000::BIGINT},
            'before': {'id': 8::BIGINT}, 'after': NULL::STRUCT(id BIGINT)})
        ) AS t(value)";

    #[test]
    fn a_change_is_built_into_the_shard_of_the_day_its_event_time_falls_on() {
        let pipeline = pipeline();
        let client = logged(
            PERSON_PROCESSED_PAST_THE_MIDNIGHT_ITS_EVENT_TIME_FALLS_BEFORE,
            &[],
        );
        let to = RunCommand::CatchUp {
            target: Target::default(),
            to: Some("2026-09-20".parse().unwrap()),
        };

        settle(&pipeline, &client, &to, NOW).unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260920"
            ),
            [("8".to_owned(), "kept".to_owned())]
        );
    }

    #[test]
    fn changes_within_a_shard_are_folded_in_the_order_of_their_processing_time() {
        let pipeline = pipeline();
        let client = logged(PERSON_DELETED_AT_AN_EVENT_TIME_BEFORE_ITS_CREATION, &[]);
        let to = RunCommand::CatchUp {
            target: Target::default(),
            to: Some("2026-09-20".parse().unwrap()),
        };

        settle(&pipeline, &client, &to, NOW).unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260920"
            ),
            []
        );
    }

    #[test]
    fn the_shard_the_change_log_opens_in_is_built_from_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        settle(&pipeline, &client, &up_to("1970-01-01"), NOW).unwrap();

        assert_eq!(built(&client), [folded("mysql_shop.people_19700101", "")]);
    }

    #[test]
    fn the_shard_the_change_log_opens_in_is_built_before_anything_was_logged() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);
        client
            .execute(&SqlString::from("DROP TABLE mysql_shop._lithify_log"))
            .unwrap();

        settle(&pipeline, &client, &up_to("1970-01-01"), NOW).unwrap();

        assert_eq!(built(&client), [folded("mysql_shop.people_19700101", "")]);
    }

    fn next(artifact: Option<&'static str>) -> RunCommand {
        RunCommand::Build(Target::new(artifact, None))
    }

    #[test]
    fn a_build_named_nothing_takes_the_one_step_the_build_log_says_is_next() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(&pipeline, &client, &next(None), NOW).unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
            ]
        );
    }

    #[test]
    fn a_build_named_an_artifact_takes_the_next_step_of_that_artifact() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);
        let pipeline = crate::plan::tests::opened(
            "next-scd2",
            &format!(
                "[artifact.scd2]\nshard = \"%Y%m%d\"\n{}place.scd2 = [{{ prefix = \"mysql_shop_scd2\" }}]\n",
                crate::plan::tests::INFRA
            ),
            "[default]\nartifacts = [\"snapshot\", \"scd2\"]\n",
        );

        settle(&pipeline, &client, &next(Some("scd2")), NOW).unwrap();

        assert_eq!(built(&client), [folded("mysql_shop.people_20260919", "")]);
        assert_eq!(
            rows(
                &client,
                "SELECT target, '' FROM mysql_shop_scd2._lithify_log WHERE status = 'succeeded'"
            ),
            [(
                "mysql_shop_scd2.people_open_20260921".to_owned(),
                String::new()
            )]
        );
    }

    const PETS_STANDING_BEFORE_THEY_WERE_BUILT: &str = "CREATE SCHEMA mysql_shop_cdc;
        CREATE TABLE mysql_shop_cdc.mysql AS SELECT * FROM (VALUES
          ({'source': {'file': 'binlog.000001', 'pos': 1::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE people (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"people\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]}),
          ({'source': {'file': 'binlog.000001', 'pos': 2::BIGINT, 'gtid': NULL, 'ts_ms': 0::BIGINT}, 'ts_ms': 0::BIGINT,
            'ddl': 'CREATE TABLE pets (id BIGINT)',
            'tableChanges': [{'type': 'CREATE', 'id': '\"shop\".\"pets\"',
              'table': {'primaryKeyColumnNames': ['id'], 'columns': [
                {'name': 'id', 'typeName': 'BIGINT', 'position': 1, 'optional': false,
                 'defaultValueExpression': NULL, 'length': NULL, 'scale': NULL}]}}]})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1789862400001::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 3::BIGINT, 'gtid': NULL, 'ts_ms': 1789862400001::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 8::BIGINT}})
        ) AS t(value);
        CREATE TABLE mysql_shop_cdc.\"mysql.shop.pets\" AS SELECT * FROM (VALUES
          ({'op': 'c', 'ts_ms': 1000::BIGINT,
            'source': {'file': 'binlog.000001', 'pos': 4::BIGINT, 'gtid': NULL, 'ts_ms': 1000::BIGINT},
            'before': NULL::STRUCT(id BIGINT), 'after': {'id': 9::BIGINT}})
        ) AS t(value)";

    #[test]
    fn a_catch_up_builds_a_table_with_nothing_to_fold_onto_again_from_the_start_of_its_source() {
        let pipeline = pipeline();
        let client = logged(
            PETS_STANDING_BEFORE_THEY_WERE_BUILT,
            &[built_before("people", CAPTURE)],
        );

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            NOW,
        )
        .unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
                folded("mysql_shop.pets_20260921", ""),
                folded("mysql_shop.people_20260921", "mysql_shop.people_20260920"),
            ]
        );
        assert_eq!(ids(&client, "pets_20260921"), ["9"]);
    }

    fn ids(client: &DuckDbClient, table: &str) -> Vec<String> {
        rows(
            client,
            &format!("SELECT id::VARCHAR, '' FROM mysql_shop.{table} ORDER BY id"),
        )
        .into_iter()
        .map(|(id, _)| id)
        .collect()
    }

    #[test]
    fn a_table_created_within_a_shard_is_built_from_nothing_beside_the_ones_folded() {
        let pipeline = pipeline();
        let client = logged(PETS_CREATED_ON_20260920, &[built_before("people", CAPTURE)]);

        settle(&pipeline, &client, &up_to("2026-09-20"), NOW).unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
                folded("mysql_shop.pets_20260920", ""),
            ]
        );
    }

    #[test]
    fn a_table_renamed_within_a_window_folds_the_next_window_onto_what_it_was_built_into() {
        let pipeline = pipeline();
        let client = logged(
            PEOPLE_RENAMED_ON_20260920,
            &[built_before("people", CAPTURE)],
        );

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            NOW,
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT concat_ws(' ', COALESCE(table_from, '-'), COALESCE(table_to, '-'), COALESCE(lifecycle, '-')), concat_ws(' <- ', target, COALESCE(base, '-')) FROM mysql_shop._lithify_log ORDER BY id"
            ),
            [
                folded(
                    "shop.people shop.people -",
                    "mysql_shop.people_20260919 <- -"
                ),
                folded(
                    "shop.people shop.persons renamed",
                    "mysql_shop.persons_20260920 <- mysql_shop.people_20260919"
                ),
                folded(
                    "shop.persons shop.persons -",
                    "mysql_shop.persons_20260921 <- mysql_shop.persons_20260920"
                ),
            ]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.persons_20260921 ORDER BY id"
            ),
            [
                ("7".to_owned(), "kept".to_owned()),
                ("8".to_owned(), "kept".to_owned()),
                ("9".to_owned(), "kept".to_owned())
            ]
        );
    }

    #[test]
    fn a_table_dropped_and_created_again_within_a_window_is_built_anew_and_folded_onto_after() {
        let client = logged(
            PETS_REPLACED_ON_20260920,
            &[
                built_before("people", CAPTURE),
                built_before("pets", CAPTURE),
            ],
        );

        settle(
            &pipeline(),
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            NOW,
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT concat_ws(' ', COALESCE(table_from, '-'), COALESCE(table_to, '-'), COALESCE(lifecycle, '-')), concat_ws(' <- ', COALESCE(target, '-'), COALESCE(base, '-')) FROM mysql_shop._lithify_log WHERE window_from >= '2026-09-20' AND table_from IS DISTINCT FROM 'shop.people' ORDER BY id"
            ),
            [
                folded("shop.pets - dropped", "- <- mysql_shop.pets_20260919"),
                folded("- shop.pets created", "mysql_shop.pets_20260920 <- -"),
                folded(
                    "shop.pets shop.pets -",
                    "mysql_shop.pets_20260921 <- mysql_shop.pets_20260920"
                ),
            ]
        );
        assert_eq!(ids(&client, "pets_20260920"), ["9"]);
        assert_eq!(ids(&client, "pets_20260921"), ["9"]);
    }

    #[test]
    fn a_table_created_again_in_a_later_window_is_built_anew_rather_than_onto_what_was_dropped() {
        let client = logged(
            PETS_DROPPED_ON_20260920_AND_CREATED_AGAIN_ON_20260921,
            &[
                built_before("people", CAPTURE),
                built_before("pets", CAPTURE),
            ],
        );

        settle(
            &pipeline(),
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            NOW,
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT concat_ws(' ', COALESCE(table_from, '-'), COALESCE(table_to, '-'), COALESCE(lifecycle, '-')), concat_ws(' <- ', COALESCE(target, '-'), COALESCE(base, '-')) FROM mysql_shop._lithify_log WHERE window_from >= '2026-09-20' AND table_from IS DISTINCT FROM 'shop.people' ORDER BY id"
            ),
            [
                folded("shop.pets - dropped", "- <- mysql_shop.pets_20260919"),
                folded("- shop.pets created", "mysql_shop.pets_20260921 <- -"),
            ]
        );
        assert_eq!(ids(&client, "pets_20260921"), ["9"]);
    }

    #[test]
    fn a_table_refusing_a_schema_change_within_a_window_is_logged_as_failed_while_the_others_advance()
     {
        let client = logged(
            PETS_REKEYED_ON_20260920,
            &[
                built_before("people", CAPTURE),
                built_before("pets", CAPTURE),
            ],
        );

        settle(
            &pipeline(),
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            NOW,
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, concat_ws(': ', status, error) FROM mysql_shop._lithify_log WHERE window_from >= '2026-09-20' ORDER BY id"
            ),
            [
                folded("mysql_shop.people_20260920", "succeeded"),
                folded(
                    "mysql_shop.pets_20260920",
                    "failed: shop.pets changed its primary key within the window"
                ),
                folded("mysql_shop.people_20260921", "succeeded"),
            ]
        );
    }

    #[test]
    fn a_table_dropped_within_a_window_is_logged_as_dropped_and_built_no_more() {
        let pipeline = pipeline();
        let client = logged(
            PETS_DROPPED_ON_20260920,
            &[
                built_before("people", CAPTURE),
                built_before("pets", CAPTURE),
            ],
        );

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            NOW,
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT concat_ws(' ', COALESCE(table_from, '-'), COALESCE(table_to, '-'), COALESCE(lifecycle, '-')), COALESCE(target, '-') FROM mysql_shop._lithify_log WHERE window_from >= '2026-09-20' ORDER BY id"
            ),
            [
                folded("shop.pets - dropped", "-"),
                folded("shop.people shop.people -", "mysql_shop.people_20260920"),
                folded("shop.people shop.people -", "mysql_shop.people_20260921"),
            ]
        );
        assert_eq!(
            Expectation::new(
                Schedule::default(),
                pipeline.sources().next().unwrap().since(),
                CAPTURE,
                Ts::from(at(NOW)),
            )
            .read::<_, Unrunnable>(&client, &Namespace::from("mysql_shop"), &Snapshot::days())
            .unwrap()
            .discrepancies(),
            []
        );
    }
}

#[cfg(test)]
mod catch_up {
    use lithify_core::runner::Unsteppable;
    use lithify_protocol::sink::duckdb::client::DuckDbClient;

    use crate::command::tests::settle;
    use crate::command::{RunCommand, Target};
    use crate::plan::tests::{
        BUILT_CHANGE_LOGS, CAPTURE, PEOPLE, built_before, logged, pipeline, rows,
    };
    use crate::unrunnable::Unrunnable;

    #[test]
    fn a_pipeline_catches_up_window_by_window_folding_each_onto_the_one_before() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, COALESCE(base, '') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                ("mysql_shop.people_20260919".to_owned(), String::new()),
                (
                    "mysql_shop.people_20260920".to_owned(),
                    "mysql_shop.people_20260919".to_owned()
                ),
                (
                    "mysql_shop.people_20260921".to_owned(),
                    "mysql_shop.people_20260920".to_owned()
                ),
            ]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260921 ORDER BY id"
            ),
            [
                ("7".to_owned(), "kept".to_owned()),
                ("8".to_owned(), "kept".to_owned())
            ]
        );
    }

    #[test]
    fn a_pipeline_built_for_the_first_time_builds_only_the_shard_closing_at_the_last_midnight() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, strftime(window_from, '%Y-%m-%d') || '..' || strftime(window_to, '%Y-%m-%d') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [(
                "mysql_shop.people_20260921".to_owned(),
                "1970-01-01..2026-09-22".to_owned()
            )]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260921 ORDER BY id"
            ),
            [("8".to_owned(), "kept".to_owned())]
        );
    }

    #[test]
    fn a_pipeline_caught_up_to_a_date_builds_no_shard_dated_after_it() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, COALESCE(base, '') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                ("mysql_shop.people_20260919".to_owned(), String::new()),
                (
                    "mysql_shop.people_20260920".to_owned(),
                    "mysql_shop.people_20260919".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn a_pipeline_built_for_the_first_time_up_to_a_date_is_caught_up_from_there_after() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();
        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, strftime(window_from, '%Y-%m-%d') || '..' || strftime(window_to, '%Y-%m-%d') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                (
                    "mysql_shop.people_20260920".to_owned(),
                    "1970-01-01..2026-09-21".to_owned()
                ),
                (
                    "mysql_shop.people_20260921".to_owned(),
                    "2026-09-21..2026-09-22".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn a_pipeline_is_not_caught_up_to_a_date_whose_shard_has_not_closed() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        let unclosed = settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-22"),
            "2026-09-22T05:00:00Z",
        );

        assert!(matches!(
            unclosed,
            Err(Unrunnable::Unsteppable(Unsteppable::Unclosed(_)))
        ));
    }

    #[test]
    fn a_pipeline_is_not_caught_up_to_a_date_before_its_source_began() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        let unsharded = settle(
            &pipeline,
            &client,
            &caught_up_to("1969-12-31"),
            "2026-09-22T05:00:00Z",
        );

        assert!(matches!(
            unsharded,
            Err(Unrunnable::Unsteppable(Unsteppable::Unopened(_)))
        ));
    }

    #[test]
    fn a_pipeline_is_not_caught_up_to_a_date_the_build_log_has_already_built_past() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);
        settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        let overtaken = settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-19"),
            "2026-09-22T05:00:00Z",
        );

        assert!(matches!(
            overtaken,
            Err(Unrunnable::Unsteppable(Unsteppable::Overtaken { date, .. })) if date == "2026-09-19".parse().unwrap()
        ));
    }

    #[test]
    fn a_pipeline_caught_up_to_the_date_it_was_last_built_to_builds_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);
        settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        let journal = settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(journal.entries().count(), 0);
    }

    fn caught_up_to(date: &str) -> RunCommand {
        RunCommand::CatchUp {
            target: Target::default(),
            to: Some(date.parse().unwrap()),
        }
    }

    #[test]
    fn a_pipeline_built_for_the_first_time_before_its_first_midnight_builds_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        let journal = settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "1970-01-01T23:00:00Z",
        )
        .unwrap();

        assert_eq!(journal.entries().count(), 0);
    }

    #[test]
    fn a_pipeline_that_has_caught_up_builds_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        let journal = settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-20T23:00:00Z",
        )
        .unwrap();

        assert_eq!(journal.entries().count(), 0);
    }

    fn attempts(client: &DuckDbClient) -> Vec<(String, String)> {
        rows(
            client,
            "SELECT target, status FROM mysql_shop._lithify_log WHERE strftime(window_from, '%Y%m%d') >= '20260920' ORDER BY id",
        )
    }

    #[test]
    fn a_table_failing_in_a_window_stops_itself_alone_and_is_tried_again_by_the_next_catch_up() {
        let pipeline = pipeline();
        let client = logged(
            BUILT_CHANGE_LOGS,
            &[
                built_before("people", CAPTURE),
                built_before("pets", CAPTURE),
            ],
        );
        let attempted = |target: &str, status: &str| (target.to_owned(), status.to_owned());

        let journal = settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();
        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T06:00:00Z",
        )
        .unwrap();

        assert!(!journal.complete());
        assert_eq!(
            attempts(&client),
            [
                attempted("mysql_shop.people_20260920", "succeeded"),
                attempted("mysql_shop.pets_20260920", "failed"),
                attempted("mysql_shop.people_20260921", "succeeded"),
                attempted("mysql_shop.pets_20260920", "failed"),
            ]
        );
    }

    #[test]
    fn a_pipeline_whose_capture_changed_since_its_last_run_is_not_caught_up() {
        let pipeline = pipeline();
        let client = logged(
            PEOPLE,
            &[built_before(
                "people",
                r#"{"debezium":{"dialect":"postgres"}}"#,
            )],
        );

        let changed = settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        );

        assert!(matches!(
            changed,
            Err(Unrunnable::Unsteppable(Unsteppable::CaptureChanged { .. }))
        ));
    }
}

#[cfg(test)]
mod derived {
    use lithify_config::Pipeline;

    use crate::command::tests::settle;
    use crate::command::{RunCommand, Target};
    use crate::pipeline::CaptureConfig;
    use crate::plan::tests::{CAPTURE, INFRA, PEOPLE, built_before, logged, opened, rows};

    use super::*;

    fn derived(
        name: &str,
        declared: &str,
        placed: &str,
        listed: &str,
    ) -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        opened(name, &format!("{declared}{INFRA}{placed}"), listed)
    }

    fn cut_at_six(name: &str) -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        derived(
            name,
            "[[artifact.instant]]\nat = \"06:00\"\nshard = \"%Y%m%d%H\"\n",
            "place.instant.\"06:00\" = [{ prefix = \"mysql_shop_instant\" }]\n",
            "[default]\nartifacts = [\"snapshot\", \"instant\"]\n",
        )
    }

    #[test]
    fn a_catch_up_folds_the_change_log_after_the_last_daily_snapshot_into_the_current_state() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);
        let pipeline = derived(
            "scd1",
            "[artifact.scd1]\n",
            "place.scd1 = [{ prefix = \"mysql_shop_scd1\" }]\n",
            "[default]\nartifacts = [\"snapshot\", \"scd1\"]\n",
        );

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-21T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, base FROM mysql_shop_scd1._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [(
                "mysql_shop_scd1.people_scd_type_1".to_owned(),
                "mysql_shop.people_20260920".to_owned()
            )]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'people' FROM mysql_shop_scd1.people_scd_type_1 ORDER BY id"
            ),
            [
                ("7".to_owned(), "people".to_owned()),
                ("8".to_owned(), "people".to_owned())
            ]
        );
    }

    #[test]
    fn a_catch_up_builds_the_current_state_again_only_once_its_snapshot_has_stepped() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);
        let pipeline = derived(
            "scd1-once",
            "[artifact.scd1]\n",
            "place.scd1 = [{ prefix = \"mysql_shop_scd1\" }]\n",
            "[default]\nartifacts = [\"snapshot\", \"scd1\"]\n",
        );

        for now in [
            "2026-09-21T05:00:00Z",
            "2026-09-21T06:00:00Z",
            "2026-09-22T05:00:00Z",
        ] {
            settle(
                &pipeline,
                &client,
                &RunCommand::CatchUp {
                    target: Target::default(),
                    to: None,
                },
                now,
            )
            .unwrap();
        }

        assert_eq!(
            rows(
                &client,
                "SELECT base, '' FROM mysql_shop_scd1._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                ("mysql_shop.people_20260920".to_owned(), String::new()),
                ("mysql_shop.people_20260921".to_owned(), String::new())
            ]
        );
    }

    #[test]
    fn a_catch_up_keeps_every_version_of_each_window_it_builds() {
        let client = logged(PEOPLE, &[]);
        let pipeline = derived(
            "scd2",
            "[artifact.scd2]\nshard = \"%Y%m%d\"\n",
            "place.scd2 = [{ prefix = \"mysql_shop_scd2\" }]\n",
            "[default]\nartifacts = [\"snapshot\", \"scd2\"]\n",
        );

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, COALESCE(base, '') FROM mysql_shop_scd2._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [(
                "mysql_shop_scd2.people_open_20260921".to_owned(),
                String::new()
            )]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, _valid_from::VARCHAR FROM mysql_shop_scd2.people_open_20260921"
            ),
            [("8".to_owned(), "1789948800001".to_owned())]
        );
    }

    #[test]
    fn a_catch_up_builds_an_artifact_only_for_the_tables_that_name_it() {
        let client = logged(PEOPLE, &[]);
        let pipeline = derived(
            "scd2-unnamed",
            "[artifact.scd2]\nshard = \"%Y%m%d\"\n",
            "place.scd2 = [{ prefix = \"mysql_shop_scd2\" }]\n",
            "[default]\nartifacts = [\"snapshot\"]\n",
        );

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, '' FROM mysql_shop_scd2._lithify_log WHERE status = 'succeeded'"
            ),
            []
        );
    }

    #[test]
    fn a_catch_up_cuts_an_instant_first_in_the_last_day_its_snapshot_was_built_for_and_then_each_day_after()
     {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);
        let pipeline = cut_at_six("cut-each-day");
        let cut = |target: &str, base: &str| (target.to_owned(), base.to_owned());

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-21T07:00:00Z",
        )
        .unwrap();
        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-23T07:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, base FROM mysql_shop_instant._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                cut(
                    "mysql_shop_instant.people_2026092106",
                    "mysql_shop.people_20260920"
                ),
                cut(
                    "mysql_shop_instant.people_2026092206",
                    "mysql_shop.people_20260921"
                ),
                cut(
                    "mysql_shop_instant.people_2026092306",
                    "mysql_shop.people_20260922"
                ),
            ]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'people' FROM mysql_shop_instant.people_2026092106 ORDER BY id"
            ),
            [
                ("7".to_owned(), "people".to_owned()),
                ("8".to_owned(), "people".to_owned())
            ]
        );
    }

    #[test]
    fn a_catch_up_named_an_artifact_advances_that_one_alone() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);
        let pipeline = cut_at_six("one-at-a-time");
        let caught_up = |artifact, at: Option<&str>| RunCommand::CatchUp {
            target: Target::new(Some(artifact), at.map(|at| at.parse().unwrap())),
            to: None,
        };
        let instants = |client: &lithify_protocol::sink::duckdb::client::DuckDbClient| {
            rows(
                client,
                "SELECT target, base FROM mysql_shop_instant._lithify_log WHERE status = 'succeeded' ORDER BY id",
            )
        };

        settle(
            &pipeline,
            &client,
            &caught_up("snapshot", None),
            "2026-09-21T07:00:00Z",
        )
        .unwrap();
        let untouched = !lithify_core::sink::WarehouseClient::exists(
            &client,
            &lithify_core::sink::TablePath::new("mysql_shop_instant", "_lithify_log"),
        )
        .unwrap();
        settle(
            &pipeline,
            &client,
            &caught_up("instant", Some("06:00")),
            "2026-09-21T07:00:00Z",
        )
        .unwrap();

        assert!(untouched);
        assert_eq!(
            instants(&client),
            [(
                "mysql_shop_instant.people_2026092106".to_owned(),
                "mysql_shop.people_20260920".to_owned()
            )]
        );
    }

    #[test]
    fn an_instant_not_yet_reached_is_not_cut() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(
            &cut_at_six("not-yet-cut"),
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert!(
            !lithify_core::sink::WarehouseClient::exists(
                &client,
                &lithify_core::build_log::BuildLog::path(&lithify_core::sink::Namespace::from(
                    "mysql_shop_instant"
                )),
            )
            .unwrap()
        );
    }
}
