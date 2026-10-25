use chrono::DateTime;
use duckdb::Connection;
use ulid::Ulid;

use chrono::NaiveTime;

use lithify_artifact::instant::Instant;
use lithify_artifact::scd1::Scd1;
use lithify_artifact::scd2::Scd2;
use lithify_artifact::shard::Shard;
use lithify_artifact::snapshot::Snapshot;
use lithify_core::artifact::Artifact;
use lithify_core::build_log::{BuildLog, Run, Status};
use lithify_core::listener::{Finish, ReplayEnd};
use lithify_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use lithify_core::schedule::{Schedule, Timeline};
use lithify_core::sink::{Namespace, TablePath, WarehouseClient};
use lithify_core::sql::SqlString;
use lithify_core::testing::{Mutation, Replica, Row};
use lithify_core::ts::Ts;
use lithify_core::window::Window;
use lithify_protocol::sink::duckdb::client::DuckDbClient;
use lithify_protocol::{Debezium, DuckDbWarehouse, MySql};

const DAY: i64 = 86_400_000;

fn people() -> TableModel {
    TableModel::new(
        TableId::new("shop", "people"),
        vec![
            ColumnModel::new("id", "BIGINT", ColumnType::Int64, false, None),
            ColumnModel::new("name", "VARCHAR", ColumnType::Text, true, None),
        ],
        vec!["id".to_owned()],
    )
}

struct ChangeLog<'a>(&'a Replica);

impl ChangeLog<'_> {
    const AFTER: &'static str = "STRUCT(id BIGINT, name VARCHAR)";

    fn create(&self) -> SqlString {
        let values: Vec<String> = self
            .0
            .mutations()
            .iter()
            .enumerate()
            .map(|(position, (at, mutation))| {
                let at = at.milliseconds_since_epoch();
                let (op, before, after) = match mutation {
                    Mutation::Upsert(row) => ("u", Self::null(), Self::image(row)),
                    Mutation::Delete(row) => ("d", Self::image(row), Self::null()),
                    other => panic!("{other:?} is not a change of a row"),
                };

                format!(
                    "({{'op': '{op}', 'ts_ms': {at}::BIGINT, 'source': {{'file': 'binlog.000001', 'pos': {}::BIGINT, 'ts_ms': {at}::BIGINT}}, 'before': {before}, 'after': {after}}})",
                    position + 1
                )
            })
            .collect();

        SqlString::from(format!(
            "CREATE SCHEMA IF NOT EXISTS cdc;\nCREATE TABLE cdc.\"mysql.shop.people\" AS SELECT * FROM (VALUES\n{}\n) AS t(value)",
            values.join(",\n")
        ))
    }

    fn null() -> String {
        format!("NULL::{}", Self::AFTER)
    }

    fn image(row: &Row) -> String {
        let cell = |column: &str| match row.get(column).cloned().flatten() {
            Some(value) if column == "id" => value,
            Some(value) => format!("'{value}'"),
            None => "NULL".to_owned(),
        };

        format!(
            "{{'id': {}::BIGINT, 'name': {}::VARCHAR}}",
            cell("id"),
            cell("name")
        )
    }
}

struct Shards {
    client: DuckDbClient,
    artifact: Snapshot,
}

impl Shards {
    fn over(replica: &Replica) -> Self {
        let client = DuckDbClient::new(
            Connection::open_in_memory().unwrap(),
            DuckDbWarehouse::new("cdc"),
        );
        client.execute(&ChangeLog(replica).create()).unwrap();

        Self {
            client,
            artifact: Snapshot::new("shop_snapshot", Shard::new("%Y%m%d", chrono_tz::Tz::UTC)),
        }
    }

    fn build(&self, window: &Window, previous: Option<&BuildLog>) -> BuildLog {
        self.build_into(&self.artifact, window, previous)
    }

    fn build_into<A: Artifact>(
        &self,
        artifact: &A,
        window: &Window,
        previous: Option<&BuildLog>,
    ) -> BuildLog {
        let log = Debezium::under("value", "mysql", MySql);
        let build = artifact
            .builder::<_, DuckDbWarehouse>(&log, people().id().clone(), window, previous)
            .finish(self.client.warehouse(), ReplayEnd::standing(people(), None))
            .unwrap();
        for statement in build.statements().unwrap() {
            self.client.execute(statement).unwrap();
        }

        build.built(Status::Succeeded).logged(
            &Run::new(
                Ulid::nil(),
                DateTime::UNIX_EPOCH,
                artifact.series().artifact(),
                artifact.series().schedule(),
                "primary",
                "{}",
                "0.1.0",
            ),
            DateTime::UNIX_EPOCH,
        )
    }

    fn rows(&self, built: &BuildLog) -> Vec<Row> {
        let target = built.target().unwrap();

        self.client
            .connection()
            .prepare(&format!(
                "SELECT id::VARCHAR, name FROM \"{}\".\"{}\" ORDER BY id",
                target.namespace(),
                target.name()
            ))
            .unwrap()
            .query_map([], |row| {
                Ok(Row::from([
                    ("id".to_owned(), row.get::<_, Option<String>>(0)?),
                    ("name".to_owned(), row.get::<_, Option<String>>(1)?),
                ]))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }
}

fn day(from: i64) -> Window {
    Window::new(
        Ts::try_from(from * DAY).unwrap(),
        Ts::try_from((from + 1) * DAY).unwrap(),
    )
}

fn replica() -> Replica {
    Replica::keyed_by(&["id"])
        .upsert(1_000, &[("id", Some("1")), ("name", Some("Alice"))])
        .upsert(2_000, &[("id", Some("2")), ("name", Some("Bob"))])
        .upsert(3_000, &[("id", Some("1")), ("name", Some("Alicia"))])
        .delete(DAY + 1_000, &[("id", Some("2"))])
        .upsert(DAY + 2_000, &[("id", Some("3")), ("name", None)])
        .upsert(DAY + 3_000, &[("id", Some("1")), ("name", Some("Ally"))])
}

#[test]
fn a_snapshot_holds_what_the_source_held_as_its_window_closed() {
    let replica = replica();
    let shards = Shards::over(&replica);

    let built = shards.build(&day(0), None);

    assert_eq!(shards.rows(&built), replica.at(*day(0).to()));
}

#[test]
fn a_snapshot_folded_onto_the_one_before_holds_what_the_source_held_as_its_window_closed() {
    let replica = replica();
    let shards = Shards::over(&replica);

    let first = shards.build(&day(0), None);
    let second = shards.build(&day(1), Some(&first));

    assert_eq!(shards.rows(&second), replica.at(*day(1).to()));
}

#[test]
fn an_instant_holds_what_the_source_held_as_it_was_cut() {
    let replica = replica();
    let shards = Shards::over(&replica);
    let six = Instant::new(
        NaiveTime::from_hms_opt(6, 0, 0).unwrap(),
        Schedule::default(),
        "shop_instant",
        Shard::new("%Y%m%d%H", chrono_tz::Tz::UTC),
    );
    let first = shards.build(&day(0), None);
    let timeline = Timeline::new(Schedule::default(), *day(0).from()..*day(2).to());
    let cut = six.window(&day(1), &timeline).unwrap();

    let instant = shards.build_into(
        &six,
        &cut,
        six.base(std::slice::from_ref(&first), people().id(), &cut),
    );

    assert_eq!(shards.rows(&instant), replica.at(*cut.to()));
}

#[test]
fn an_scd1_holds_what_the_source_holds_now_onto_the_last_daily_snapshot() {
    let replica = replica();
    let shards = Shards::over(&replica);
    let scd1 = Scd1::new("shop_scd1");
    let first = shards.build(&day(0), None);
    let built = Ts::try_from(DAY + 1_500).unwrap();
    let now = Ts::try_from(i64::from(i32::MAX)).unwrap();
    let timeline = Timeline::new(Schedule::default(), *day(0).from()..built);
    let window = scd1.window(&day(1), &timeline).unwrap();

    let current = shards.build_into(
        &scd1,
        &window,
        scd1.base(std::slice::from_ref(&first), people().id(), &window),
    );

    assert_eq!(shards.rows(&current), replica.at(now));
}

impl Shards {
    fn as_of(&self, tables: &[TablePath], at: i64) -> Vec<Row> {
        let versions = tables
            .iter()
            .map(|table| {
                format!(
                    "SELECT id, name, _valid_from, _valid_to FROM \"{}\".\"{}\"",
                    table.namespace(),
                    table.name()
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        self.client
            .connection()
            .prepare(&format!(
                "SELECT id::VARCHAR, name FROM ({versions}) WHERE _valid_from < {at} AND (_valid_to IS NULL OR _valid_to >= {at}) ORDER BY id"
            ))
            .unwrap()
            .query_map([], |row| {
                Ok(Row::from([
                    ("id".to_owned(), row.get::<_, Option<String>>(0)?),
                    ("name".to_owned(), row.get::<_, Option<String>>(1)?),
                ]))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }
}

fn scd2() -> Scd2 {
    Scd2::new(Namespace::from("shop_scd2"), "%Y%m%d", chrono_tz::Tz::UTC)
}

#[test]
fn an_scd2_holds_at_every_instant_the_version_the_source_held_then() {
    let replica = replica();
    let shards = Shards::over(&replica);
    let scd2 = scd2();
    let first = shards.build_into(&scd2, &day(0), None);
    let second = shards.build_into(
        &scd2,
        &day(1),
        scd2.base(std::slice::from_ref(&first), people().id(), &day(1)),
    );
    let versions = [
        scd2.closed(people().id(), &day(0)),
        scd2.closed(people().id(), &day(1)),
        second.target().unwrap().clone(),
    ];

    for at in [
        500,
        1_500,
        2_500,
        3_500,
        DAY,
        DAY + 1_500,
        DAY + 2_500,
        DAY + 3_500,
        2 * DAY,
    ] {
        assert_eq!(
            shards.as_of(&versions, at),
            replica.at(Ts::try_from(at).unwrap()),
            "as of {at}"
        );
    }
}

#[test]
fn an_scd2_built_again_for_a_window_holds_the_same_versions() {
    let replica = replica();
    let shards = Shards::over(&replica);
    let scd2 = scd2();
    let first = shards.build_into(&scd2, &day(0), None);
    let base = scd2.base(std::slice::from_ref(&first), people().id(), &day(1));
    let second = shards.build_into(&scd2, &day(1), base);
    let versions = [
        scd2.closed(people().id(), &day(1)),
        second.target().unwrap().clone(),
    ];
    let built = shards.as_of(&versions, DAY + 2_500);

    shards.build_into(&scd2, &day(1), base);

    assert_eq!(shards.as_of(&versions, DAY + 2_500), built);
}
