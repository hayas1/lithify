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
use lithify_core::model::{ColumnModel, ColumnName, ColumnType, TableId, TableModel};
use lithify_core::runner::Replayer;
use lithify_core::schedule::{Schedule, Timeline};
use lithify_core::sink::{Namespace, TablePath, WarehouseClient};
use lithify_core::testing::{CellValue, ColumnDdl, Placed, Replica, Row};
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;
use lithify_protocol::sink::duckdb::client::DuckDbClient;
use lithify_protocol::testing::Landing;
use lithify_protocol::{Debezium, DuckDbWarehouse, MySql};

const DAY: i64 = 86_400_000;

fn people() -> TableModel {
    TableModel::new(
        TableId::new("shop", "people"),
        vec![
            ColumnModel::new("id", "BIGINT", ColumnType::Int64, false, None),
            ColumnModel::new("name", "VARCHAR", ColumnType::Text, true, None),
        ],
        ["id"].into(),
    )
}

struct Shards {
    client: DuckDbClient,
    artifact: Snapshot,
}

impl Shards {
    async fn over(replica: &Replica) -> Self {
        let warehouse = DuckDbWarehouse::new("cdc");
        let landing = Landing::of(replica).create(&warehouse);
        let client = DuckDbClient::new(Connection::open_in_memory().unwrap(), warehouse);
        client.execute(&landing).await.unwrap();

        Self {
            client,
            artifact: Snapshot::new("shop_snapshot", Shard::new("%Y%m%d", chrono_tz::Tz::UTC)),
        }
    }

    async fn build(&self, window: &Window, previous: &[BuildLog]) -> BuildLog {
        self.build_into(&self.artifact, window, previous).await
    }

    async fn build_into<A: Artifact>(
        &self,
        artifact: &A,
        window: &Window,
        previous: &[BuildLog],
    ) -> BuildLog {
        let mut built = self.settle(artifact, window, previous).await;
        assert_eq!(built.len(), 1);
        let built = built.remove(0);
        assert_eq!(built.status(), &Status::Succeeded);

        built
    }

    async fn settle<A: Artifact>(
        &self,
        artifact: &A,
        window: &Window,
        previous: &[BuildLog],
    ) -> Vec<BuildLog> {
        let log = Debezium::under("value", Landing::TOPIC_PREFIX, MySql);
        let run = Run::new(
            Ulid::nil(),
            Ts::from(DateTime::UNIX_EPOCH),
            artifact.series().artifact(),
            artifact.series().schedule(),
            "primary",
            "{}",
            "0.1.0",
        );

        Replayer::new(&self.client, &log)
            .built::<_, Box<dyn std::error::Error>>(artifact, window, previous, |_| true)
            .await
            .unwrap()
            .into_iter()
            .map(|built| built.logged(&run))
            .collect()
    }

    fn columns(&self, built: &BuildLog) -> Vec<String> {
        let target = built.target().unwrap();

        self.client
            .connection()
            .prepare(&format!(
                "SELECT * FROM \"{}\".\"{}\" LIMIT 0",
                target.namespace(),
                target.name()
            ))
            .unwrap()
            .query([])
            .unwrap()
            .as_ref()
            .unwrap()
            .column_names()
    }

    fn rows(&self, built: &BuildLog) -> Vec<Row> {
        let target = built.target().unwrap();

        self.read(&format!(
            "SELECT * FROM \"{}\".\"{}\"",
            target.namespace(),
            target.name()
        ))
    }

    fn read(&self, query: &str) -> Vec<Row> {
        let mut statement = self.client.connection().prepare(query).unwrap();
        let mut rows = statement.query([]).unwrap();
        let names = rows.as_ref().unwrap().column_names();
        let mut read = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            read.push(
                names
                    .iter()
                    .enumerate()
                    .map(|(index, name)| {
                        (
                            ColumnName::from(name.as_str()),
                            Self::cell(row.get(index).unwrap()),
                        )
                    })
                    .collect(),
            );
        }

        read.sort();

        read
    }

    fn cell(value: duckdb::types::Value) -> CellValue {
        match value {
            duckdb::types::Value::Null => CellValue::Null,
            duckdb::types::Value::SmallInt(number) => CellValue::Int(number.into()),
            duckdb::types::Value::Int(number) => CellValue::Int(number.into()),
            duckdb::types::Value::BigInt(number) => CellValue::Int(number),
            duckdb::types::Value::Text(text) => CellValue::Text(text),
            unread => panic!("a cell of {unread:?} is not read into a replica's"),
        }
    }

    fn as_of(&self, tables: &[TablePath], at: i64) -> Vec<Row> {
        let versions = tables
            .iter()
            .map(|table| {
                format!(
                    "SELECT * FROM \"{}\".\"{}\"",
                    table.namespace(),
                    table.name()
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL BY NAME ");

        self.read(&format!(
            "SELECT * EXCLUDE (_valid_from, _valid_to) FROM ({versions}) WHERE _valid_from < {at} AND (_valid_to IS NULL OR _valid_to >= {at})"
        ))
    }
}

fn at(millis: i64) -> Ts<Event> {
    Ts::try_from(millis).unwrap()
}

fn day(from: i64) -> Window {
    Window::new(
        Ts::try_from(from * DAY).unwrap(),
        Ts::try_from((from + 1) * DAY).unwrap(),
    )
}

fn replica() -> Replica {
    let people = people();
    let id = people.id().clone();

    Replica::default()
        .create(at(0), people)
        .insert(
            at(1_000),
            &id,
            &[("id", 1.into()), ("name", "Alice".into())],
        )
        .insert(at(2_000), &id, &[("id", 2.into()), ("name", "Bob".into())])
        .update(
            at(3_000),
            &id,
            &[("id", 1.into()), ("name", "Alicia".into())],
        )
        .delete(at(DAY + 1_000), &id, &[("id", 2.into())])
        .insert(
            at(DAY + 2_000),
            &id,
            &[("id", 3.into()), ("name", CellValue::Null)],
        )
        .update(
            at(DAY + 3_000),
            &id,
            &[("id", 1.into()), ("name", "Ally".into())],
        )
}

fn held(replica: &Replica, at: Ts<Event>, table: &TableId) -> Vec<Row> {
    let mut rows = replica.dump(at).rows(table).unwrap_or_default();
    rows.sort();

    rows
}

#[tokio::test]
async fn a_snapshot_holds_what_the_source_held_as_its_window_closed() {
    let replica = replica();
    let shards = Shards::over(&replica).await;

    let built = shards.build(&day(0), &[]).await;

    assert_eq!(
        shards.rows(&built),
        held(&replica, *day(0).to(), people().id())
    );
}

#[tokio::test]
async fn a_snapshot_holds_a_column_added_within_its_window_as_the_source_held_it() {
    let tier = ColumnModel::new("tier", "VARCHAR", ColumnType::Text, true, Some("gold"));
    let replica = replica()
        .alter(at(DAY + 4_000), people().id(), vec![ColumnDdl::add(tier)])
        .update(
            at(DAY + 5_000),
            people().id(),
            &[
                ("id", 3.into()),
                ("name", "Cy".into()),
                ("tier", "silver".into()),
            ],
        );
    let shards = Shards::over(&replica).await;

    let first = shards.build(&day(0), &[]).await;
    let second = shards.build(&day(1), &[first]).await;

    assert_eq!(
        shards.rows(&second),
        held(&replica, *day(1).to(), people().id())
    );
}

#[tokio::test]
async fn a_snapshot_leaves_out_a_column_dropped_within_its_window_as_the_source_did() {
    let replica = replica().alter(
        at(DAY + 4_000),
        people().id(),
        vec![ColumnDdl::drop("name")],
    );
    let shards = Shards::over(&replica).await;

    let first = shards.build(&day(0), &[]).await;
    let second = shards.build(&day(1), &[first]).await;

    assert_eq!(
        shards.rows(&second),
        held(&replica, *day(1).to(), people().id())
    );
}

#[tokio::test]
async fn a_snapshot_carries_a_column_renamed_within_its_window_under_its_new_name() {
    let replica = replica()
        .alter(
            at(DAY + 4_000),
            people().id(),
            vec![ColumnDdl::rename("name", "label")],
        )
        .insert(
            at(DAY + 5_000),
            people().id(),
            &[("id", 4.into()), ("label", "Dee".into())],
        );
    let shards = Shards::over(&replica).await;

    let first = shards.build(&day(0), &[]).await;
    let second = shards.build(&day(1), &[first]).await;

    assert_eq!(
        shards.rows(&second),
        held(&replica, *day(1).to(), people().id())
    );
}

#[tokio::test]
async fn a_snapshot_folded_onto_the_one_before_holds_what_the_source_held_as_its_window_closed() {
    let replica = replica();
    let shards = Shards::over(&replica).await;

    let first = shards.build(&day(0), &[]).await;
    let second = shards.build(&day(1), &[first]).await;

    assert_eq!(
        shards.rows(&second),
        held(&replica, *day(1).to(), people().id())
    );
}

#[tokio::test]
async fn an_instant_holds_what_the_source_held_as_it_was_cut() {
    let replica = replica();
    let shards = Shards::over(&replica).await;
    let six = Instant::new(
        NaiveTime::from_hms_opt(6, 0, 0).unwrap(),
        Schedule::default(),
        "shop_instant",
        Shard::new("%Y%m%d%H", chrono_tz::Tz::UTC),
    );
    let first = shards.build(&day(0), &[]).await;
    let timeline = Timeline::new(Schedule::default(), *day(0).from()..*day(2).to());
    let cut = six.window(&day(1), &timeline).unwrap();

    let instant = shards
        .build_into(&six, &cut, std::slice::from_ref(&first))
        .await;

    assert_eq!(
        shards.rows(&instant),
        held(&replica, *cut.to(), people().id())
    );
}

#[tokio::test]
async fn an_scd1_holds_what_the_source_holds_now_onto_the_last_daily_snapshot() {
    let replica = replica();
    let shards = Shards::over(&replica).await;
    let scd1 = Scd1::new("shop_scd1");
    let first = shards.build(&day(0), &[]).await;
    let built = at(DAY + 1_500);
    let now = at(i64::from(i32::MAX));
    let timeline = Timeline::new(Schedule::default(), *day(0).from()..built);
    let window = scd1.window(&day(1), &timeline).unwrap();

    let current = shards
        .build_into(&scd1, &window, std::slice::from_ref(&first))
        .await;

    assert_eq!(shards.rows(&current), held(&replica, now, people().id()));
}

fn scd2() -> Scd2 {
    Scd2::new(Namespace::from("shop_scd2"), "%Y%m%d", chrono_tz::Tz::UTC)
}

#[tokio::test]
async fn an_scd2_holds_at_every_instant_the_version_the_source_held_then() {
    let replica = replica();
    let shards = Shards::over(&replica).await;
    let scd2 = scd2();
    let first = shards.build_into(&scd2, &day(0), &[]).await;
    let second = shards
        .build_into(&scd2, &day(1), std::slice::from_ref(&first))
        .await;
    let versions = [
        scd2.closed(people().id(), &day(0)),
        scd2.closed(people().id(), &day(1)),
        second.target().unwrap().clone(),
    ];

    for millis in [
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
            shards.as_of(&versions, millis),
            held(&replica, at(millis), people().id()),
            "as of {millis}"
        );
    }
}

#[tokio::test]
async fn an_scd2_built_again_for_a_window_holds_the_same_versions() {
    let replica = replica();
    let shards = Shards::over(&replica).await;
    let scd2 = scd2();
    let first = shards.build_into(&scd2, &day(0), &[]).await;
    let second = shards
        .build_into(&scd2, &day(1), std::slice::from_ref(&first))
        .await;
    let versions = [
        scd2.closed(people().id(), &day(1)),
        second.target().unwrap().clone(),
    ];
    let built = shards.as_of(&versions, DAY + 2_500);

    shards
        .build_into(&scd2, &day(1), std::slice::from_ref(&first))
        .await;

    assert_eq!(shards.as_of(&versions, DAY + 2_500), built);
}

fn persons() -> TableId {
    TableId::new("shop", "persons")
}

fn text(name: &str) -> ColumnModel {
    ColumnModel::new(name, "VARCHAR", ColumnType::Text, true, None)
}

fn number(name: &str) -> ColumnModel {
    ColumnModel::new(name, "BIGINT", ColumnType::Int64, true, None)
}

fn names(table: &TableModel) -> Vec<String> {
    table
        .columns()
        .map(|column| column.name().to_string())
        .collect()
}

fn of<'a>(built: &'a [BuildLog], table: &TableId) -> &'a BuildLog {
    built
        .iter()
        .find(|log| log.table() == Some(table))
        .unwrap_or_else(|| panic!("{table} was not built"))
}

async fn two_days(replica: &Replica) -> (Shards, Vec<BuildLog>) {
    let shards = Shards::over(replica).await;
    let first = shards.settle(&shards.artifact, &day(0), &[]).await;
    let second = shards.settle(&shards.artifact, &day(1), &first).await;

    (shards, second)
}

#[tokio::test]
async fn a_snapshot_casts_a_column_retyped_within_its_window_as_the_source_did() {
    let replica = replica()
        .insert(
            at(DAY + 4_000),
            people().id(),
            &[("id", 5.into()), ("name", "42".into())],
        )
        .delete(at(DAY + 4_500), people().id(), &[("id", 3.into())])
        .update(
            at(DAY + 4_600),
            people().id(),
            &[("id", 1.into()), ("name", "7".into())],
        )
        .alter(
            at(DAY + 5_000),
            people().id(),
            vec![ColumnDdl::modify(number("name"))],
        );

    let (shards, built) = two_days(&replica).await;

    assert_eq!(
        shards.rows(of(&built, people().id())),
        held(&replica, *day(1).to(), people().id())
    );
}

#[tokio::test]
async fn a_snapshot_lands_each_column_where_the_statements_placed_it() {
    let replica = replica()
        .alter(
            at(DAY + 4_000),
            people().id(),
            vec![
                ColumnDdl::add(text("tier")).placed(Placed::First),
                ColumnDdl::change("name", text("label")).placed(Placed::After("tier".into())),
            ],
        )
        .insert(
            at(DAY + 5_000),
            people().id(),
            &[
                ("id", 6.into()),
                ("label", "Eve".into()),
                ("tier", "gold".into()),
            ],
        );

    let (shards, built) = two_days(&replica).await;
    let built = of(&built, people().id());

    assert_eq!(
        shards.columns(built),
        names(replica.dump(*day(1).to()).table(people().id()).unwrap())
    );
    assert_eq!(
        shards.rows(built),
        held(&replica, *day(1).to(), people().id())
    );
}

#[tokio::test]
async fn a_snapshot_of_a_table_renamed_within_its_window_holds_its_rows_under_its_new_name() {
    let replica = replica()
        .rename(at(DAY + 4_000), people().id(), &persons())
        .insert(
            at(DAY + 5_000),
            &persons(),
            &[("id", 6.into()), ("name", "Eve".into())],
        );

    let (shards, built) = two_days(&replica).await;

    assert_eq!(
        shards.rows(of(&built, &persons())),
        held(&replica, *day(1).to(), &persons())
    );
}

#[tokio::test]
async fn a_snapshot_of_a_table_renamed_and_altered_by_one_statement_holds_both() {
    let replica = replica()
        .alter_into(
            at(DAY + 4_000),
            people().id(),
            &persons(),
            vec![ColumnDdl::add(text("tier"))],
        )
        .insert(
            at(DAY + 5_000),
            &persons(),
            &[
                ("id", 6.into()),
                ("name", "Eve".into()),
                ("tier", "gold".into()),
            ],
        );

    let (shards, built) = two_days(&replica).await;

    assert_eq!(
        shards.rows(of(&built, &persons())),
        held(&replica, *day(1).to(), &persons())
    );
}

#[tokio::test]
async fn a_table_dropped_within_its_window_is_built_no_more() {
    let replica = replica().drop(at(DAY + 4_000), people().id());

    let (_, built) = two_days(&replica).await;
    let dropped = of(&built, people().id());

    assert_eq!(dropped.status(), &Status::Succeeded);
    assert_eq!(dropped.target(), None);
}

#[tokio::test]
async fn a_table_dropped_and_created_again_within_its_window_holds_only_what_it_was_given_since() {
    let replica = replica()
        .drop(at(DAY + 4_000), people().id())
        .create(
            at(DAY + 5_000),
            TableModel::new(
                people().id().clone(),
                vec![number("id"), text("nickname")],
                ["id"].into(),
            ),
        )
        .insert(
            at(DAY + 6_000),
            people().id(),
            &[("id", 8.into()), ("nickname", "Hal".into())],
        );

    let (shards, built) = two_days(&replica).await;
    let created = built.iter().find(|log| log.target().is_some()).unwrap();

    assert_eq!(
        shards.rows(created),
        held(&replica, *day(1).to(), people().id())
    );
}

#[tokio::test]
async fn a_table_rekeyed_within_its_window_is_refused_while_the_others_are_built() {
    let pets = TableModel::new(
        TableId::new("shop", "pets"),
        vec![number("id"), text("kind")],
        ["id"].into(),
    );
    let replica = replica()
        .create(at(DAY + 3_500), pets.clone())
        .insert(
            at(DAY + 3_600),
            pets.id(),
            &[("id", 1.into()), ("kind", "cat".into())],
        )
        .alter(
            at(DAY + 4_000),
            people().id(),
            vec![ColumnDdl::rekey(&["id", "name"])],
        );

    let (shards, built) = two_days(&replica).await;

    assert!(of(&built, people().id()).status().failed());
    assert_eq!(
        shards.rows(of(&built, pets.id())),
        held(&replica, *day(1).to(), pets.id())
    );
}

#[tokio::test]
async fn changes_in_the_instant_of_a_schema_change_fall_on_the_side_their_position_puts_them() {
    let tier = ColumnModel::new("tier", "VARCHAR", ColumnType::Text, true, Some("gold"));
    let replica = replica()
        .update(
            at(DAY + 4_000),
            people().id(),
            &[("id", 1.into()), ("name", "Before".into())],
        )
        .alter(at(DAY + 4_000), people().id(), vec![ColumnDdl::add(tier)])
        .update(
            at(DAY + 4_000),
            people().id(),
            &[
                ("id", 3.into()),
                ("name", "After".into()),
                ("tier", "silver".into()),
            ],
        );

    let (shards, built) = two_days(&replica).await;

    assert_eq!(
        shards.rows(of(&built, people().id())),
        held(&replica, *day(1).to(), people().id())
    );
}
