use std::collections::BTreeSet;

use lithify_artifact::shard::Shard;
use lithify_artifact::snapshot::Snapshot;
use lithify_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;
use lithify_pipeline::build_log::{BuildLog, Status};
use lithify_testing::{Replica, Shards};

const DAY: i64 = 86_400_000;

fn at(millis: i64) -> Ts<Event> {
    Ts::try_from(millis).unwrap()
}

fn day(from: i64) -> Window {
    Window::new(at(from * DAY), at((from + 1) * DAY))
}

fn table(name: &str) -> TableId {
    TableId::new("shop", name)
}

fn keyed(name: &str) -> TableModel {
    TableModel::new(
        table(name),
        vec![
            ColumnModel::new("id", "BIGINT", ColumnType::Int64, false, None),
            ColumnModel::new("name", "VARCHAR", ColumnType::Text, true, None),
        ],
        ["id"].into(),
    )
}

fn snapshot() -> Snapshot {
    Snapshot::new("shop_snapshot", Shard::new("%Y%m%d", chrono_tz::Tz::UTC))
}

fn opened() -> Replica {
    Replica::default()
        .create(at(0), keyed("people"))
        .insert(
            at(1_000),
            &table("people"),
            &[("id", 1.into()), ("name", "Alice".into())],
        )
        .create(at(2_000), keyed("orders"))
        .insert(
            at(3_000),
            &table("orders"),
            &[("id", 1.into()), ("name", "pen".into())],
        )
}

fn passed(built: &[BuildLog]) -> BTreeSet<(Option<TableId>, Option<TableId>)> {
    built
        .iter()
        .map(|log| (log.table_from().cloned(), log.table_to().cloned()))
        .collect()
}

async fn two_days(replica: &Replica) -> (Vec<BuildLog>, Vec<BuildLog>) {
    let shards = Shards::over(replica).await;
    let first = shards.settle(&snapshot(), &day(0), &[]).await;
    let second = shards.settle(&snapshot(), &day(1), &first).await;

    (first, second)
}

#[tokio::test]
async fn a_build_log_says_each_table_passed_through_a_window_as_the_source_did() {
    let replica = opened()
        .create(at(DAY + 1_000), keyed("pets"))
        .insert(
            at(DAY + 2_000),
            &table("pets"),
            &[("id", 1.into()), ("name", "Tama".into())],
        )
        .rename(at(DAY + 3_000), &table("people"), &table("persons"))
        .insert(
            at(DAY + 4_000),
            &table("persons"),
            &[("id", 2.into()), ("name", "Bob".into())],
        )
        .drop(at(DAY + 5_000), &table("orders"));

    let (_, built) = two_days(&replica).await;

    assert_eq!(passed(&built), replica.passages(&day(1)));
    assert!(built.iter().all(|log| log.status() == &Status::Succeeded));
}

#[tokio::test]
async fn a_build_log_folds_each_table_the_window_opened_with_onto_what_closed_it_the_day_before() {
    let replica = opened()
        .rename(at(DAY + 1_000), &table("people"), &table("persons"))
        .insert(
            at(DAY + 2_000),
            &table("persons"),
            &[("id", 2.into()), ("name", "Bob".into())],
        );

    let (first, second) = two_days(&replica).await;

    for log in second.iter().filter(|log| log.table_from().is_some()) {
        let before = first
            .iter()
            .find(|before| before.table_to() == log.table_from())
            .unwrap();

        assert_eq!(log.base(), before.target());
    }
}
