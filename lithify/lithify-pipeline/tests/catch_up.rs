use std::error::Error;

use chrono::{DateTime, NaiveDate, Utc};

use lithify_pipeline::build_log::{BuildLog, Status};
use lithify_pipeline::runner::Unsteppable;
use lithify_pipeline::stepping::Target;
use lithify_schema::model::{ColumnModel, ColumnType, TableId, TableModel};
use lithify_schema::ts::{Event, Replay, Ts};
use lithify_testing::{CellValue, ColumnDdl, Deployment, Replica};

const NOW: &str = "2026-09-22T05:00:00Z";

fn now(rfc3339: &str) -> Ts<Replay> {
    Ts::from(rfc3339.parse::<DateTime<Utc>>().unwrap())
}

fn date(date: &str) -> NaiveDate {
    date.parse().unwrap()
}

fn on(date: &str, millis: i64) -> Ts<Event> {
    let midnight = format!("{date}T00:00:00Z")
        .parse::<DateTime<Utc>>()
        .unwrap();

    Ts::try_from(midnight.timestamp_millis() + millis).unwrap()
}

fn table(name: &str) -> TableId {
    TableId::new("shop", name)
}

fn keyed(name: &str) -> TableModel {
    TableModel::new(
        table(name),
        vec![ColumnModel::new(
            "id",
            "BIGINT",
            ColumnType::Int64,
            false,
            None,
        )],
        ["id"].into(),
    )
}

fn tagged(name: &str) -> TableModel {
    TableModel::new(
        table(name),
        vec![
            ColumnModel::new("id", "BIGINT", ColumnType::Int64, false, None),
            ColumnModel::new("tag", "BIGINT", ColumnType::Int64, false, None),
        ],
        ["id"].into(),
    )
}

fn people() -> Replica {
    Replica::default()
        .create(on("1970-01-01", 0), keyed("people"))
        .insert(on("2026-09-19", 1), &table("people"), &[("id", 7.into())])
}

fn with(pets: TableModel, row: &[(&str, CellValue)]) -> Replica {
    Replica::default()
        .create(on("1970-01-01", 0), keyed("people"))
        .create(on("1970-01-01", 1), pets)
        .insert(on("2026-09-19", 1), &table("people"), &[("id", 7.into())])
        .insert(on("2026-09-19", 2), &table("pets"), row)
}

fn with_pets() -> Replica {
    with(keyed("pets"), &[("id", 7.into())])
}

async fn built_up_to_20260919(replica: &Replica) -> Deployment {
    let deployment = Deployment::over(replica).await;
    deployment
        .catch_up(&Target::default(), Some(date("2026-09-19")), now(NOW))
        .await
        .unwrap();

    deployment
}

async fn caught_up(deployment: &Deployment) -> Vec<BuildLog> {
    deployment
        .catch_up(&Target::default(), None, now(NOW))
        .await
        .unwrap()
}

async fn logged(deployment: &Deployment) -> Vec<BuildLog> {
    deployment.build_log(&"snapshot".into()).await
}

fn named(path: Option<&impl ToString>) -> String {
    path.map_or_else(|| "-".to_owned(), ToString::to_string)
}

fn folded(log: &BuildLog) -> (String, String) {
    (named(log.target()), named(log.base()))
}

fn passed(log: &BuildLog) -> (String, String) {
    (
        format!(
            "{} {} {}",
            named(log.table_from()),
            named(log.table_to()),
            named(log.lifecycle().as_ref())
        ),
        folded(log).0,
    )
}

fn pair(left: &str, right: &str) -> (String, String) {
    (left.to_owned(), right.to_owned())
}

fn since_20260920(logs: Vec<BuildLog>) -> Vec<BuildLog> {
    logs.into_iter()
        .filter(|log| *log.window().from() >= on("2026-09-20", 0))
        .collect()
}

fn succeeded(logs: Vec<BuildLog>) -> Vec<BuildLog> {
    logs.into_iter()
        .filter(|log| log.status() == &Status::Succeeded)
        .collect()
}

fn ids(deployment: &Deployment, logs: &[BuildLog], target: &str) -> Vec<CellValue> {
    let built = logs
        .iter()
        .find(|log| named(log.target()) == target)
        .unwrap();

    deployment
        .shard(built)
        .into_iter()
        .map(|row| row["id"].clone())
        .collect()
}

fn unsteppable(failed: Box<dyn Error>) -> Unsteppable {
    *failed.downcast::<Unsteppable>().unwrap()
}

#[tokio::test]
async fn a_shard_folds_onto_the_one_the_build_log_recorded_closing_at_its_opening() {
    let replica = people().insert(on("2026-09-20", 1), &table("people"), &[("id", 8.into())]);
    let deployment = built_up_to_20260919(&replica).await;

    deployment
        .catch_up(&Target::default(), Some(date("2026-09-20")), now(NOW))
        .await
        .unwrap();

    assert_eq!(
        logged(&deployment)
            .await
            .iter()
            .map(folded)
            .collect::<Vec<_>>(),
        [
            pair("mysql_shop.people_20260919", "-"),
            pair("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
        ]
    );
}

#[tokio::test]
async fn a_run_is_named_after_the_instant_it_took_for_now() {
    let deployment = Deployment::over(&people()).await;

    let built = caught_up(&deployment).await;

    assert!(!built.is_empty());
    assert!(
        built
            .iter()
            .all(|log| log.run_id().timestamp_ms() == 1790053200000)
    );
}

#[tokio::test]
async fn the_shard_the_change_log_opens_in_is_built_from_nothing_before_anything_was_logged() {
    let deployment = Deployment::over(&people()).await;

    deployment
        .catch_up(&Target::default(), Some(date("1970-01-01")), now(NOW))
        .await
        .unwrap();

    assert_eq!(
        logged(&deployment)
            .await
            .iter()
            .map(folded)
            .collect::<Vec<_>>(),
        [pair("mysql_shop.people_19700101", "-")]
    );
}

#[tokio::test]
async fn a_table_created_within_a_shard_is_built_from_nothing_beside_the_ones_folded() {
    let replica = people()
        .insert(on("2026-09-20", 1), &table("people"), &[("id", 8.into())])
        .create(on("2026-09-20", 5), keyed("pets"))
        .insert(on("2026-09-20", 9), &table("pets"), &[("id", 9.into())]);
    let deployment = built_up_to_20260919(&replica).await;

    deployment
        .catch_up(&Target::default(), Some(date("2026-09-20")), now(NOW))
        .await
        .unwrap();

    assert_eq!(
        logged(&deployment)
            .await
            .iter()
            .map(folded)
            .collect::<Vec<_>>(),
        [
            pair("mysql_shop.people_20260919", "-"),
            pair("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
            pair("mysql_shop.pets_20260920", "-"),
        ]
    );
}

#[tokio::test]
async fn a_table_renamed_within_a_window_folds_the_next_window_onto_what_it_was_built_into() {
    let replica = people()
        .insert(on("2026-09-20", 1), &table("people"), &[("id", 8.into())])
        .rename(on("2026-09-20", 5), &table("people"), &table("persons"))
        .insert(on("2026-09-21", 9), &table("persons"), &[("id", 9.into())]);
    let deployment = built_up_to_20260919(&replica).await;

    caught_up(&deployment).await;

    let logs = logged(&deployment).await;
    assert_eq!(
        logs.iter()
            .map(|log| (passed(log).0, folded(log)))
            .collect::<Vec<_>>(),
        [
            (
                "- shop.people created".to_owned(),
                pair("mysql_shop.people_20260919", "-")
            ),
            (
                "shop.people shop.persons renamed".to_owned(),
                pair("mysql_shop.persons_20260920", "mysql_shop.people_20260919")
            ),
            (
                "shop.persons shop.persons -".to_owned(),
                pair("mysql_shop.persons_20260921", "mysql_shop.persons_20260920")
            ),
        ]
    );
    assert_eq!(
        ids(&deployment, &logs, "mysql_shop.persons_20260921"),
        [7.into(), 8.into(), 9.into()]
    );
}

#[tokio::test]
async fn a_table_dropped_and_created_again_within_a_window_is_built_anew_and_folded_onto_after() {
    let replica = with_pets()
        .insert(on("2026-09-20", 1), &table("people"), &[("id", 8.into())])
        .insert(on("2026-09-20", 2), &table("pets"), &[("id", 6.into())])
        .drop(on("2026-09-20", 5), &table("pets"))
        .create(on("2026-09-20", 6), keyed("pets"))
        .insert(on("2026-09-20", 9), &table("pets"), &[("id", 9.into())]);
    let deployment = built_up_to_20260919(&replica).await;

    caught_up(&deployment).await;

    let logs = since_20260920(logged(&deployment).await)
        .into_iter()
        .filter(|log| log.table_from() != Some(&table("people")))
        .collect::<Vec<_>>();
    assert_eq!(
        logs.iter()
            .map(|log| (passed(log).0, folded(log)))
            .collect::<Vec<_>>(),
        [
            (
                "shop.pets - dropped".to_owned(),
                pair("-", "mysql_shop.pets_20260919")
            ),
            (
                "- shop.pets created".to_owned(),
                pair("mysql_shop.pets_20260920", "-")
            ),
            (
                "shop.pets shop.pets -".to_owned(),
                pair("mysql_shop.pets_20260921", "mysql_shop.pets_20260920")
            ),
        ]
    );
    assert_eq!(
        ids(&deployment, &logs, "mysql_shop.pets_20260920"),
        [9.into()]
    );
    assert_eq!(
        ids(&deployment, &logs, "mysql_shop.pets_20260921"),
        [9.into()]
    );
}

#[tokio::test]
async fn a_table_created_again_in_a_later_window_is_built_anew_rather_than_onto_what_was_dropped() {
    let replica = with_pets()
        .insert(on("2026-09-20", 1), &table("people"), &[("id", 8.into())])
        .insert(on("2026-09-20", 2), &table("pets"), &[("id", 6.into())])
        .drop(on("2026-09-20", 5), &table("pets"))
        .create(on("2026-09-21", 6), keyed("pets"))
        .insert(on("2026-09-21", 9), &table("pets"), &[("id", 9.into())]);
    let deployment = built_up_to_20260919(&replica).await;

    caught_up(&deployment).await;

    let logs = since_20260920(logged(&deployment).await)
        .into_iter()
        .filter(|log| log.table_from() != Some(&table("people")))
        .collect::<Vec<_>>();
    assert_eq!(
        logs.iter()
            .map(|log| (passed(log).0, folded(log)))
            .collect::<Vec<_>>(),
        [
            (
                "shop.pets - dropped".to_owned(),
                pair("-", "mysql_shop.pets_20260919")
            ),
            (
                "- shop.pets created".to_owned(),
                pair("mysql_shop.pets_20260921", "-")
            ),
        ]
    );
    assert_eq!(
        ids(&deployment, &logs, "mysql_shop.pets_20260921"),
        [9.into()]
    );
}

fn rekeyed_on_20260920() -> Replica {
    with(tagged("pets"), &[("id", 7.into()), ("tag", 7.into())])
        .insert(on("2026-09-20", 1), &table("people"), &[("id", 8.into())])
        .alter(
            on("2026-09-20", 5),
            &table("pets"),
            vec![ColumnDdl::rekey(&["tag"])],
        )
        .insert(
            on("2026-09-20", 9),
            &table("pets"),
            &[("id", 9.into()), ("tag", 1.into())],
        )
}

fn attempted(logs: Vec<BuildLog>) -> Vec<(String, String)> {
    since_20260920(logs)
        .iter()
        .map(|log| (folded(log).0, log.status().to_string()))
        .collect()
}

fn why(logs: Vec<BuildLog>) -> Vec<(String, String)> {
    since_20260920(logs)
        .iter()
        .map(|log| match log.status() {
            Status::Succeeded => (folded(log).0, "-".to_owned()),
            Status::Failed(why) => (folded(log).0, why.clone()),
        })
        .collect()
}

#[tokio::test]
async fn a_table_refusing_a_schema_change_within_a_window_is_logged_as_failed_while_the_others_advance()
 {
    let deployment = built_up_to_20260919(&rekeyed_on_20260920()).await;

    caught_up(&deployment).await;

    assert_eq!(
        why(logged(&deployment).await),
        [
            pair("mysql_shop.people_20260920", "-"),
            pair(
                "mysql_shop.pets_20260920",
                "shop.pets changed its primary key within the window"
            ),
            pair("mysql_shop.people_20260921", "-"),
        ]
    );
}

#[tokio::test]
async fn a_table_failing_in_a_window_stops_itself_alone_and_is_tried_again_by_the_next_catch_up() {
    let deployment = built_up_to_20260919(&rekeyed_on_20260920()).await;

    let first = caught_up(&deployment).await;
    deployment
        .catch_up(&Target::default(), None, now("2026-09-22T06:00:00Z"))
        .await
        .unwrap();

    assert!(first.iter().any(|log| log.status().failed()));
    assert_eq!(
        attempted(logged(&deployment).await),
        [
            pair("mysql_shop.people_20260920", "succeeded"),
            pair("mysql_shop.pets_20260920", "failed"),
            pair("mysql_shop.people_20260921", "succeeded"),
            pair("mysql_shop.pets_20260920", "failed"),
        ]
    );
}

#[tokio::test]
async fn a_table_dropped_within_a_window_is_logged_as_dropped_and_built_no_more() {
    let replica = with_pets()
        .insert(on("2026-09-20", 1), &table("people"), &[("id", 8.into())])
        .drop(on("2026-09-20", 5), &table("pets"));
    let deployment = built_up_to_20260919(&replica).await;

    caught_up(&deployment).await;

    assert_eq!(
        since_20260920(logged(&deployment).await)
            .iter()
            .map(passed)
            .collect::<Vec<_>>(),
        [
            pair("shop.pets - dropped", "-"),
            pair("shop.people shop.people -", "mysql_shop.people_20260920"),
            pair("shop.people shop.people -", "mysql_shop.people_20260921"),
        ]
    );
    assert_eq!(deployment.inspect(now(NOW)).await.discrepancies(), []);
}

#[tokio::test]
async fn a_pipeline_catches_up_window_by_window_folding_each_onto_the_one_before() {
    let replica = people().insert(on("2026-09-21", 1), &table("people"), &[("id", 8.into())]);
    let deployment = built_up_to_20260919(&replica).await;

    caught_up(&deployment).await;

    let logs = succeeded(logged(&deployment).await);
    assert_eq!(
        logs.iter().map(folded).collect::<Vec<_>>(),
        [
            pair("mysql_shop.people_20260919", "-"),
            pair("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
            pair("mysql_shop.people_20260921", "mysql_shop.people_20260920"),
        ]
    );
    assert_eq!(
        ids(&deployment, &logs, "mysql_shop.people_20260921"),
        [7.into(), 8.into()]
    );
}

fn spans(logs: &[BuildLog]) -> Vec<(String, String)> {
    logs.iter()
        .map(|log| {
            let day = |at: &Ts<Event>| DateTime::<Utc>::from(*at).format("%Y-%m-%d").to_string();

            (
                folded(log).0,
                format!("{}..{}", day(log.window().from()), day(log.window().to())),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_pipeline_built_for_the_first_time_builds_only_the_shard_closing_at_the_last_midnight() {
    let replica = people().insert(on("2026-09-21", 1), &table("people"), &[("id", 8.into())]);
    let deployment = Deployment::over(&replica).await;

    caught_up(&deployment).await;

    let logs = succeeded(logged(&deployment).await);
    assert_eq!(
        spans(&logs),
        [pair("mysql_shop.people_20260921", "1970-01-01..2026-09-22")]
    );
    assert_eq!(
        ids(&deployment, &logs, "mysql_shop.people_20260921"),
        [7.into(), 8.into()]
    );
}

#[tokio::test]
async fn a_pipeline_caught_up_to_a_date_builds_no_shard_dated_after_it() {
    let deployment = built_up_to_20260919(&people()).await;

    deployment
        .catch_up(&Target::default(), Some(date("2026-09-20")), now(NOW))
        .await
        .unwrap();

    assert_eq!(
        succeeded(logged(&deployment).await)
            .iter()
            .map(folded)
            .collect::<Vec<_>>(),
        [
            pair("mysql_shop.people_20260919", "-"),
            pair("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
        ]
    );
}

#[tokio::test]
async fn a_pipeline_built_for_the_first_time_up_to_a_date_is_caught_up_from_there_after() {
    let deployment = Deployment::over(&people()).await;

    deployment
        .catch_up(&Target::default(), Some(date("2026-09-20")), now(NOW))
        .await
        .unwrap();
    caught_up(&deployment).await;

    assert_eq!(
        spans(&succeeded(logged(&deployment).await)),
        [
            pair("mysql_shop.people_20260920", "1970-01-01..2026-09-21"),
            pair("mysql_shop.people_20260921", "2026-09-21..2026-09-22"),
        ]
    );
}

#[tokio::test]
async fn a_pipeline_is_not_caught_up_to_a_date_whose_shard_has_not_closed() {
    let deployment = Deployment::over(&people()).await;

    let unclosed = deployment
        .catch_up(&Target::default(), Some(date("2026-09-22")), now(NOW))
        .await
        .unwrap_err();

    assert!(matches!(unsteppable(unclosed), Unsteppable::Unclosed(_)));
}

#[tokio::test]
async fn a_pipeline_is_not_caught_up_to_a_date_before_its_source_began() {
    let deployment = Deployment::over(&people()).await;

    let unopened = deployment
        .catch_up(&Target::default(), Some(date("1969-12-31")), now(NOW))
        .await
        .unwrap_err();

    assert!(matches!(unsteppable(unopened), Unsteppable::Unopened(_)));
}

#[tokio::test]
async fn a_pipeline_is_not_caught_up_to_a_date_the_build_log_has_already_built_past() {
    let deployment = Deployment::over(&people()).await;
    deployment
        .catch_up(&Target::default(), Some(date("2026-09-20")), now(NOW))
        .await
        .unwrap();

    let overtaken = deployment
        .catch_up(&Target::default(), Some(date("2026-09-19")), now(NOW))
        .await
        .unwrap_err();

    assert!(matches!(
        unsteppable(overtaken),
        Unsteppable::Overtaken { date: overtaken, .. } if overtaken == date("2026-09-19")
    ));
}

#[tokio::test]
async fn a_pipeline_caught_up_to_the_date_it_was_last_built_to_builds_nothing() {
    let deployment = Deployment::over(&people()).await;
    deployment
        .catch_up(&Target::default(), Some(date("2026-09-20")), now(NOW))
        .await
        .unwrap();

    let built = deployment
        .catch_up(&Target::default(), Some(date("2026-09-20")), now(NOW))
        .await
        .unwrap();

    assert_eq!(built, []);
}

#[tokio::test]
async fn a_pipeline_built_for_the_first_time_before_its_first_midnight_builds_nothing() {
    let deployment = Deployment::over(&people()).await;

    let built = deployment
        .catch_up(&Target::default(), None, now("1970-01-01T23:00:00Z"))
        .await
        .unwrap();

    assert_eq!(built, []);
}

#[tokio::test]
async fn a_pipeline_that_has_caught_up_builds_nothing() {
    let deployment = built_up_to_20260919(&people()).await;

    let built = deployment
        .catch_up(&Target::default(), None, now("2026-09-20T23:00:00Z"))
        .await
        .unwrap();

    assert_eq!(built, []);
}
