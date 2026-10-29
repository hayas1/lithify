use ulid::Ulid;

use chrono::{DateTime, Utc};

use std::collections::BTreeSet;

use lithify_core::model::TableId;
use lithify_core::sink::{Namespace, TablePath};
use lithify_core::ts::Ts;
use lithify_core::window::Window;
use lithify_pipeline::build_log::{BuildLog, Run, Series, Status, TableBuilt};
use lithify_pipeline::check::*;
use lithify_pipeline::schedule::Schedule;
use lithify_testing::{Engine, Recorder};

fn at<A>(rfc3339: &str) -> Ts<A> {
    Ts::from(rfc3339.parse::<DateTime<Utc>>().unwrap())
}

fn daily() -> Series {
    Series::new("snapshot".into(), "daily")
}

fn day(date: &str) -> Window {
    let from = at(&format!("{date}T00:00:00Z"));

    Window::new(
        from,
        Ts::try_from(from.milliseconds_since_epoch() + 86_400_000).unwrap(),
    )
}

fn logged(run: u64, table: &str, window: Window, status: Status) -> BuildLog {
    TableBuilt::new(
        Some(TableId::new("shop", table)),
        Some(TableId::new("shop", table)),
        None,
        Some(TablePath::new(
            "mysql_shop",
            &format!("{table}_{}", window.from().milliseconds_since_epoch()),
        )),
        window,
        status,
        Ts::from(DateTime::from_timestamp_millis(0).unwrap()),
    )
    .logged(&Run::new(
        Ulid::from_parts(run, 0),
        Ts::from(DateTime::UNIX_EPOCH),
        &"snapshot".into(),
        "daily",
        "primary",
        "{}",
        "0.1.0",
    ))
}

fn expected(now: &str) -> Expectation {
    Expectation::new(
        Schedule::default(),
        at("2026-09-19T00:00:00Z"),
        "{}",
        at(now),
    )
}

fn present(logs: &[BuildLog]) -> BTreeSet<TablePath> {
    logs.iter().filter_map(BuildLog::target).cloned().collect()
}

#[test]
fn a_pipeline_built_up_to_the_last_closed_window_holds_no_discrepancy() {
    let people = logged(2, "people", day("2026-09-20"), Status::Succeeded);
    let built = [people];

    assert_eq!(
        expected("2026-09-21T05:00:00Z").discrepancies(&built, &built, &present(&built)),
        []
    );
}

#[test]
fn a_pipeline_never_built_is_due_once_a_window_has_closed() {
    assert_eq!(
        expected("2026-09-21T05:00:00Z").discrepancies(&[], &[], &BTreeSet::new()),
        [Discrepancy::NeverBuilt {
            since: at("2026-09-19T00:00:00Z")
        }]
    );
    assert_eq!(
        expected("2026-09-19T05:00:00Z").discrepancies(&[], &[], &BTreeSet::new()),
        []
    );
}

#[test]
fn a_table_built_short_of_the_last_closed_window_is_overdue() {
    let people = logged(2, "people", day("2026-09-19"), Status::Succeeded);
    let built = [people];

    assert_eq!(
        expected("2026-09-21T05:00:00Z").discrepancies(&built, &built, &present(&built)),
        [Discrepancy::Overdue {
            table: TableId::new("shop", "people"),
            built_until: at("2026-09-20T00:00:00Z"),
            due_until: at("2026-09-21T00:00:00Z"),
        }]
    );
}

#[test]
fn a_table_whose_last_build_failed_is_reported_as_failed_rather_than_overdue() {
    let before = logged(1, "people", day("2026-09-19"), Status::Succeeded);
    let failed = logged(
        2,
        "people",
        day("2026-09-20"),
        Status::Failed("no era".to_owned()),
    );

    assert_eq!(
        expected("2026-09-21T05:00:00Z").discrepancies(
            std::slice::from_ref(&failed),
            std::slice::from_ref(&before),
            &present(std::slice::from_ref(&before))
        ),
        [Discrepancy::Failed {
            table: TableId::new("shop", "people"),
            window: day("2026-09-20"),
            error: "no era".to_owned(),
        }]
    );
}

#[test]
fn a_pipeline_whose_capture_changed_since_its_last_run_is_reported_as_such() {
    let people = logged(2, "people", day("2026-09-20"), Status::Succeeded);
    let built = [people];

    assert_eq!(
        Expectation::new(
            Schedule::default(),
            at("2026-09-19T00:00:00Z"),
            r#"{"debezium":{"dialect":"postgres"}}"#,
            at("2026-09-21T05:00:00Z")
        )
        .discrepancies(&built, &built, &present(&built)),
        [Discrepancy::CaptureChanged {
            logged: "{}".to_owned(),
            configured: r#"{"debezium":{"dialect":"postgres"}}"#.to_owned(),
        }]
    );
}

#[test]
fn a_table_logged_as_built_whose_target_is_gone_is_missing() {
    let people = logged(2, "people", day("2026-09-20"), Status::Succeeded);
    let built = [people.clone()];

    assert_eq!(
        expected("2026-09-21T05:00:00Z").discrepancies(&built, &built, &BTreeSet::new()),
        [Discrepancy::Missing {
            table: TableId::new("shop", "people"),
            target: people.target().unwrap().clone(),
        }]
    );
}

#[test]
fn a_table_the_last_run_did_not_build_is_still_expected() {
    let left = logged(1, "pets", day("2026-09-19"), Status::Succeeded);
    let people = logged(2, "people", day("2026-09-20"), Status::Succeeded);
    let built = [left, people];

    assert_eq!(
        expected("2026-09-21T05:00:00Z").discrepancies(&built, &built, &present(&built)),
        [Discrepancy::Overdue {
            table: TableId::new("shop", "pets"),
            built_until: at("2026-09-20T00:00:00Z"),
            due_until: at("2026-09-21T00:00:00Z"),
        }]
    );
}

#[test]
fn a_discrepancy_reads_as_what_went_wrong_with_which_table() {
    assert_eq!(
        Discrepancy::Overdue {
            table: TableId::new("shop", "people"),
            built_until: at("2026-09-20T00:00:00Z"),
            due_until: at("2026-09-21T00:00:00Z"),
        }
        .to_string(),
        "shop.people is built until 2026-09-20T00:00:00+00:00 but due until 2026-09-21T00:00:00+00:00"
    );
}

#[tokio::test]
async fn a_warehouse_is_checked_against_the_tables_it_holds() {
    let people = logged(2, "people", day("2026-09-20"), Status::Succeeded);
    let pets = logged(2, "pets", day("2026-09-20"), Status::Succeeded);
    let client = Recorder::new(Engine::default())
        .logging(
            &Namespace::from("mysql_shop"),
            &[people.clone(), pets.clone()],
        )
        .holding(&[people.target().unwrap().clone()]);

    assert_eq!(
        expected("2026-09-21T05:00:00Z")
            .read::<_, Box<dyn std::error::Error>>(
                &client,
                &Namespace::from("mysql_shop"),
                &daily()
            )
            .await
            .unwrap()
            .discrepancies(),
        [Discrepancy::Missing {
            table: TableId::new("shop", "pets"),
            target: pets.target().unwrap().clone(),
        }]
    );
}

#[tokio::test]
async fn a_warehouse_holding_no_build_log_was_never_built() {
    assert_eq!(
        expected("2026-09-21T05:00:00Z")
            .read::<_, Box<dyn std::error::Error>>(
                &Recorder::new(Engine::default()),
                &Namespace::from("mysql_shop"),
                &daily()
            )
            .await
            .unwrap()
            .discrepancies(),
        [Discrepancy::NeverBuilt {
            since: at("2026-09-19T00:00:00Z")
        }]
    );
}

#[tokio::test]
async fn a_warehouse_is_inspected_for_the_latest_build_of_each_table() {
    let failed = logged(
        3,
        "people",
        day("2026-09-20"),
        Status::Failed("no such column".to_owned()),
    );

    assert_eq!(
        expected("2026-09-21T05:00:00Z")
            .read::<_, Box<dyn std::error::Error>>(
                &Recorder::new(Engine::default()).logging(
                    &Namespace::from("mysql_shop"),
                    std::slice::from_ref(&failed)
                ),
                &Namespace::from("mysql_shop"),
                &daily()
            )
            .await
            .unwrap()
            .latest(),
        [failed]
    );
}
