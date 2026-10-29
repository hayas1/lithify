use chrono::DateTime;

use lithify_core::sink::Namespace;
use lithify_core::ts::{Replay, Ts};
use lithify_pipeline::Pipeline;
use lithify_pipeline::build_log::Series;
use lithify_pipeline::check::{Inspecting, Uninspectable};
use lithify_pipeline::warehouse::SinkWarehouse;
use lithify_testing::pipeline::{Captured, INFRA, Kept, Made, configure, logged, scratch};

fn opened(name: &str, infra: &str, artifacts: &str) -> Pipeline<Captured, Kept, Made> {
    let directory = scratch(name);
    configure(&directory, infra);
    std::fs::write(directory.join("artifact.toml"), artifacts).unwrap();

    Pipeline::open(&directory).unwrap()
}

fn now() -> Ts<Replay> {
    Ts::from(DateTime::UNIX_EPOCH)
}

fn inspecting(pipeline: &Pipeline<Captured, Kept, Made>) -> Result<Inspecting, Uninspectable> {
    let source = pipeline.sources().next().unwrap();
    let sink = pipeline.sinks().next().unwrap();

    Inspecting::of(sink, source, now())
}

const SNAPSHOT: &str = include_str!("../testdata/inspecting/snapshot.toml");

#[test]
fn a_sink_is_inspected_under_the_first_place_of_the_artifact_it_schedules() {
    let pipeline = opened("inspected-place", INFRA, SNAPSHOT);

    let inspecting = inspecting(&pipeline).unwrap();

    assert_eq!(inspecting.namespace(), &Namespace::from("mysql_shop"));
    assert_eq!(
        inspecting.series(),
        &Series::new("snapshot".into(), "daily")
    );
}

#[test]
fn a_sink_scheduling_two_artifacts_is_not_told_apart_by_a_check() {
    let pipeline = opened(
        "inspected-twice",
        &INFRA.replace(
            "place.snapshot = [{ prefix = \"mysql_shop\" }]",
            "place.snapshot = [{ prefix = \"mysql_shop\" }]\n    place.copy = [{ prefix = \"mysql_copy\" }]",
        ),
        &format!("{SNAPSHOT}[[artifact]]\nname = \"copy\"\nkind = \"snapshot\"\n"),
    );

    assert_eq!(
        inspecting(&pipeline).err(),
        Some(Uninspectable::ScheduledTwice("rustfs".to_owned()))
    );
}

#[tokio::test]
async fn a_sink_is_inspected_by_the_build_log_its_warehouse_holds() {
    let pipeline = opened("inspected-logged", &logged(), SNAPSHOT);
    let sink = pipeline.sinks().next().unwrap();
    let inspecting = inspecting(&pipeline).unwrap();

    let inspection = sink
        .warehouse()
        .with_client(sink.change_log(), &(), &inspecting)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(inspection.latest(), Kept::logs());
}
