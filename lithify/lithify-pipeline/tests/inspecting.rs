use chrono::DateTime;

use lithify_build::sink::Namespace;
use lithify_pipeline::Pipeline;
use lithify_pipeline::build_log::Series;
use lithify_pipeline::check::{Expectation, Inspecting, Uninspectable};
use lithify_pipeline::materialization::Materialization;
use lithify_pipeline::schedule::Timeline;
use lithify_pipeline::warehouse::SinkWarehouse;
use lithify_schema::ts::{Replay, Ts};
use lithify_testing::pipeline::{
    ArtifactConfig, CaptureConfig, INFRA, Kept, configure, logged, scratch,
};

fn opened(
    name: &str,
    infra: &str,
    artifacts: &str,
) -> Pipeline<CaptureConfig, Kept, ArtifactConfig> {
    let directory = scratch(name);
    configure(&directory, infra);
    std::fs::write(directory.join("artifact.toml"), artifacts).unwrap();

    Pipeline::open(&directory).unwrap()
}

fn now() -> Ts<Replay> {
    Ts::from(DateTime::UNIX_EPOCH)
}

fn inspecting(
    pipeline: &Pipeline<CaptureConfig, Kept, ArtifactConfig>,
) -> Result<Inspecting, Uninspectable> {
    let source = pipeline.sources().next().unwrap();
    let sink = pipeline.sinks().next().unwrap();

    Inspecting::of(sink, source, now())
}

const SNAPSHOT: &str = include_str!("../testdata/inspecting/snapshot.toml");

#[test]
fn a_sink_is_inspected_under_the_first_place_of_the_artifact_it_schedules() {
    let pipeline = opened("inspected-place", INFRA, SNAPSHOT);

    let inspecting = inspecting(&pipeline).unwrap();

    assert_eq!(
        inspecting.scheduled(),
        &Materialization::new(
            Namespace::from("mysql_shop"),
            Series::new("snapshot".into(), "daily"),
            None,
        )
    );
}

#[test]
fn a_sink_is_expected_to_be_built_since_its_source_is_captured_until_now_as_it_is_captured() {
    let pipeline = opened("expected", INFRA, SNAPSHOT);
    let source = pipeline.sources().next().unwrap();
    let sink = pipeline.sinks().next().unwrap();

    let expectation = Expectation::of(sink, source, now()).unwrap();

    assert_eq!(
        expectation.timeline(),
        &Timeline::new(sink.schedule(), source.since()..now().as_event())
    );
    assert_eq!(expectation.capture(), source.written_capture().unwrap());
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
