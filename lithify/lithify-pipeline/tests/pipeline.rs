use std::path::{Path, PathBuf};

use lithify_core::model::TableId;
use lithify_core::sink::Namespace;
use lithify_pipeline::infra::Miswiring;
use lithify_pipeline::table::{Mask, Mislisting};
use lithify_pipeline::{Cause, Misdeclaring, Pipeline, Unreadable};
use lithify_testing::pipeline::{ArtifactConfig, CaptureConfig, Kept, scratch};

const ARTIFACTS: &str = include_str!("../testdata/pipeline/artifacts.toml");

const SOURCE: &str = include_str!("../testdata/pipeline/source.toml");

const SINK: &str = include_str!("../testdata/pipeline/sink.toml");

const TABLES: &str = include_str!("../testdata/pipeline/table.toml");

type Opened = Result<Pipeline<CaptureConfig, Kept, ArtifactConfig>, Unreadable>;

fn open(directory: &Path) -> Opened {
    Pipeline::open(directory)
}

fn write(directory: &Path, file: &str, contents: &str) {
    std::fs::create_dir_all(directory).unwrap();
    std::fs::write(directory.join(file), contents).unwrap();
}

fn configure(directory: &Path, infra: &str, listed: &str) {
    write(directory, "infra.toml", infra);
    write(directory, "artifact.toml", ARTIFACTS);
    write(directory, "table.toml", listed);
}

fn placing_noons() -> String {
    format!("{SOURCE}{SINK}place.noon = {{ prefix = \"mysql_shop_noon\" }}\n")
}

fn configured(name: &str) -> PathBuf {
    let directory = scratch(name).join(name);
    configure(&directory, &placing_noons(), TABLES);

    directory
}

fn listing(name: &str, listed: &str) -> Opened {
    let directory = scratch(name);
    configure(&directory, &placing_noons(), listed);

    open(&directory)
}

#[test]
fn a_pipeline_is_named_after_the_directory_that_configures_it() {
    assert_eq!(open(&configured("mysql")).unwrap().name(), "mysql");
    assert_eq!(open(&configured("postgres")).unwrap().name(), "postgres");
}

#[test]
fn a_directory_that_configures_a_pipeline_is_the_one_pipeline_it_holds() {
    let directory = configured("one-pipeline");

    assert_eq!(
        Pipeline::<CaptureConfig, Kept, ArtifactConfig>::directories(&directory).unwrap(),
        [directory]
    );
}

#[test]
fn a_directory_holding_pipelines_holds_each_one_configured_in_it_in_the_order_of_their_names() {
    let root = scratch("pipelines");
    for configured in ["postgres", "mysql"] {
        write(&root.join(configured), "infra.toml", "");
    }
    std::fs::create_dir_all(root.join("dry-run")).unwrap();

    assert_eq!(
        Pipeline::<CaptureConfig, Kept, ArtifactConfig>::directories(&root).unwrap(),
        [root.join("mysql"), root.join("postgres")]
    );
}

#[test]
fn a_pipeline_says_the_namespace_each_sink_holds_the_change_logs_in() {
    let pipeline = open(&configured("change-logs")).unwrap();

    let change_logs: Vec<_> = pipeline
        .sinks()
        .map(|sink| Namespace::from(sink.change_log().clone()))
        .collect();

    assert_eq!(change_logs, [Namespace::from("mysql_shop_cdc")]);
}

#[test]
fn a_table_the_pipeline_does_not_configure_takes_the_defaults() {
    let pipeline = open(&configured("defaults")).unwrap();
    let table = pipeline.tables().table(&TableId::new("shop", "heartbeat"));

    assert!(table.builds(&"snapshot".into()));
    assert!(!table.builds(&"noon".into()));
    assert_eq!(table.mask("beat_at"), Mask::Identity);
}

#[test]
fn a_table_the_pipeline_configures_overrides_the_defaults() {
    let pipeline = open(&configured("overrides")).unwrap();
    let table = pipeline.tables().table(&TableId::new("shop", "orders"));

    assert!(table.builds(&"noon".into()));
    assert_eq!(table.mask("attributes"), Mask::Null);
    assert_eq!(table.mask("amount"), Mask::Identity);
}

#[test]
fn a_directory_that_configures_no_pipeline_is_reported_with_the_file_it_misses() {
    let unreadable = open(Path::new("/nowhere")).unwrap_err();

    assert!(matches!(unreadable.cause(), Cause::Missing(_)));
    assert_eq!(unreadable.path(), Path::new("/nowhere/infra.toml"));
    assert!(unreadable.to_string().contains("/nowhere/infra.toml"));
}

#[test]
fn a_pipeline_that_configures_no_table_is_reported_with_the_file_it_misses() {
    let directory = scratch("pipeline-without-tables");
    write(&directory, "infra.toml", &format!("{SOURCE}{SINK}"));
    write(&directory, "artifact.toml", ARTIFACTS);

    let unreadable = open(&directory).unwrap_err();

    assert!(matches!(unreadable.cause(), Cause::Missing(_)));
    assert_eq!(unreadable.path(), directory.join("table.toml"));
}

#[test]
fn a_pipeline_captured_from_no_server_is_reported_with_the_file_that_wires_it() {
    let directory = scratch("pipeline-without-servers");
    configure(&directory, SINK, "");

    let unreadable = open(&directory).unwrap_err();

    assert!(matches!(
        unreadable.cause(),
        Cause::Miswiring(Miswiring::WithoutSources)
    ));
    assert_eq!(unreadable.path(), directory.join("infra.toml"));
    assert_eq!(
        unreadable.to_string(),
        format!(
            "{}: no server is captured",
            directory.join("infra.toml").display()
        )
    );
}

#[test]
fn a_table_listed_twice_is_reported_with_the_file_that_lists_it() {
    let directory = scratch("pipeline-with-a-table-twice");
    configure(
        &directory,
        &format!("{SOURCE}{SINK}"),
        "[[table]]\nschema = \"shop\"\nname = \"orders\"\n\n[[table]]\nschema = \"shop\"\nname = \"orders\"\n",
    );

    let unreadable = open(&directory).unwrap_err();

    assert!(matches!(unreadable.cause(), Cause::Mislisting(_)));
    assert_eq!(unreadable.path(), directory.join("table.toml"));
}

#[test]
fn a_table_built_into_an_artifact_no_sink_declares_is_rejected() {
    let unreadable = listing(
        "pipeline-with-an-undeclared-artifact",
        "[[table]]\nschema = \"shop\"\nname = \"orders\"\nartifacts = [\"scd2\"]\n",
    )
    .unwrap_err();

    assert!(matches!(
        unreadable.cause(),
        Cause::Mislisting(Mislisting::Undeclared { table: Some(_), artifact }) if artifact.as_str() == "scd2"
    ));
}

#[test]
fn a_table_built_into_an_artifact_declared_but_placed_by_no_sink_is_rejected() {
    let directory = scratch("pipeline-with-an-unplaced-artifact");
    configure(
        &directory,
        &format!("{SOURCE}{SINK}"),
        "[default]\nartifacts = [\"snapshot\", \"noon\"]\n",
    );

    let unreadable = open(&directory).unwrap_err();

    assert!(matches!(
        unreadable.cause(),
        Cause::Mislisting(Mislisting::Undeclared { table: None, artifact }) if artifact.as_str() == "noon"
    ));
}

#[test]
fn a_table_built_into_an_artifact_but_not_into_the_one_it_is_built_from_is_rejected() {
    let unreadable = listing(
        "pipeline-without-a-source",
        "[default]\nartifacts = [\"noon\"]\n",
    )
    .unwrap_err();

    assert!(matches!(
        unreadable.cause(),
        Cause::Mislisting(Mislisting::WithoutBase { table: None, artifact, base })
            if artifact.as_str() == "noon" && base.as_str() == "snapshot"
    ));
}

#[test]
fn a_table_built_into_an_artifact_and_the_one_it_is_built_from_is_read() {
    assert!(
        listing(
            "pipeline-with-a-source",
            "[default]\nartifacts = [\"snapshot\", \"noon\"]\n",
        )
        .is_ok()
    );
}

#[test]
fn a_configuration_that_does_not_parse_is_reported_with_its_file() {
    let directory = scratch("unparsable-pipeline");
    write(&directory, "infra.toml", "capture = 1");

    let unreadable = open(&directory).unwrap_err();

    assert!(matches!(unreadable.cause(), Cause::Malformed(_)));
    assert_eq!(unreadable.path(), directory.join("infra.toml"));
}

#[test]
fn a_pipeline_that_declares_no_artifact_is_reported_with_the_file_it_misses() {
    let directory = scratch("pipeline-without-artifacts");
    write(&directory, "infra.toml", &format!("{SOURCE}{SINK}"));
    write(&directory, "table.toml", TABLES);

    let unreadable = open(&directory).unwrap_err();

    assert!(matches!(unreadable.cause(), Cause::Missing(_)));
    assert_eq!(unreadable.path(), directory.join("artifact.toml"));
}

#[test]
fn an_artifact_declared_twice_is_reported_with_the_file_that_declares_it() {
    let directory = scratch("pipeline-with-an-artifact-twice");
    configure(&directory, &format!("{SOURCE}{SINK}"), "");
    write(
        &directory,
        "artifact.toml",
        &format!(
            "{ARTIFACTS}\n[[artifact]]\nname = \"noon\"\nkind = \"instant\"\nbase = \"snapshot\"\nat = \"12:00\"\nshard = \"%Y%m%d\"\n"
        ),
    );

    let unreadable = open(&directory).unwrap_err();

    assert!(matches!(
        unreadable.cause(),
        Cause::Misdeclaring(Misdeclaring::NamedTwice(artifact)) if artifact.as_str() == "noon"
    ));
    assert_eq!(unreadable.path(), directory.join("artifact.toml"));
}

#[test]
fn a_place_naming_an_artifact_declared_nowhere_is_reported_with_the_file_that_wires_it() {
    let directory = scratch("pipeline-placing-an-undeclared-artifact");
    configure(
        &directory,
        &format!("{SOURCE}{SINK}place.history = {{ prefix = \"mysql_shop_scd2\" }}\n"),
        "",
    );

    let unreadable = open(&directory).unwrap_err();

    assert!(matches!(
        unreadable.cause(),
        Cause::Miswiring(Miswiring::Undeclared(sink, artifact)) if sink == "rustfs" && artifact.as_str() == "history"
    ));
    assert_eq!(unreadable.path(), directory.join("infra.toml"));
}
