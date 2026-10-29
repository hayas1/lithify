use std::path::Path;

use lithify_pipeline::pipelines::{PipelineLocation, Pipelines};
use lithify_testing::pipeline::{Captured, INFRA, Kept, Made, configure, scratch};

fn under(root: &Path) -> Pipelines<Captured, Kept, Made> {
    Pipelines::new(root.to_owned())
}

#[test]
fn the_pipelines_under_a_directory_are_located_in_the_order_of_their_names() {
    let root = scratch("ordered");
    configure(&root.join("postgres"), INFRA);
    configure(&root.join("mysql"), INFRA);

    assert_eq!(
        under(&root).locations().unwrap(),
        [
            PipelineLocation::new("mysql", &root.join("mysql").display().to_string()),
            PipelineLocation::new("postgres", &root.join("postgres").display().to_string())
        ]
    );
}

#[test]
fn a_pipeline_is_located_by_its_name() {
    let root = scratch("named");
    configure(&root.join("postgres"), INFRA);
    configure(&root.join("mysql"), INFRA);

    assert_eq!(
        under(&root).located("postgres").unwrap(),
        Some(PipelineLocation::new(
            "postgres",
            &root.join("postgres").display().to_string()
        ))
    );
}

#[test]
fn a_directory_configuring_no_pipeline_locates_none() {
    assert!(under(&scratch("empty")).locations().is_err());
}
