use lithify_core::artifact::UnbuildableArtifact;
use lithify_core::build::{Base, TableBuild};
use lithify_core::model::TableId;
use lithify_core::sink::TablePath;
use lithify_core::sql::SqlString;
use lithify_core::ts::Ts;
use lithify_core::window::Window;
use lithify_pipeline::build_log::Status;
use lithify_pipeline::warehouse::WarehouseControl;
use lithify_testing::{Engine, Recorder};

fn people(base: Base, making: Result<Vec<SqlString>, UnbuildableArtifact>) -> TableBuild {
    TableBuild::new(
        Some(TableId::new("shop", "people")),
        Some(TableId::new("shop", "people")),
        base,
        Some(TablePath::new("shop", "people")),
        Window::until(Ts::try_from(1).unwrap()),
        making,
    )
}

fn statements() -> Vec<SqlString> {
    vec![
        SqlString::from("CREATE SCHEMA IF NOT EXISTS \"shop\""),
        SqlString::from("CREATE TABLE shop.people (id BIGINT)"),
    ]
}

#[tokio::test]
async fn a_client_builds_a_table_by_running_its_statements_in_order() {
    let client = Recorder::new(Engine::default());

    let built = client.build(people(Base::ChangeLog, Ok(statements())));

    assert_eq!(built.await.status(), &Status::Succeeded);
    assert_eq!(
        client.executed(),
        [
            "CREATE SCHEMA IF NOT EXISTS \"shop\"",
            "CREATE TABLE shop.people (id BIGINT)"
        ]
    );
}

#[tokio::test]
async fn a_client_builds_nothing_of_a_table_with_nothing_to_fold_onto() {
    let client = Recorder::new(Engine::default());

    let built = client.build(people(Base::Missing, Ok(statements())));

    assert_eq!(
        built.await.status(),
        &Status::Failed(
            UnbuildableArtifact::WithoutBase(TableId::new("shop", "people")).to_string()
        )
    );
    assert_eq!(client.executed(), Vec::<String>::new());
}

#[tokio::test]
async fn a_client_builds_nothing_of_a_table_its_builder_refused() {
    let client = Recorder::new(Engine::default());
    let refused = UnbuildableArtifact::PrimaryKeyChanged(TableId::new("shop", "people"));

    let built = client.build(people(Base::ChangeLog, Err(refused.clone())));

    assert_eq!(built.await.status(), &Status::Failed(refused.to_string()));
    assert_eq!(client.executed(), Vec::<String>::new());
}
