use lithify_core::artifact::UnbuildableArtifact;
use lithify_core::build_log::{Base, Status, TableBuild};
use lithify_core::model::TableId;
use lithify_core::sink::*;
use lithify_core::sql::SqlString;
use lithify_core::ts::Ts;
use lithify_core::window::Window;
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

#[test]
fn a_table_is_created_whole_from_what_a_select_reads_unless_the_warehouse_says_otherwise() {
    let people = TablePath::new("shop", "people");

    assert_eq!(
        Engine::default().create_table_as(&people, &SqlString::from("SELECT 1 AS id")),
        "CREATE OR REPLACE TABLE \"shop\".\"people\" AS\nSELECT 1 AS id"
    );
    assert_eq!(
        Engine::default().create_table(
            &people,
            &[(SqlString::from("\"id\""), SqlString::from("BIGINT"))]
        ),
        "CREATE TABLE IF NOT EXISTS \"shop\".\"people\" (\"id\" BIGINT)"
    );
    assert_eq!(
        Engine::default().insert(
            &people,
            &[SqlString::from("\"id\"")],
            &SqlString::from("SELECT 1")
        ),
        "INSERT INTO \"shop\".\"people\" (\"id\")\nSELECT 1"
    );
}

#[test]
fn a_namespace_is_a_path_of_names_joined_by_dots() {
    let namespace = Namespace::from("analytics.shop");

    assert_eq!(namespace.parts().collect::<Vec<_>>(), ["analytics", "shop"]);
    assert_eq!(namespace.to_string(), "analytics.shop");
}

#[test]
fn a_table_is_written_after_the_namespace_it_lies_in() {
    assert_eq!(
        TablePath::new("analytics.shop", "people").to_string(),
        "analytics.shop.people"
    );
}

#[test]
fn a_change_log_is_read_from_the_topic_it_landed_on_in_the_namespace_of_the_warehouse() {
    assert_eq!(
        Engine::default().change_log_ref("mysql.shop.people"),
        Engine::default().table_ref(&TablePath::new("mysql_shop_cdc", "mysql.shop.people"))
    );
}
