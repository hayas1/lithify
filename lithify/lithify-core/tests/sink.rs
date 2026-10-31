use lithify_core::sink::*;
use lithify_core::sql::SqlString;
use lithify_testing::Engine;

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
