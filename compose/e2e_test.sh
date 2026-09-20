#!/bin/sh
set -eu

root=$(cd "$(dirname "$0")" && pwd)

compose() {
    docker compose --project-directory "${root}" "$@"
}

connect_setting() {
    compose exec -T connect printenv "$1"
}

connect_api() {
    compose exec -T connect curl -fsS "http://localhost:8083$1"
}

bigquery_query() {
    compose exec -T connect curl -fsS -X POST \
        -H 'Content-Type: application/json' \
        -d "{\"useLegacySql\": false, \"query\": \"$1\"}" \
        "http://bigquery-emulator:9050/bigquery/v2/projects/${bigquery_project}/queries"
}

bigquery_scalar() {
    bigquery_query "$1" | tr -d '\n' | sed 's/.*"rows":\[{"f":\[{"v":"//; s/"}.*//'
}

bigquery_rows_contain() {
    bigquery_query "$1" | grep -q "$2"
}

duckdb_scalar() {
    compose run --rm -T duckdb /duckdb -init /cdc.sql -noheader -list -c "$1" | tr -d '\r' | tail -1
}

duckdb_scalar_is() {
    [ "$(duckdb_scalar "$1")" = "$2" ]
}

running_connectors() {
    [ "$(connect_api '/connectors?expand=status' | grep -o '"connector":{"state":"RUNNING"' | wc -l)" -eq "$1" ]
}

wait_for() {
    what="$1"
    shift
    attempts=0
    until "$@"; do
        attempts=$((attempts + 1))
        if [ "${attempts}" -ge 60 ]; then
            echo "FAIL: timed out waiting for ${what}" >&2
            exit 1
        fi
        sleep 5
    done
    echo "ok: ${what}"
}

assert_equals() {
    what="$1"
    expected="$2"
    actual="$3"
    if [ "${actual}" != "${expected}" ]; then
        echo "FAIL: ${what} = ${actual}, want ${expected}" >&2
        exit 1
    fi
    echo "ok: ${what}"
}

wait_for "connectors to run" running_connectors 6

bigquery_project=$(connect_setting BIGQUERY_PROJECT)
mysql_dataset=$(connect_setting BIGQUERY_MYSQL_DATASET)
postgres_dataset=$(connect_setting BIGQUERY_POSTGRES_DATASET)
mysql_log_namespace=$(connect_setting S3_MYSQL_PREFIX)
postgres_log_namespace=$(connect_setting S3_POSTGRES_PREFIX)

compose exec -T -e MYSQL_PWD=root mysql mysql -uroot shop -e "UPDATE customers SET name = 'Alice2' WHERE id = 1"
compose exec -T postgres psql -U postgres -q shop -c "DELETE FROM shop.order_items WHERE order_id = 2"

wait_for "the MySQL update in BigQuery" \
    bigquery_rows_contain "SELECT after.name FROM ${mysql_dataset}.mysql_shop_customers WHERE op = 'u'" Alice2
wait_for "the PostgreSQL delete in BigQuery" \
    bigquery_rows_contain "SELECT _kafka.offset FROM ${postgres_dataset}.postgres_shop_order_items WHERE op = 'd'" '"v"'

assert_equals "the MySQL change log in BigQuery" "r,r,u" "$(
    bigquery_scalar "SELECT STRING_AGG(op, ',' ORDER BY _kafka.offset) FROM ${mysql_dataset}.mysql_shop_customers"
)"

assert_equals "the PostgreSQL change log in BigQuery" "r,r,d" "$(
    bigquery_scalar "SELECT STRING_AGG(op, ',' ORDER BY _kafka.offset) FROM ${postgres_dataset}.postgres_shop_order_items"
)"

wait_for "the MySQL change log in DuckDB" \
    duckdb_scalar_is "SELECT string_agg(op, ',' ORDER BY _kafka.offset) FROM cdc_events('${mysql_log_namespace}', 'mysql.shop.customers')" "r,r,u"
wait_for "the PostgreSQL change log in DuckDB" \
    duckdb_scalar_is "SELECT string_agg(op, ',' ORDER BY _kafka.offset) FROM cdc_events('${postgres_log_namespace}', 'postgres.shop.order_items')" "r,r,d"

assert_equals "the Kafka coordinates of the MySQL update in BigQuery" "mysql.shop.customers:0:2" \
    "$(bigquery_scalar "SELECT CONCAT(_kafka.topic, ':', CAST(_kafka.partition AS STRING), ':', CAST(_kafka.offset AS STRING)) FROM ${mysql_dataset}.mysql_shop_customers WHERE op = 'u'")"

assert_equals "the Kafka coordinates of the MySQL update in DuckDB" "mysql.shop.customers:0:2" \
    "$(duckdb_scalar "SELECT _kafka.topic || ':' || _kafka.partition || ':' || _kafka.offset FROM cdc_events('${mysql_log_namespace}', 'mysql.shop.customers') WHERE op = 'u'")"

echo "e2e_test.sh passed"
