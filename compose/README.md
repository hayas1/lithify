# Local CDC stack

A local stack to run lithify against: MySQL / PostgreSQL changes are captured by Debezium, published to Kafka, and sunk into BigQuery (or its emulator) and S3 (or RustFS) as a raw change log.

## Services

| Service | Profile | Endpoint (inside `devcontainer-network`) | Host port |
| --- | --- | --- | --- |
| kafka | - | `kafka:9092` | `localhost:9094` |
| connect | - | `http://connect:8083` | 8083 |
| kafka-ui | - | `http://kafka-ui:8080` | 8080 |
| mysql | - | `mysql:3306` (root/root) | 3306 |
| postgres | - | `postgres:5432` (postgres/postgres) | 5432 |
| bigquery-emulator | `bigquery-emulator` | `http://bigquery-emulator:9050` (REST), `:9060` (gRPC) | 9050, 9060 |
| rustfs | `rustfs` | `http://rustfs:9000`, console `:9001` (rustfsadmin/rustfsadmin) | 9000, 9001 |
| duckdb | `cli` | run on demand | - |

Both source databases have a `shop` schema with `customers`, `orders` and `order_items` (`01-shop.sql`), and the Debezium user plus CDC settings (`02-debezium.sql`), see `compose/mysql/initdb.d` and `compose/postgres/initdb.d`.
Column types follow each engine's idiom (e.g. `customers.tags` is `JSON` in MySQL and `TEXT[]` in PostgreSQL); `updated_at` is maintained by `ON UPDATE` in MySQL and a trigger in PostgreSQL.

## Usage

Run from WSL (bind mounts are resolved against the host path).

```sh
docker network create devcontainer-network   # once
docker compose up -d --build
./e2e_test.sh
```

`compose/e2e_test.sh` waits for the connectors, changes both source databases, and asserts that the change log reaches BigQuery and DuckDB. `.github/workflows/test.yml` runs it, `compose/connect/register_test.sh`, `scripts/install-clients_test.sh` and the emulator patch's own tests on every push.

`compose/.env` holds every default. Override any of them with shell environment variables, which take precedence over `.env`.
`COMPOSE_PROFILES` selects the sinks: `bigquery-emulator` or `bigquery`, and `rustfs` or `s3`.
The `register-*` one-shot services register the connectors in `compose/connect/connectors` once Kafka Connect is healthy.
There is one sink connector per source database and sink, named `sink-<sink>-<source>`. Each is registered from the template `compose/connect/connectors/sink-<sink>.json`: `register.sh sink-bigquery:mysql` replaces `@SOURCE@` with `MYSQL`, so it subscribes to `CDC_MYSQL_TOPICS_REGEX` and writes to `BIGQUERY_MYSQL_DATASET` (`sink-s3:mysql` writes under `S3_MYSQL_PREFIX`).

`CDC_<SOURCE>_NAMESPACE` names the source and `CDC_<SOURCE>_LOG_NAMESPACE` is where its raw change log lands, as a dataset in BigQuery and as a prefix in S3. That leaves the source namespace free for what an application derives from the log.
Log tables keep the names the source connector and the sink give them, so `mysql_shop_cdc.mysql_shop_customers`, while each table is built into the artifacts `compose/lithify/<source>/table.toml` names for it, out of those `compose/lithify/<source>/artifact.toml` declares and `compose/lithify/<source>/infra.toml` places: every table reads as a daily snapshot such as `mysql_shop.heartbeat_20260920`, which is all a table kept for bookkeeping such as the heartbeat or the schema migrations needs, the customers and the products as their current state such as `mysql_shop_scd1.customers_scd_type_1` and as their versions of each day such as `mysql_shop_scd2.customers_closed_20260920` and `mysql_shop_scd2.customers_open_20260920`, and the orders as their versions of each day and as they stood at 09:00, 12:00 and 18:00 such as `mysql_shop_1200.orders_20260920`.
Run `compose/connect/register_test.sh` to test the registration script on its own.

Make some changes and look at the result (`docker compose` runs in `compose/`):

```sh
docker compose exec mysql mysql -uroot -proot shop -e "UPDATE customers SET name = 'Alice2' WHERE id = 1"

curl -s -X POST http://localhost:9050/bigquery/v2/projects/local-project/queries \
  -H 'Content-Type: application/json' \
  -d '{"useLegacySql": false, "query": "SELECT op, before.name, after.name, _kafka.offset FROM mysql_shop_cdc.mysql_shop_customers ORDER BY _kafka.offset"}'

docker compose run --rm duckdb
# D SELECT op, before.name, after.name, _kafka.offset FROM cdc_events('mysql_shop_cdc', 'mysql.shop.customers');
```

Sinks commit every 10 seconds, so a change shows up there with that much delay.

## Local clients

`scripts/install-clients.sh` installs `mysql`, `psql`, `duckdb` and `bq`, skipping the ones already on PATH. Pass `-y` to install without asking, which is what an automated setup such as a devcontainer `postCreateCommand` wants. Name some of them to install those alone, as CI does with `libduckdb` for the Rust workspace. `scripts/install-clients_test.sh` tests the script.

From a shell on `devcontainer-network` the services are reachable by name (the last one runs in `compose/`):

```sh
mysql -h mysql -uroot -proot --ssl-verify-server-cert=0 shop -e "INSERT INTO customers (email, name) VALUES ('dave@example.com', 'Dave')"
PGPASSWORD=postgres psql -h postgres -U postgres shop -c "UPDATE shop.customers SET tags = '{vip,gold}' WHERE id = 1"

bq --api=http://bigquery-emulator:9050 --project_id=local-project query --use_legacy_sql=false \
  "SELECT op, after.name, _kafka.offset FROM mysql_shop_cdc.mysql_shop_customers ORDER BY _kafka.offset"

S3_ENDPOINT=http://rustfs:9000 S3_REGION=us-east-1 S3_BUCKET=lithify S3_ACCESS_KEY_ID=rustfsadmin S3_SECRET_ACCESS_KEY=rustfsadmin \
  duckdb -init duckdb/cdc.sql -c "SELECT op, after.name, _kafka.offset FROM cdc_events('mysql_shop_cdc', 'mysql.shop.customers')"
```

The MySQL client is MariaDB's, which does not trust the server's self-signed certificate, hence `--ssl-verify-server-cert=0`.

Reset everything with `docker compose down -v`.

## Change log shape

The Debezium envelope is kept as is (no `ExtractNewRecordState`), so every sink row carries `op` (`r` snapshot / `c` / `u` / `d`), `before`, `after`, `source` (binlog position, LSN, ...) and `ts_ms`.
Deletes are plain `op = d` events; tombstones are disabled.
Decimals are emitted as strings (`decimal.handling.mode=string`). PostgreSQL tables use `REPLICA IDENTITY FULL` so `before` is always complete.
The MySQL connector includes column comments in the schema changes it logs (`include.schema.comments=true`).

| Sink | Table | Extra columns |
| --- | --- | --- |
| BigQuery | `<BIGQUERY_<SOURCE>_DATASET>.<topic with . replaced by _>` (e.g. `mysql_shop_cdc.mysql_shop_customers`), ingestion-time partitioned by day (`_PARTITIONTIME`) | `_kafka_key` (primary key struct), `_kafka` (`topic`, `partition`, `offset`, `insertTime`) |
| S3 / DuckDB | `s3://<S3_BUCKET>/<S3_<SOURCE>_PREFIX>/<topic>/<partition>-<start offset>.parquet` (e.g. `mysql_shop_cdc/mysql.shop.customers/`) | Parquet columns `key`, `value`, `offset`, `timestamp`; `cdc_events(log_namespace, topic)` in DuckDB returns the BigQuery column layout, except that `_kafka.timestamp` (the Kafka record time) stands in for `_kafka.insertTime` |

## BigQuery emulator

The Aiven BigQuery sink has no setting for the API endpoint, so the emulator is reached by impersonating Google's hosts on a network only Kafka Connect joins:

- `bigquery-proxy` (Caddy) holds the network aliases `bigquery.googleapis.com` and `oauth2.googleapis.com` on the internal `bigquery-emulator` network. `devcontainer-network` is not affected.
- `bigquery-proxy-cert` generates a self-signed certificate, which `compose/connect/entrypoint.sh` adds to the JVM trust store.
- `oauth2.googleapis.com` answers with a dummy token for the dummy credentials in `compose/bigquery-proxy/application_default_credentials.json`.
- `bigquery-emulator-init` creates the datasets, since the connector does not.
- `bigquery-emulator` is built from the pinned upstream source with `compose/bigquery-emulator/patches` applied, because the released emulator is not compatible enough for the connector. One patch per fix, each with its own tests, which the image build runs and counts:
  - `0001-quote-nested-struct-field-names.patch` quotes nested STRUCT field names in the generated DDL, so a field named after a reserved word works. The connector's `kafkaDataFieldName` struct has `partition`. Upstream closed the reports (goccy/bigquery-emulator#402, #414) as fixed in v0.7.0, but v0.8.1 still reproduces them; drop this patch once upstream fixes it.
  - `0002-report-missing-tables-like-bigquery.patch` reports missing tables and datasets in BigQuery's wording (`Not found: Table <project>:<dataset>.<table>`), which the connector matches to decide that a table has to be created. Its test reuses the client helper `0001` adds, so the patches apply in order.

The emulator's `_PARTITIONTIME` is the query time rather than a stored value, and it rejects partition decorators (`table$YYYYMMDD`), so `bigQueryPartitionDecorator` is off. Real BigQuery assigns streamed rows to the same ingestion-day partition without it.

## Real BigQuery

```sh
export COMPOSE_PROFILES=bigquery,rustfs
export BIGQUERY_PROJECT=<project>
export BIGQUERY_MYSQL_DATASET=<existing dataset>
export BIGQUERY_POSTGRES_DATASET=<existing dataset>
export GCP_CREDENTIALS_FILE=~/.config/gcloud/application_default_credentials.json   # or a service account key
cd compose && docker compose up -d --build
```

Real S3 works the same way with the `s3` profile instead of `rustfs` and the `S3_*` variables.

## Deferred

Iceberg (Iceberg Kafka Connect sink + REST catalog on RustFS) and Snowflake (Snowpipe Streaming) are prepared but commented out in `compose/connect/Dockerfile` and `compose/compose.yaml`. `compose/connect/connectors/sink-snowflake.json` is untested.
