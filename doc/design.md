# Design

What has been settled about how lithify is configured and how its artifacts are put together, as opposed to the [roadmap](roadmap.md), which tracks what is still to be done.

## Configuration

A pipeline is a directory of files, each answering one question about it.

| File | Answers | Holds |
| --- | --- | --- |
| `infra.toml` | What can be built and where it lies | The sources a change log is captured from, the warehouses it is sunk into, where in each warehouse the change log is read from, and the artifacts each sink builds, each with what it needs to know and where it is written to, such as the project and dataset in BigQuery or the bucket and prefix of a store |
| `table.toml` | What is to be made of each table | The artifacts each table is built into, named as `infra.toml` names them, and how its columns are treated in them, such as a mask |
| `owner.toml` | Who answers for the data | The team that owns each table and the people allowed to read each artifact |

A pipeline moves to another warehouse by rewriting `infra.toml` alone, and a table changes what is built of it by rewriting `table.toml` alone.

### Artifacts are declared by sink and chosen by table

An artifact is one of the kinds of table lithify builds: `snapshot`, `instant`, `masked`, `scd1` and `scd2`, and is named by its kind alone.
`infra.toml` declares the artifacts a sink builds under the names of their kinds, each with the series it advances by, how it names its tables and where each series is placed.
A series is what the build log records under one `artifact` and `schedule`: a `snapshot` has one, cut at `at`, an `instant` has one for each instant of the day it is cut at, and an `scd1` and an `scd2` have one each.
How a kind writes its series down and names its tables is its own: an `instant` lists its series as an array of tables, each with its instant and where it is placed, and a `snapshot`, an `instant` and a `masked` name a table for each shard by `shard`, while an `scd1` names one table after the table it is built from.

```toml
time_zone = "Asia/Tokyo"

[[sink]]
name = "bigquery"
warehouse.bigquery.project = "local-project"
warehouse.bigquery.endpoint = "http://bigquery-emulator:9050"
change_log.bigquery = { project = "raw-project", dataset = "mysql_shop_cdc" }

[sink.artifact.snapshot]
at = "00:00"
shard = "%Y%m%d"
place = [{ bigquery.dataset = "mysql_shop" }]

[[sink.artifact.instant]]
at = "09:00"
shard = "%Y%m%d"
place = [
  { bigquery.dataset = "mysql_shop_instant_0900" },
  { bigquery = { project = "analytics-project", dataset = "shop_instant_0900" } },
]

[[sink.artifact.instant]]
at = "18:00"
shard = "%Y%m%d"
place = [{ bigquery.dataset = "mysql_shop_instant_1800" }]

[sink.artifact.masked]
shard = "%Y%m%d"
place = [{ bigquery = { project = "analytics-project", dataset = "shop_masked" } }]

[sink.artifact.scd1]
place = [{ bigquery.dataset = "mysql_shop_scd1" }]

[[sink]]
name = "duckdb_s3"
warehouse.duckdb.s3.bucket = "lithify"
change_log.duckdb.prefix = "mysql_shop_cdc"

[sink.artifact.snapshot]
at = "00:00"
shard = "%Y%m%d"
place = [{ duckdb.prefix = "mysql_shop" }]

[sink.artifact.scd2]
place = [{ duckdb.prefix = "mysql_shop_scd2" }]
```

```toml
[default]
artifacts = ["snapshot", "scd1"]

[[table]]
schema = "shop"
name = "orders"
artifacts = ["snapshot", "instant", "masked", "scd2"]
column.attributes.mask = "null"
```

A sink builds an artifact of a table when the sink declares it and the table names it, so sinks may build different artifacts of the same table, and a table naming an artifact builds every series the sink declares of it.
A table naming an artifact no sink declares is refused.

An artifact declared under `default` is declared in every sink, and a sink declaring one of the same kind replaces it whole.

### Placement

The change log and each placement are written the way the warehouse of the sink is, under the name of the warehouse: a project and a dataset in BigQuery, the project defaulting to the one the warehouse names, and a prefix in the bucket of a store.
One way of writing where a table lies serves the change log and every placement alike, and a placement written for a warehouse other than the one of its sink is refused.
The change log and each series are placed on their own, so the series of one sink may lie in projects or datasets apart from the change log and from one another.

A series may be placed more than once, each placement holding the whole of it: it is built into the first placement and copied into the others, so every placement holds the same rows.

## Artifacts

### The build log advances daily

The build log closes a window once a day, at the instant the `snapshot` of the sink is cut at, which is midnight unless `snapshot.at` says otherwise, and at no other instant, and each daily shard folds onto the one before it.
Every instant of the day, of a `snapshot` and of an `instant` alike, is read in the `time_zone` of the pipeline.

The instant a `snapshot` is cut at and the time zone it is read in are fixed once a shard is built: each shard folds onto the one before it, so shards cut at another instant would fold onto a shard that never held the state they start from.

An artifact that holds the state at another instant of the day is an `instant`, which cuts within a daily window rather than closing one, and leaves what the build log folds onto untouched.

### An artifact knows what it is built from

Which artifact another is built from is the artifact's to know, not the configuration's:

- an `instant` folds the change log up to the instant it is cut at onto the `snapshot` before it;
- an `scd1` folds the change log after the latest `snapshot` onto it;
- a `masked` projects the `snapshot` of the same window;
- an `scd2` appends each window onto what the window before it left.

A sink declaring an artifact without the one it is built from is refused, and so is a table naming an artifact without the one it is built from.
A sink declares each kind at most once, so what an artifact is built from is never in doubt.

### The set of artifacts is open

No crate but the command line enumerates the kinds of artifact there are.
As a capture is configured by `CdcFormat::Config` and a warehouse by `OpenWarehouse::Config`, each kind of artifact defines its own configuration in the crate that implements it, and the command line gathers them into one enum the way it gathers `CaptureConfig` and `WarehouseConfig`, so another kind is added without touching the configuration crate.
The build log records each series by the kind of its artifact and by what makes it advance, such as the instant of the day it is cut at, so the `artifact` and `schedule` columns read whatever the artifact writes into them.
