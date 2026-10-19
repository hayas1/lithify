# Design

What has been settled about how lithify is configured and how its artifacts are put together, as opposed to the [roadmap](roadmap.md), which tracks what is still to be done.

## Configuration

A pipeline is a directory of files, each answering one question about it.

| File | Answers | Holds |
| --- | --- | --- |
| `infra.toml` | What can be built and where it lies | The sources a change log is captured from, the artifacts the pipeline builds, each with what it needs to know, and the warehouses the change log is sunk into, each with where in it the change log is read from and each artifact is written to, such as the project and dataset in BigQuery or the prefix in the bucket of a store |
| `table.toml` | What is to be made of each table | The artifacts each table is built into, named as `infra.toml` names them, and how its columns are treated in them, such as a mask |
| `owner.toml` | Who answers for the data | The team that owns each table and the people allowed to read each artifact |

A pipeline moves to another warehouse by rewriting `infra.toml` alone, and a table changes what is built of it by rewriting `table.toml` alone.

### Artifacts are declared by pipeline, placed by sink and chosen by table

An artifact is one of the kinds of table lithify builds: `snapshot`, `instant`, `scd1` and `scd2`, and is named by its kind alone.
`infra.toml` declares under `artifact` what each kind is, once for the whole pipeline, and each sink places in its warehouse the artifacts it builds.
A series is what the build log records under one `artifact` and `schedule`: a `snapshot` has one, cut at `at`, an `instant` has one for each instant of the day it is cut at, and an `scd1` and an `scd2` have one each.
How a kind writes its series down and names its tables is its own: an `instant` lists its series as an array of tables, each with its instant, a `snapshot` and an `instant` name a table for each shard by `shard`, an `scd2` names by `shard` the versions each window closed and those it left open, and an `scd1` names one table after the table it is built from.

```toml
time_zone = "Asia/Tokyo"

[[sink]]
name = "bigquery"

[sink.warehouse.bigquery]
project = "local-project"
endpoint = "http://bigquery-emulator:9050"
change_log = { project = "raw-project", dataset = "mysql_shop_cdc" }
place.snapshot = [{ dataset = "mysql_shop" }]
place.instant."09:00" = [
  { dataset = "mysql_shop_instant_0900" },
  { project = "analytics-project", dataset = "shop_instant_0900" },
]
place.instant."18:00" = [{ dataset = "mysql_shop_instant_1800" }]
place.scd1 = [{ dataset = "mysql_shop_scd1" }]

[[sink]]
name = "duckdb_s3"

[sink.warehouse.duckdb]
s3 = { endpoint = "http://rustfs:9000", region = "us-east-1", bucket = "lithify", format = "parquet" }
change_log = { prefix = "mysql_shop_cdc" }
place.snapshot = [{ prefix = "mysql_shop" }]
place.scd2 = [{ prefix = "mysql_shop_scd2" }]

[artifact.snapshot]
at = "00:00"
shard = "%Y%m%d"

[[artifact.instant]]
at = "09:00"
shard = "%Y%m%d"

[[artifact.instant]]
at = "18:00"
shard = "%Y%m%d"

[artifact.scd1]

[artifact.scd2]
shard = "%Y%m%d"
```

```toml
[default]
artifacts = ["snapshot"]

[[table]]
schema = "shop"
name = "customers"
artifacts = ["snapshot", "scd1"]

[[table]]
schema = "shop"
name = "orders"
artifacts = ["snapshot", "instant", "scd2"]
```

A sink builds the artifacts its warehouse places, and an `instant` only at the instants it places, so sinks may build different artifacts of the same table.
Each artifact is built only of the tables naming it, and a table that names none takes `[default]`, which is a `snapshot` alone unless it is written, so what costs more than a snapshot is paid only for the tables that need it.
A table naming an artifact no sink places is refused.

### Placement

The change log and each placement are written inside the warehouse of the sink, the way that warehouse is: a project and a dataset in BigQuery, the project defaulting to the one the warehouse names, and a prefix in the bucket of a store.
A placement cannot be written for a warehouse other than the one of its sink, since it is read as that warehouse reads where a table lies.
An artifact is placed by the name of its kind, and an `instant` by the instant it is cut at as well; a placement of an artifact, or of an instant, that `artifact` does not declare is refused.
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
- an `scd2` appends each window onto what the window before it left.

A sink placing an artifact without the one it is built from is refused, and so is a table naming an artifact without the one it is built from.
A pipeline declares each kind at most once, so what an artifact is built from is never in doubt.

### The set of artifacts is open

No crate but the command line enumerates the kinds of artifact there are.
As a capture is configured by `CdcFormat::Config` and a warehouse by `OpenWarehouse::Config`, each kind of artifact defines its own configuration in the crate that implements it, and the command line gathers them into one enum the way it gathers `CaptureConfig` and `WarehouseConfig`, so another kind is added without touching the configuration crate.
The build log records each series by the kind of its artifact and by what makes it advance, such as the instant of the day it is cut at, so the `artifact` and `schedule` columns read whatever the artifact writes into them.
