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

## Running

### A build takes one step, and a catch-up repeats it

`lithify build` takes one step: it builds one window of one series for the tables that window is next for, onto what the build log records them built from, and logs each table.
Named nothing, it takes the step the build log says is next; `--artifact` and `--at` name the series.
`--dry-run` writes the statements of the step rather than running them, and logs nothing.

`lithify catch-up` repeats `lithify build`, reading the build log afresh before each step, until no step is left.
Named an artifact with `--artifact` and `--at`, it advances that series alone, so each artifact may be run by a process of its own at the times that suit it, such as an `instant` after the instant it is cut at.
Named nothing, it advances the series of a sink in the order they are built from one another: the `snapshot`, each `instant`, the `scd2`, then the `scd1`, each as far as it can.

### Each table advances on its own

The build log says, for each series and each table, how far the table was last built; the table's next window follows from there, so tables may stand at different days.
A step takes the earliest next window of the series and builds every table it is next for; a table the build log has never seen is built with whatever step its window opens in, and one it saw but never built starts again from the source's `since` in one window spanning every day closed since.
A table is never built into a window after one it was not built into, since each window folds onto the one before.

Every series advances day by day: a day closes at the instant the `snapshot` of the sink is cut at, and the first day of a series never built spans every day closed since `since`.
Each series steps into the day after the one its last window opened in; one built from the `snapshot` steps no further than the day the `snapshot` stands at, and, never built, starts there.
Each artifact maps the day onto its own window, the closed range of the change log it updates for, which always opens as the day does, and the build log records that window for each table.

- a `snapshot` and an `scd2` take the day itself;
- an `instant` takes the day up to the instant it is cut at, once that instant has passed, so it may be built by a batch running more often than daily;
- an `scd1` takes only the day still open, from where it opens up to when it is built, and stays open past it, reading every row change since the window opened, so it is built once each time its `snapshot` stands at a new day;
- one built from the `snapshot` folds onto the shard closing as its window opens.

The schema changes of a build are replayed over its window, which is always closed, and split it into eras.
Only an artifact left open, the `scd1`, reads the row changes of the last era past the window, rather than stop at an instant taken when the build runs; every other artifact is done with the change log once its window closes.

### A table that fails stops itself alone

A table that fails is logged as failed and stays where it was, while the other tables of the step advance.
A catch-up takes no window twice: a step whose window it has already taken leaves out the tables it was taken for, so a table that failed is tried again by the next catch-up rather than within the same one.
A step that cannot run at all, such as one whose warehouse is out of reach or whose capture changed since the build log was written, stops the catch-up of the sink.
A table with nothing to fold onto is logged as failed rather than refusing the step, and so starts again from `since` at the next step it is taken into.
