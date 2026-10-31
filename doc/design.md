# Design

What has been settled about how lithify is configured and how its artifacts are put together, as opposed to the [roadmap](roadmap.md), which tracks what is still to be done.

## Configuration

A pipeline is a directory of files, each answering one question about it.

| File | Answers | Holds |
| --- | --- | --- |
| `artifact.toml` | What can be built | The artifacts the pipeline builds, each under a name with its kind and what that kind needs to know, such as the instant it is cut at |
| `infra.toml` | Where it is captured from and where it lies | The sources a change log is captured from, and the warehouses the change log is sunk into, each with where in it the change log is read from and where each artifact is written to, such as a dataset in BigQuery or a prefix in the bucket of a store |
| `table.toml` | What is to be made of each table | The artifacts each table is built into, by the names `artifact.toml` gives them, and how its columns are treated in them, such as a mask |
| `owner.toml` | Who answers for the data | The team that owns each table and the people allowed to read each artifact |

A pipeline moves to another warehouse by rewriting `infra.toml` alone, and a table changes what is built of it by rewriting `table.toml` alone; the artifacts and the places they lie in are set up once and seldom change.

### Artifacts are declared by name, placed by sink and chosen by table

An artifact is declared in `artifact.toml` under a `name`, with its `kind`, one of `snapshot`, `instant`, `scd1` and `scd2`, and what that kind needs to know; the name may be the kind itself as long as no other artifact takes it.
The name is what the rest of the configuration and the build log know the artifact by: each sink places it by its name, each table chooses it by its name, and the build log records its series under it, so renaming an artifact starts its series afresh.
Each kind names its tables its own way: a `snapshot` and an `instant` name a table for each shard by formatting with `shard` an instant read in the time zone, a `snapshot` the last instant its window holds, as the data of a day is the day as it ended, and an `instant` the instant it is cut at, an `scd2` names by `shard`, as a `snapshot` is named, the versions each window closed and those it left open, and an `scd1` names one view after the table it is built from, as `<table>_scd_type_1`.

```toml
# artifact.toml
[[artifact]]
name = "snapshot"
kind = "snapshot"
at = "00:00"
shard = "%Y%m%d"

[[artifact]]
name = "instant_0900"
kind = "instant"
base = "snapshot"
at = "09:00"
shard = "%Y%m%d"

[[artifact]]
name = "scd1"
kind = "scd1"
base = "snapshot"

[[artifact]]
name = "scd2"
kind = "scd2"
shard = "%Y%m%d"
```

```toml
# infra.toml
time_zone = "Asia/Tokyo"

[[sink]]
name = "bigquery"

[sink.warehouse.bigquery]
project = "local-project"
endpoint = "http://bigquery-emulator:9050"
change_log = { project = "raw-project", dataset = "mysql_shop_cdc" }
place.snapshot = { dataset = "mysql_shop" }
place.instant_0900 = { project = "analytics-project", dataset = "shop_instant_0900" }
place.scd1 = { dataset = "mysql_shop_scd1" }

[[sink]]
name = "duckdb_s3"

[sink.warehouse.duckdb]
s3 = { endpoint = "http://rustfs:9000", region = "us-east-1", bucket = "lithify", format = "parquet" }
change_log = { prefix = "mysql_shop_cdc" }
place.snapshot = { prefix = "mysql_shop" }
place.scd2 = { prefix = "mysql_shop_scd2" }
```

```toml
# table.toml
[default]
artifacts = ["snapshot"]

[[table]]
schema = "shop"
name = "customers"
artifacts = ["snapshot", "scd1"]

[[table]]
schema = "shop"
name = "orders"
artifacts = ["snapshot", "instant_0900", "scd2"]
```

A sink builds the artifacts it places, so sinks may build different artifacts of the same table.
Each artifact is built only of the tables naming it, and a table that names none takes `[default]`, which builds nothing unless it is written, so nothing is built of a table no one asked for, and what costs more than a snapshot is paid only for the tables that name it.
A table naming an artifact no sink places is refused.

### Placement

The change log and each placement are written inside the warehouse of the sink, the way that warehouse is: a project and a dataset in BigQuery, the project defaulting to the one the warehouse names, and a prefix in the bucket of a store.
A placement cannot be written for a warehouse other than the one of its sink, since it is read as that warehouse reads where a table lies.
An artifact is placed by its name; a placement of an artifact `artifact.toml` does not declare is refused.
The change log and each artifact are placed on their own, so the artifacts of one sink may lie in projects or datasets apart from the change log and from one another.

An artifact lies in one place, and a list of places is refused as `infra.toml` is read; the same rows in another place are an artifact of their own, built from the one they hold, with a build log of its own.

### The build log advances daily

The build log closes a window once a day, at the instant the `snapshot` of the sink is cut at, which is midnight unless `snapshot.at` says otherwise, and at no other instant, and each daily shard folds onto the one before it.
Every instant of the day, of a `snapshot` and of an `instant` alike, is read in the `time_zone` of the pipeline.

The instant a `snapshot` is cut at and the time zone it is read in are fixed once a shard is built: each shard folds onto the one before it, so shards cut at another instant would fold onto a shard that never held the state they start from.

An artifact that holds the state at another instant of the day is an `instant`, which cuts within a daily window rather than closing one, and leaves what the build log folds onto untouched.

### An artifact names what it is built from

What kind of artifact another is built from is the artifact's to know, as `Artifact::From`, and which one of that kind is the configuration's to say, as its `base`:

- an `instant` folds the change log up to the instant it is cut at onto the `snapshot` before it;
- an `scd1` folds the change log after the latest `snapshot` onto it;
- an `scd2` appends each window onto what the window before it left, and names no base.

A base declared nowhere, or one of another kind than the artifact is built from, is refused as `artifact.toml` is read, and so are a sink placing an artifact without its base and a table naming an artifact without its base.
A sink places one `snapshot`, which the build log of the sink advances by, so what an artifact is built from is never in doubt.

### The set of artifacts is open

No crate but the command line enumerates the kinds of artifact there are.
As a capture is configured by `CdcFormat::Config` and a warehouse by `OpenWarehouse::Config`, each kind of artifact is configured by `Artifact::Config`, and is made by `Artifact::declared` from its name, its configuration, the `Placement` it is written into and the time zone; the command line gathers the configurations into one enum tagged by `kind`, the way it gathers `CaptureConfig` and `WarehouseConfig`, so another kind is added without touching the configuration crate.
The build log records each series under the name of its artifact and by what makes it advance, such as the instant of the day it is cut at, so the `artifact` and `schedule` columns read whatever the artifact writes into them.

## Running

### A build takes one step, and a catch-up repeats it

`lithify build` takes one step: it builds one window of one series for the tables that window is next for, onto what the build log records them built from, and logs each table.
Named nothing, it takes the step the build log says is next; `--artifact` names the series by the name of its artifact.
`--dry-run` takes the step as a build does, but on a client that records the statements it would run, the build log it would write among them, rather than running them, and writes them into one file for the step; a table that fails fails the dry run as it would the build, while the statements of the other tables are still written.

`lithify catch-up` repeats `lithify build`, reading the build log afresh before each step, until no step is left.
Named an artifact with `--artifact`, it advances that series alone, so each artifact may be run by a process of its own at the times that suit it, such as an `instant` after the instant it is cut at.
Named nothing, it advances the series of a sink in the order `artifact.toml` declares them, each as far as it can; a series waiting on its base takes its step once the base has stepped.

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
A table whose builder refuses a schema change the replay tells it of, such as one changing its primary key, is told nothing more of the window and is logged as failed with why, while the replay goes on for the other tables.
A table dropped and created again, as a table refilled from scratch is, is logged as dropped and then as created, and is built anew from the change log of the table created rather than onto what was built of the one dropped.
