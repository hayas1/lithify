# Roadmap

What lithify runs into once it leaves the playground, and how far it has come in handling each.

## Hazards

What makes an artifact come out wrong.

Status is one of:

- **Open**: not handled yet; the artifact may come out wrong.
- **Rejected**: detected, and the artifact is refused rather than built wrong.
- **In progress**: being worked on.
- **Handled**: the artifact comes out right.

| Hazard | Handling | Status | Target |
| --- | --- | --- | --- |
| Eras are split at a schema change by its processing time, `ts_ms` in Debezium, before its position, so changes processed within the same millisecond, or under a clock that went back, fall into the wrong era | Split eras by the position the source orders changes by, such as the binlog file and position in MySQL | In progress | |
| Windows are cut by event time, the instant a change took place at the source, `source.ts_ms` in Debezium, while the change log holds a change only once the capture and the sinks have carried it, so a window built right after it closes misses the changes still on their way, and a capture that stalled for hours brings a day's changes in after its shard was built | Build a window only once a watermark says nothing occurring within it is still on its way, such as the change log having recorded a change that occurred past its end, see [Replay](#replay) | Open | |
| Changes whose event time falls within a millisecond of midnight may be processed in another order than their event time, so a window can hold a change processed after one the next window holds | Accepted as the state at midnight: a window holds the changes whose event time falls before it closes, and folds them in the order of their processing time, `ts_ms` in Debezium, and then their position | Handled | |
| A schema change older than one already applied to the same table reaches the replay | The replay stops with `OutOfOrder` naming the table | Rejected | |
| Rows of a table without a primary key cannot be told apart, so there is nothing to fold their changes by | The artifact is refused with `UnbuildableArtifact::WithoutPrimaryKey` | Rejected | |
| Rows before and after a primary key change within a window are identified by different columns, so their changes cannot be folded together | The artifact is refused with `UnbuildableArtifact::PrimaryKeyChanged` | Rejected | |
| A source column type has no counterpart in the warehouse | The artifact is refused with `UnbuildableArtifact::UnsupportedColumn` | Rejected | |
| A table stands when a window opens but nothing was built for it closing then, so it would be folded onto nothing | A build by `lithify build` or `lithify catch-up` is refused with `Unrunnable::Baseless` naming the tables; a table created within the window, or one whose change log holds it from its first row such as a captured one, folds onto nothing as it should | Rejected | |
| A statement renames a column in a way the dialect cannot read, so the rename reads as a drop and an add, and the column it became is filled as an added one rather than with the values it had | A rename read from the DDL is carried over; for any other drop and add in one statement a warning names the table, the columns and the statement, so the values can be carried over by hand from the shard the window folds onto and the change log before the statement | Open | |
| A statement drops a column and renames another onto its name, such as `DROP b, RENAME COLUMN a TO b`, but the renames are read from the DDL without the order of its operations | The rename is ignored with a warning, so the rows keep the values of the dropped column under its name and lose those of the renamed one; reading the operations of the statement in order would carry the renamed column over | Open | |
| A column added within a window has a default that is an expression such as `CURRENT_TIMESTAMP`, which the source evaluates for the rows already there but the replay reads as text | The rows folded before the column was added are filled with `Warehouse::cast_or_null`, so the build goes on with null where the source holds the value of the expression; filling them with the instant of the schema change would match `CURRENT_TIMESTAMP` | Open | |

## Operations

What running lithify in production needs beyond replaying a change log.

Status is one of:

- **Not started**
- **Partial**: some of it exists, see the note.
- **In progress**
- **Available**

### Artifacts

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Snapshot | The state of every table at the end of a window, folded onto the shard of the window before it | Available | |
| SCD type 2 | One row per version of a source row with the interval it held, so consumers can ask what a row was at any instant | Not started: only configured by `artifacts = ["history"]` | |
| SCD type 2 across schema changes | Versions recorded in different eras share one table, as columns are added, dropped, renamed or retyped along the way | Not started | |
| SCD type 2 appended per window | A window appends its versions and closes the ones the window before it left open, rather than rebuilding every version | Not started | |
| Deletes in SCD type 2 | A deleted row closes its last version instead of disappearing | Not started | |
| Naming template | Every artifact names its tables by one template that reads the schema, the table and the instant a window closes, written in the language of Expressions so it can say whether that instant is read as the end of its day, as 24:00:00, or the start of the next, as 00:00:00, and an hourly or daily shard is named after what it holds | Not started: a shard is named by formatting the last millisecond its window holds with `shard`, which is not checked against the schedule, so a daily schedule with `%Y%m` names every shard of a month alike | |
| Instant of the daily snapshot | A daily snapshot holds each table as it stood at the instant `snapshot.at` names, midnight unless it says otherwise, read in the `time_zone` of the pipeline, as are the instants a snapshot within a day is cut at | Not started: `Schedule::Daily` closes every window at midnight in UTC, with no setting to change the instant or the time zone | |
| Snapshots between daily shards | The build log closes a window at every midnight and at no other instant, so a snapshot at another instant, such as every hour or an hour other than midnight, is derived from the daily shard before it and the change log up to that instant, configured as an artifact of its own rather than as a schedule of the build log, and leaving what the build log folds onto untouched | Not started: the build log is daily, with no setting to change it | |
| SCD type 1 refreshed by schema changes | A view holding the current state of each row, read straight from the latest shard and the change log after it, so consumers see changes near real time; it is rebuilt when a schema change opens a new era, and is managed by the era it holds from rather than by the instant it holds until, while the build log still records it | Not started | |
| Views | Named by `artifacts` in `table.toml` and created within `lithify catch-up` and `lithify build`, such as a view over the shards of a table | Not started | |
| Place each artifact | `infra.toml` places every artifact on its own, in a project and dataset apart from the change log and from one another, and one artifact in more than one place, such as a snapshot for analysts and the same snapshot for another team | Partial: the change log is written the way the warehouse of the sink is, such as `change_log.bigquery = { project, dataset }`, and refused in a warehouse other than the sink's, but `sink.artifacts` still names one namespace per artifact kind, in the project the warehouse of the sink names | |
| Mask columns | `mask` keeps the values of a column out of the artifacts, such as a large document or a personal detail; further masks such as a hash come with the questions written on `Mask` | Partial: `mask` is read from `table.toml` but no artifact applies it | |

### Capture

The change log only replays if the source and sink connectors write it the way a dialect reads it, so the pipeline configuration is the one place that contract is written down.

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Generate source connector configuration | Debezium settings the replay depends on (envelope kept, schema changes and column comments included, tombstones off, topic naming) follow from `infra.toml` instead of being kept in step by hand | Partial: templates for the compose stack only | |
| Generate sink connector configuration | The namespace and table names a sink writes to must be the ones `sink.change_log` names | Partial: templates for the compose stack only | |
| Validate existing connector configuration | Teams that already run Debezium can check their connectors against the contract without adopting generated ones | Not started | |
| Validate source database prerequisites | Row-based binlog with full row images on MySQL, logical replication on PostgreSQL, and the privileges Debezium needs | Not started | |
| Read only the partitions a window needs | A sink partitions the change log by a time of its own, such as when it wrote a change, rather than by event time, so a window in event time is read through the range of partition time that can hold it, widened by how late a change may reach the sink, and partitioning and clustering by that time or position keep every window from scanning the whole log | Not started | |

### Replay

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Execute artifacts | `lithify catch-up` builds every window closed since the build log says it was last built, or, when nothing was built yet, one shard spanning every window closed since the source's `since`, which defaults to the epoch, or only up to the shard dated `--to` when that shard has closed and the build log has not built past it, which is refused with `Unrunnable::Overtaken` otherwise; a shard is dated by the last day its window holds; and `lithify build --snapshot <DATE> [--table]` rebuilds one shard onto the one the build log recorded before it, or writes its statements with `--dry-run` | Available | |
| Record runs as a table | Which window was built when, by which version, with what outcome, so the next run, a backfill and an audit read the same record | Available: `_lithify_log` per artifact namespace, with the names a table opens and closes each window with and whether it was created, renamed or dropped in it | |
| Resume from the last built window | A daily run picks its window from what was built rather than from the clock, and a missed day is caught up | Partial: every table of a window is rebuilt rather than only the missing ones; a table the catalog holds but the build log never saw stops the build, and one the build log holds but the catalog dropped without a logged drop holds catch-up back | |
| Backfill a range | Rebuild a range of windows in order, each on top of the one before it | Partial: `lithify build --snapshot` rebuilds one shard and the next catch-up rebuilds what folded onto it, but a date inside the shard a first catch-up spanned has no shard to fold onto | |
| Series in the build log | The build log holds a series for each artifact and `schedule`, each advancing its own way: daily shards close at midnight and fold onto the one before, snapshots between them close within a day onto the daily shard before, and an SCD type 1 view is replaced when a schema change opens an era; `schedule` names what makes a series advance rather than only a period | Partial: only the daily snapshot series is built, and `Resumption` reads every row of the build log without narrowing to the series it resumes | |
| Stream the change log | `lithify stream` keeps the artifacts current as changes arrive rather than once a window closes | Not started | |
| Wait for the change log to be complete | A window is built only once a watermark says the sinks have written everything whose event time falls before its end, rather than once the clock passes it | Not started | |
| Publish atomically | Consumers never see a window half built across tables | Not started | |
| Guard against concurrent runs | Two runs building the same window overwrite each other | Partial: a window built twice comes out the same and is read from the build log as built once, but nothing keeps two runs apart | |
| Record schema changes as a table | Consumers can tell which era a row belongs to without reading the log | Not started: the build log keeps only whether a table was created, renamed or dropped, as what building needs; column changes are left to whatever manages the schema, though the replay already reads every alteration a statement makes to a table, renames and comments included, as such a table would record them | |
| Stable name for the latest shard | Consumers query one name rather than a dated shard, likely as one of the [views](#artifacts) | Not started | |

### Interface

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Expressions | One small expression language shared by the naming template and the end-to-end scenarios, such as an instant reckoned from now that a scenario compares a shard with | Not started | |
| Overview of the build log | `lithify serve` lists the latest build of each table and what `lithify check` finds for every source of every pipeline under the directory it is given, with a tab for each sink, the view and the sink shown read from the query so a link keeps what it shows, saying in place of the build log why a sink could not be read or that nothing was logged in it | Available: behind the `serve` feature, rendered on the server; a pipeline of several sources shows the same build log under each, as the build log is kept per sink | |
| Run a retry from the page | A failed build is rebuilt from the page rather than by typing the `lithify build --snapshot` it needs | Not started | |
| List dropped tables | The overview lists the latest build of each table still standing, so a dropped table needs its own read of the build log | Not started | |

### Deployment

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Container image | Every scheduler below runs a container | Not started | |
| Kubernetes CronJob template | The daily run most deployments start from | Not started | |
| Workflow scheduler examples | Airflow, Digdag and Cloud Run Jobs, for teams that schedule elsewhere | Not started | |
| Ambient credentials | Workload identity for BigQuery and IAM roles for S3 instead of keys passed on the command line | Partial: S3 keys from flags or environment | |
| Configuration per environment | One pipeline directory serves staging and production with values from the environment | Not started | |

### Observability

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Structured logs | Logs a platform can index and alert on | In progress: tracing | |
| Exit status by cause | A scheduler retries what may pass next time and pages on what will not | Partial: success or failure only | |
| Alert on check | `lithify check` lists what should have been built by now but was not, to be paged on | Partial: `lithify check` fails with a nonzero exit status on any discrepancy, but nothing hands it to an alerting platform, and a table the build log never saw is not expected | |
| Watch for missing shards | A watcher names each shard that should stand by now through the naming template and logs the ones the warehouse does not hold, without trusting the build log to say what was built | Not started: `lithify check` only looks for the targets the build log recorded | |
| Metrics | Duration, rows and bytes processed per table and window | Not started | |
| Cost estimate | A dry run reports what a window would scan before it is paid for | Not started | |

### Verification

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Reconcile against the source | Row counts and checksums of a snapshot compared with the source database at the same position | Partial: end-to-end scenarios only | |
| Test on the store a warehouse builds in | A DuckDB warehouse always builds in object storage, reading tables through `read_parquet` and writing them with `COPY`, so tests that build on native tables in memory leave that path unexercised, such as parquet files written before a column was added reading it as null; a store over a local directory would run the same statements in tests, and leave the native tables to no one | Not started: the store is only checked by the statements it writes, `compose/e2e_test.sh` does not run lithify, and `DuckDbWarehouse::new` builds on native tables nothing but tests use | |

### Lifecycle

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Expire old shards | Shards accumulate one per window | Not started | |
| Switch to a new capture | A source re-snapshotted or moved to another host continues from its `since` | Partial: sources are configured with `since` | |
| Rebuild after an upgrade | A version that builds artifacts differently tells which windows to rebuild | Not started | |
| Detect a change of dialect | A pipeline captured under one dialect and configured under another reads its change log differently | Partial: a capture that differs from the one the build log recorded stops `lithify catch-up` and is reported by `lithify check`, but `lithify build --snapshot` does not notice it, and which windows to rebuild under the new one is not worked out | |
| Plan a configuration change | Comparing the configuration with the one last applied, kept in a table such as `_lithify_state`, shows what a change would rebuild before it is made, and refuses a change nothing can be rebuilt under, such as the instant a daily snapshot is cut at or the time zone it is read in once a shard is built | Not started | |

## Refactoring

What the command line crate defines that other crates need, so that the command line only parses arguments and reports.
Status reads as in [Operations](#operations).

| Refactoring | Why | Into | Status | Target |
| --- | --- | --- | --- | --- |
| Replay a window into the artifacts of every table | Opening a builder per table on an `EraTracker`, replaying the schema changes through it and finishing it on the tables left standing is spelled out in `SnapshotPlan::builders`, again in `SnapshotBuild` of the end-to-end crate and again in the snapshot tests | core | Not started | |
| Choose the shard a window builds on | `Resumption` in the command line crate reads the build logs a window folds onto, which a scheduled run, a resume and a backfill all need, alongside `Schedule` and the check already in core | core | Not started | |
| Order a change log by its position alone | `Bound::Position` carries the processing time of a schema change ahead of its position, and the change log is ordered by `ts_ms` before the position, only because positions stop comparing across a switch to another server, such as a promoted replica; how positions compare, and where they stop comparing, is the capture's to know, so ordering moves beside it, falling back on processing time only across such a switch | artifact | Not started | |
| Choose the source a window reads | `Run::source` takes the only source and leaves ordering the change log across sources to a `todo!` | core | Not started | |
| Open the warehouse a sink names | `OpenWarehouse` hands a `WarehouseFn` the client of whatever warehouse a sink is configured with, so the overview reads every sink without knowing which warehouses there are, but `Run` still matches on `Warehouse` to open the clients it builds and checks with | core | Partial: the overview opens warehouses through it | |
| Call a fn with the change logs of any warehouse | Every caller implements `DuckDbChangeLogsFn` and `BigQueryChangeLogsFn` with the same body, as `Catalog` and `SnapshotPlan` do | protocol | Not started | |
| Name the artifacts a sink builds | `Artifacts` is implemented for `Sink<Warehouse>` but reads nothing of the warehouse, and the end-to-end crate reaches into the command line crate for it | config or artifact | Not started | |
