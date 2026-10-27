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
| The sink cannot write a change into the change log, as a BigQuery sink connector cannot when a column changes to a type its table does not take, so the connector stops, sends the change to a dead letter queue or writes it into another table, while the change log holds one table for each topic in the one namespace `change_log` names | Not handled: the change never reaches the change log, so every artifact built afterwards misses it without a word; it could be detected by a watermark, see the row on windows cut by event time, or by comparing the column types the schema changes declare with those of the change log table | Open | |
| A schema change older than one already applied to the same table reaches the replay | The replay stops with `OutOfOrder` naming the table | Rejected | |
| Rows of a table without a primary key cannot be told apart, so there is nothing to fold their changes by | The artifact is refused with `UnbuildableArtifact::WithoutPrimaryKey` | Rejected | |
| Rows before and after a primary key change within a window are identified by different columns, so their changes cannot be folded together | The table alone is logged as failed with `UnbuildableArtifact::PrimaryKeyChanged` while the other tables of the window are built | Rejected | |
| A source column type has no counterpart in the warehouse | The artifact is refused with `UnbuildableArtifact::UnsupportedColumn` | Rejected | |
| A table stands when a window opens but nothing was built for it closing then, so it would be folded onto nothing | The table alone is logged as failed by `UnbuildableArtifact::WithoutBase` while the other tables of the window are built, and the next step it is taken into builds it again from the source's `since`; a table created within the window, or one whose change log holds it from its first row such as a captured one, folds onto nothing as it should | Rejected | |
| A statement renames a column in a way the dialect cannot read, so the rename reads as a drop and an add, and the column it became is filled as an added one rather than with the values it had | A rename read from the DDL is carried over; for any other drop and add in one statement a warning names the table, the columns and the statement, so the values can be carried over by hand from the shard the window folds onto and the change log before the statement | Open | |
| A statement drops a column and renames another onto its name, such as `DROP b, RENAME COLUMN a TO b`, but the renames are read from the DDL without the order of its operations | The rename is ignored with a warning, so the rows keep the values of the dropped column under its name and lose those of the renamed one; reading the operations of the statement in order would carry the renamed column over | Open | |
| A sink creates the change log table of a topic, and adds a column to it, only once a row change carrying it arrives, so after a table is created or renamed, or a column added, with no row change since, the change log holds no table for that name or no field for that column, while the replay reads them for the era the schema change opened | Not handled: the build fails on the missing table or field, such as `Table with name mysql.shop.persons does not exist` or `Could not find key "tier" in struct`; the era could read an absent table as holding no changes and an absent field as null | Open | |
| A column added within a window has a default that is an expression such as `CURRENT_TIMESTAMP`, which the source evaluates for the rows already there but the replay reads as text | The rows folded before the column was added are filled with `Warehouse::cast_or_null`, so the build goes on with null where the source holds the value of the expression; filling them with the instant of the schema change would match `CURRENT_TIMESTAMP` | Open | |
| An SCD type 1 view is built in the shape its table had when it was built, so a column added after it is left out, and the rows changed after a column is renamed read that column as null, until the next run rebuilds it | `lithify catch-up` rebuilds it once each daily snapshot is built | Open | |

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
| SCD type 2 | One row per version of a source row with the interval it held, so consumers can ask what a row was at any instant | Partial: an artifact of `kind = "scd2"`, placed by its name, keeps, for each window a catch-up builds, the versions the window closed with `_valid_from` and `_valid_to` and those still open as it closes, in milliseconds of event time; a consumer reads them as the union of every closed table and the latest open one, which nothing builds into a view yet, and an artifact placed more than once is built into its first placement alone | |
| SCD type 2 across schema changes | Versions recorded in different eras share one table, as columns are added, dropped, renamed or retyped along the way | Not started | |
| SCD type 2 appended per window | A window appends its versions and closes the ones the window before it left open, rather than rebuilding every version | Available: each window folds onto the versions the window before it left open, and building a window again rewrites only its own two tables | |
| Deletes in SCD type 2 | A deleted row closes its last version instead of disappearing | Available | |
| Naming template | Every artifact names its tables by one template that reads the schema, the table and the instant a window closes, written in the language of Expressions so it can say whether that instant is read as the end of its day, as 24:00:00, or the start of the next, as 00:00:00, and an hourly or daily shard is named after what it holds | Not started: a shard is named by formatting with `shard` the last instant its window holds, or for an `instant` the instant its window closes, which is not checked against the schedule, so a daily schedule with `%Y%m` names every shard of a month alike | |
| Instant of the daily snapshot | A daily snapshot holds each table as it stood at the instant `snapshot.at` names, midnight unless it says otherwise, read in the `time_zone` of the pipeline, as are the instants a snapshot within a day is cut at | Available: `snapshot.at` and `time_zone` at the top of `infra.toml`, UTC when it names none; nothing refuses them changing once a shard is built, see [Plan a configuration change](#lifecycle) | |
| Snapshots between daily shards | The build log closes a window at every midnight and at no other instant, so a snapshot at another instant, such as every hour or an hour other than midnight, is derived from the daily shard before it and the change log up to that instant, configured as an artifact of its own rather than as a schedule of the build log, and leaving what the build log folds onto untouched | Partial: each instant is an artifact of its own of `kind = "instant"`, cut at its `at` off the snapshot its `base` names and placed by its name, and each table is cut, once the instant has passed, in the day after the last it was cut in, or, never cut, in the day its daily snapshot stands at, folding onto the daily snapshot before; the days before an instant was first cut are not cut | |
| SCD type 1 refreshed by schema changes | A view holding the current state of each row, read straight from the latest shard and the change log after it, so consumers see changes near real time; it is rebuilt when a schema change opens a new era, and is managed by the era it holds from rather than by the instant it holds until, while the build log still records it | Partial: an artifact of `kind = "scd1"`, built off the snapshot its `base` names and placed by its name, is built once onto each daily snapshot as a window left open, as a view where the warehouse keeps one and as a table in a store, folding the change log after that snapshot with its last era left open; a schema change does not rebuild it, and a store could keep it as a view in the DuckDB database `database` names | |
| Views | Named by `artifacts` in `table.toml` and created within `lithify catch-up` and `lithify build`, such as a view over the shards of a table | Not started | |
| Place each artifact | `infra.toml` places every artifact on its own, in a project and dataset apart from the change log and from one another, and one artifact in more than one place, such as a snapshot for analysts and the same snapshot for another team | Partial: the change log and each artifact are placed inside the warehouse of the sink, `change_log` and `place.<name>` under `[sink.warehouse.<warehouse>]`, written the way that warehouse is, such as `{ project, dataset }`, so they cannot lie in a warehouse other than the sink's, and an artifact placed more than once is built into its first placement and copied into the others, though only its first placement keeps a build log | |
| Mask columns | `mask` keeps the values of a column out of the artifacts, such as a large document or a personal detail; which columns are masked is the table's to say, so a mask applies to whatever artifact the table is built into, a snapshot, an instant, an SCD type 1 or 2 alike, rather than being an artifact of its own; how a placement says it holds masked rows, whether masked placements are views onto the unmasked one, and further masks such as a hash with the questions written on `Mask` are still to be settled | Partial: `mask` is read from `table.toml` but no artifact applies it | |

### Capture

The change log only replays if the source and sink connectors write it the way a dialect reads it, so the pipeline configuration is the one place that contract is written down.

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Generate source connector configuration | Debezium settings the replay depends on (envelope kept, schema changes and column comments included, tombstones off, topic naming) follow from `infra.toml` instead of being kept in step by hand | Partial: templates for the compose stack only | |
| Generate sink connector configuration | The namespace and table names a sink writes to must be the ones `sink.change_log` names | Partial: templates for the compose stack only | |
| Validate existing connector configuration | Teams that already run Debezium can check their connectors against the contract without adopting generated ones | Not started | |
| Leave out tables no one builds | Tables that come and go by the hundred, such as temporary ones, are left out of the schema changes the replay reads by a pattern of names in `infra.toml`, so they are neither tracked nor logged as dropped | Not started | |
| Tables named by database as well as schema | A source such as SQL Server captures several databases through one connector, whose topics read `<prefix>.<database>.<schema>.<table>`, so a table is told apart only by all three | Not started: a table is a schema and a name, which covers MySQL, whose topics read `<prefix>.<database>.<table>` and whose database `table.toml` writes as `schema`, and PostgreSQL, whose connector reads one database; naming the third level would let `table.toml` say `database` where it means one rather than `schema` | |
| Change log that outlives a type change | A change log keeps every change however the columns of its table change type, so a sink that cannot widen a column does not leave the artifacts short | Not started: either `change_log` lists the namespaces the change log was written into, each with the instant it took over, the way sources take over one another by `since`, and the replay reads each over its own span, or the sink connector writes each change as one JSON string the replay extracts the columns from, which no type change stops but which reads slower and loses the column types BigQuery would keep | |
| Validate source database prerequisites | Row-based binlog with full row images on MySQL, logical replication on PostgreSQL, and the privileges Debezium needs | Not started | |
| Read only the partitions a window needs | A sink partitions the change log by a time of its own, such as when it wrote a change, rather than by event time, so a window in event time is read through the range of partition time that can hold it, widened by how late a change may reach the sink, and partitioning and clustering by that time or position keep every window from scanning the whole log | Not started | |

### Replay

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Execute artifacts | `lithify build` takes the one step the build log says is next, of the artifact `--artifact` names or of every one the sink places, and builds each table into the window after the last it was built into, or, never built, into one shard spanning every window closed since the source's `since`, which defaults to the epoch; it writes its statements with `--dry-run`; `lithify catch-up` repeats the step until none is left, of the artifact `--artifact` names or of every one the sink places in the order `artifact.toml` declares them, or only through the windows closing by the end of the day `--to` names when that day has ended and the build log has not built past it, which is refused with `Unsteppable::Overtaken` otherwise, and takes no window twice | Available | |
| Record runs as a table | Which window was built when, by which version, with what outcome, so the next run, a backfill and an audit read the same record | Available: `_lithify_log` per artifact namespace, with the names a table opens and closes each window with and whether it was created, renamed or dropped in it | |
| Resume from the last built window | A daily run picks its window from what was built rather than from the clock, and a missed day is caught up | Partial: each table advances from where the build log says it was last built, so tables may stand at different days and a table that fails holds back no other; one the build log holds but the catalog dropped without a logged drop is stepped for once by every catch-up without being built | |
| Backfill a range | Rebuild a range of windows in order, each on top of the one before it | Not started: `lithify build --date` was taken out while every series moved to stepping day by day, and how to name a range to rebuild is to be worked out again | |
| Series in the build log | The build log holds a series for each artifact and `schedule`, each advancing its own way: daily shards close at midnight and fold onto the one before, snapshots between them close within a day onto the daily shard before, and an SCD type 1 view is replaced when a schema change opens an era; `schedule` names what makes a series advance rather than only a period | Partial: `Progress` reads the build log of one `Series`, an artifact and a schedule, and each artifact says by `Artifact::next` what window a table steps into next from where its series and the one it is built from stand; an SCD type 1 steps with each daily snapshot rather than when a schema change opens an era | |
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
| Run a retry from the page | A failed build is rebuilt from the page rather than by typing the command it needs | Not started | |
| Dry run a catch-up and check its statements | A dry run shows every step a catch-up would take, and the warehouse checks each statement before anything runs | Partial: `lithify build --dry-run` takes one step on a client executing with `DryRun`, which records what it would run; as nothing is logged, a catch-up cannot step on until the next step reads the build log the dry run recorded, and BigQuery could check and price each statement by its own dry run rather than only record it | |
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
| Detect a change of dialect | A pipeline captured under one dialect and configured under another reads its change log differently | Partial: a capture that differs from the one the build log recorded stops `lithify catch-up` and is reported by `lithify check`, but which windows to rebuild under the new one is not worked out | |
| Plan a configuration change | Comparing the configuration with the one last applied, kept in a table such as `_lithify_state`, shows what a change would rebuild before it is made, and refuses a change nothing can be rebuilt under, such as the instant a daily snapshot is cut at or the time zone it is read in once a shard is built | Not started | |

## Refactoring

What the command line crate defines that other crates need, so that the command line only parses arguments and reports.
Status reads as in [Operations](#operations).

| Refactoring | Why | Into | Status | Target |
| --- | --- | --- | --- | --- |
| Replay a window into the artifacts of every table | Opening a builder per table on `TableListeners`, replaying the schema changes through it and finishing it on the tables left standing was spelled out in the command line crate, again in `SnapshotBuild` of the end-to-end crate and again in the snapshot tests | core | Partial: `Replayer` in core is used by the command line and end-to-end crates, while the snapshot tests still spell it out | |
| Choose the shard a window builds on | `Stepping` in core reads where each series stands and the build logs a window folds onto, and a `StepTaker` replays it while a `BuildLogWriter` logs what it built; the command line only lists the series a sink builds and says which one each is built onto | core | Available | |
| Order a change log by its position alone | `Bound::Position` carries the processing time of a schema change ahead of its position, and the change log is ordered by `ts_ms` before the position, only because positions stop comparing across a switch to another server, such as a promoted replica; how positions compare, and where they stop comparing, is the capture's to know, so ordering moves beside it, falling back on processing time only across such a switch | artifact | Not started | |
| Choose the source a window reads | `Run::source` takes the only source and leaves ordering the change log across sources to a `todo!` | core | Not started | |
| Open the warehouse a sink names | `OpenWarehouse` hands a `WarehouseFn` the client of whatever warehouse a sink is configured with, so the overview reads every sink without knowing which warehouses there are, but `Run` still matches on `Warehouse` to open the clients it builds and checks with | core | Partial: the overview opens warehouses through it | |
| Call a fn with the change logs of any warehouse | Every caller implements `DuckDbChangeLogsFn` and `BigQueryChangeLogsFn` with the same body, as `Catalog` and `Replaying` do | protocol | Not started | |
| Name the artifacts a sink builds | A pipeline declares its artifacts by kind, sinks place them, and the configuration crate reads them through `Declaration` and `Placing` without knowing the kinds, but the command line finds the snapshot of a sink by `DeclaredSnapshot`, which the end-to-end crate reaches into the command line crate for, and the overview finds it by the name `snapshot` again | config or artifact | Partial | |
