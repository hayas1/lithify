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
| Eras are split by the time a change was recorded, so changes recorded within the same millisecond, or under a clock that went back, fall into the wrong era | Split eras by the position the source orders changes by, such as the binlog file and position in MySQL | In progress | |
| Sinks write the change log some time after the source commits, so a window built right after it closes misses the changes still on their way | Build a window only once the sinks have written past its end, see [Replay](#replay) | Open | |
| A schema change older than one already applied to the same table reaches the replay | The replay stops with `OutOfOrder` naming the table | Rejected | |
| Rows of a table without a primary key cannot be told apart, so there is nothing to fold their changes by | The artifact is refused with `UnbuildableArtifact::WithoutPrimaryKey` | Rejected | |
| Rows before and after a primary key change within a window are identified by different columns, so their changes cannot be folded together | The artifact is refused with `UnbuildableArtifact::PrimaryKeyChanged` | Rejected | |
| A source column type has no counterpart in the warehouse | The artifact is refused with `UnbuildableArtifact::UnsupportedColumn` | Rejected | |
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
| History | One row per version of a source row with the interval it held, so consumers can ask what a row was at any instant | Not started: only configured by `artifacts = ["history"]` | |
| History across schema changes | Versions recorded in different eras share one table, as columns are added, dropped or retyped along the way | Not started | |
| History appended per window | A window appends its versions and closes the ones the window before it left open, rather than rebuilding every version | Not started | |
| Deletes in history | A deleted row closes its last version instead of disappearing | Not started | |
| Mask columns | `mask` keeps the values of a column out of the artifacts, such as a large document or a personal detail; further masks such as a hash come with the questions written on `Mask` | Partial: `mask` is read from `table.toml` but no artifact applies it | |

### Capture

The change log only replays if the source and sink connectors write it the way a dialect reads it, so the pipeline configuration is the one place that contract is written down.

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Generate source connector configuration | Debezium settings the replay depends on (envelope kept, schema changes included, tombstones off, topic naming) follow from `infra.toml` instead of being kept in step by hand | Partial: templates for the compose stack only | |
| Generate sink connector configuration | The namespace and table names a sink writes to must be the ones `sink.change_log` names | Partial: templates for the compose stack only | |
| Validate existing connector configuration | Teams that already run Debezium can check their connectors against the contract without adopting generated ones | Not started | |
| Validate source database prerequisites | Row-based binlog with full row images on MySQL, logical replication on PostgreSQL, and the privileges Debezium needs | Not started | |
| Lay out change log tables for cost | Partitioning and clustering by recorded time or position keep every window from scanning the whole log | Not started | |

### Replay

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Execute artifacts | Only reporting schema changes and writing planned statements is possible from the command line | Partial: dry run only | |
| Record runs as a table | Which window was built when, by which version, with what outcome, so the next run, a backfill and an audit read the same record | Not started | |
| Resume from the last built window | A daily run picks its window from what was built rather than from the clock, and a missed day is caught up | Not started | |
| Backfill a range | Rebuild a range of windows in order, each on top of the one before it | Not started | |
| Wait for the change log to be complete | A window is built only once the sinks have written everything up to its end | Not started | |
| Publish atomically | Consumers never see a window half built across tables | Not started | |
| Guard against concurrent runs | Two runs building the same window overwrite each other | Not started | |
| Record schema changes as a table | Consumers can tell which era a row belongs to without reading the log | Not started | |
| Stable name for the latest shard | Consumers query one name rather than a dated shard | Not started | |

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
| Metrics | Duration, rows and bytes processed per table and window | Not started | |
| Cost estimate | A dry run reports what a window would scan before it is paid for | Not started | |

### Verification

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Reconcile against the source | Row counts and checksums of a snapshot compared with the source database at the same position | Partial: end-to-end scenarios only | |

### Lifecycle

| Support | Why | Status | Target |
| --- | --- | --- | --- |
| Expire old shards | Shards accumulate one per window | Not started | |
| Switch to a new capture | A source re-snapshotted or moved to another host continues from its `since` | Partial: sources are configured with `since` | |
| Rebuild after an upgrade | A version that builds artifacts differently tells which windows to rebuild | Not started | |

## Refactoring

What the command line crate defines that other crates need, so that the command line only parses arguments and reports.
Status reads as in [Operations](#operations).

| Refactoring | Why | Into | Status | Target |
| --- | --- | --- | --- | --- |
| Replay a window into the artifacts of every table | Opening a builder per table on an `EraTracker`, replaying the schema changes through it and finishing it on the tables left standing is spelled out in `SnapshotPlan::statements`, again in `SnapshotBuild` of the end-to-end crate and again in the snapshot tests | core | Not started | |
| Choose the shard a window builds on | `SnapshotPlan::base` turns `--onto` into the shard of the window before, which a scheduled run, a resume and a backfill all need | core or strategy | Not started | |
| Choose the source a window reads | `Run::each_sink` takes the only source and leaves ordering the change log across sources to a `todo!` | core | Not started | |
| Call a fn with the change logs of any warehouse | Every caller implements `DuckDbChangeLogsFn` and `BigQueryChangeLogsFn` with the same body, as `Catalog` and `SnapshotPlan` do | protocol | Not started | |
| Name the artifacts a sink builds | `Artifacts` is implemented for `Sink<Warehouse>` but reads nothing of the warehouse, and the end-to-end crate reaches into the command line crate for it | config or strategy | Not started | |
