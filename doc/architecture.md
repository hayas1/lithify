# Architecture

How the code is laid out and the rules it keeps, as opposed to the [design](design.md), which settles what lithify does, and the [roadmap](roadmap.md), which tracks what is still to be done.

## Code map

| Crate | Holds |
| --- | --- |
| `lithify-core` | The data plane, the vocabulary every other crate speaks: the replay of schema changes and the listeners it tells, the `Artifact` trait, and planning what one window builds of each table, without waiting on a warehouse |
| `lithify-artifact` | One module for each kind of artifact, each naming its tables and building them from what the replay tells it, and saying which window it builds and what it folds onto |
| `lithify-protocol` | The change log formats and the warehouses lithify reads and writes, the build log among what they hold |
| `lithify-pipeline` | The control plane, generic over the kinds of capture, warehouse and artifact it is given: reading and listing pipelines, the schedule and the days it cuts, the build log, stepping a series window by window, running what the core plans, and checking a sink |
| `lithify-cli` | The one place that lists every kind, as enums of the configuration it accepts, and the commands |
| `lithify-ui` | Pages showing what the build log holds, shaped from what `lithify-pipeline` reads |
| `lithify-testing` | What `cargo test` of the other crates builds on, as a dev-dependency alone: a replica of a source database and its history as the oracle, its change log landed in an in-memory DuckDB as Debezium would land it, the shards an artifact builds over it, and stand-ins for a change log, a warehouse and a pipeline |
| `lithify-e2e` | Scenarios run against a pipeline on real services, for what `cargo test` cannot hold: what Debezium and the sinks actually write, the store, BigQuery, and the command line from end to end |

## Rules

### The replay tells a builder everything it needs

A schema change reaches a builder through `SchemaChangeListener`: the table as it is created, dropped, renamed or altered, each with the schema change that did it and so where it falls in the change log.
Once the replay is over, `Finish` hands a listener what it needs to finish and takes back what it made: a builder is handed a `ReplayEnd`, the table as it stands or the name it was dropped under, and what it refused, if it did, and gives back a `TableBuild`, the statements creating what it builds or why it builds nothing, and what it folds onto.
An artifact and its builder only write statements, in the dialect of the warehouse; `Runner` runs them, refusing a table with nothing to fold onto, and gives back a `TableBuilt` saying how it went.
`TableListeners` hands each table its own builder, following it through a rename and setting aside the one of a table dropped, keeps what each refused, so a table whose builder refuses a schema change stops alone while the replay goes on for the others, and is itself finished with the catalog the replay left, finishing every builder of a table it is told to build.
What the builders made is logged by a `BuildLogWriter` all at once, after every one of them is finished, so no listener knows of the build log or of the run.

A builder folds what it is told into the SQL it builds as it is told it, and keeps nothing else: no copy of a table model or of a name it can be told again.
What it holds besides the SQL is what it was handed when it was made, such as the artifact, the window and the base, an `EraTracker` for where the era running now began, which no schema change tells it again, and the few things the build log asks for after the replay is over, such as the table as the window opened, each named for when it was learned.

Eras are the builder's own to cut: `EraTracker` closes one at each schema change that changes what a row holds, skips one that would hold nothing, and leaves a builder that needs no eras, as one that refuses a schema change, free not to cut any.

When a builder seems to need something more, look first for the listener call that already tells it.

### The core plans, the pipeline runs and records

`lithify-core` decides what a window builds from what it is handed alone: the schema changes before the window opens and within it, read beforehand into a `SchemaHistory`, the table each one folds onto, and the dialect of the warehouse; `Replayer` replays them into a `TableBuild` for each table and runs nothing.
`lithify-pipeline` does everything else: which window to build, read from the build log, the history it reads, running the statements and logging what came of them.
The core names no build log, no schedule and no status, and each trait of the core is extended in `lithify-pipeline` with what the control plane asks of it, `ArtifactControl` of `Artifact` and `WarehouseControl` of `WarehouseClient`, which `lithify-artifact` and `lithify-protocol` implement beside the core ones.

### Only what talks to a warehouse waits

A listener, a builder, `Finish` and `Replayer` are synchronous, as they only fold what they are told into statements.
What reads or writes a warehouse is async, `WarehouseClient`, `SchemaChangeStream` and the change log callbacks of each warehouse, and so is whatever steps on them, from `Runner` up to the commands, which the CLI runs on a tokio runtime.
No future is asked to be `Send`: a run steps one sink at a time, and a DuckDB connection cannot be shared across threads, so the pages read a pipeline on a blocking thread of their own and wait on it there.

### The last era ends where the artifact says

No schema change ends the last era, so the builder closes it as it is finished, where the artifact says, a `snapshot` or an `instant` where its window closes, an `scd1` nowhere so as to read the change log up to now.

### An artifact names its tables and says how they are created

`Artifact::target` names a table, and the builder of the artifact assembles the statements creating it, so no other module knows how an artifact names or creates its tables.
An `ArtifactBuilder` selects and creates from what it was told and the `ReplayEnd` it is handed, adding the last era to a copy of what it folded, so it reads the same however often it is asked and finishing it changes nothing in it.
Which instant a shard is named after is the artifact's own, as what its data is read as differs: a `snapshot` of a day is named after the last instant the day holds, an `instant` after the instant it is cut at.
A `snapshot`, an `instant` and an `scd1` are all built by `Scd1Builder`, as each is the state of a table overwritten up to an instant: they differ only in what they fold onto, where they stop reading the change log and whether they are created as a table or a view, which each says through `Overwrite`.

### Every series steps a day at a time

`Timeline` cuts the days and `Stepping` walks them; an artifact only maps a day to its window through `ArtifactControl::window`, so the arithmetic of time stays in `lithify-pipeline::schedule`.
