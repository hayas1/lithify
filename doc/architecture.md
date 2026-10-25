# Architecture

How the code is laid out and the rules it keeps, as opposed to the [design](design.md), which settles what lithify does, and the [roadmap](roadmap.md), which tracks what is still to be done.

## Code map

| Crate | Holds |
| --- | --- |
| `lithify-core` | The vocabulary every other crate speaks: the schedule and the days it cuts, the replay of schema changes and the listeners it tells, the `Artifact` trait, the build log, and stepping a series window by window |
| `lithify-artifact` | One module for each kind of artifact, each naming its tables and building them from what the replay tells it |
| `lithify-protocol` | The change log formats and the warehouses lithify reads and writes |
| `lithify-config` | Reading a pipeline, generic over the kinds of capture, warehouse and artifact it is given |
| `lithify-cli` | The one place that lists every kind, as enums of the configuration it accepts, and the commands |
| `lithify-ui` | Pages showing what the build log holds |

## Rules

### The replay tells a builder everything it needs

A schema change reaches a builder through `SchemaChangeListener`: the table as it is created, dropped, renamed or altered, each with the schema change that did it and so where it falls in the change log.
Once the replay is over, `Finish` hands a listener what it needs to finish and takes back what it made: a builder is handed a `ReplayEnd`, the table as it stands or the name it was dropped under, and what it refused, if it did, and gives back a `TableBuild`, the statements creating what it builds or why it builds nothing, and what it folds onto.
An artifact and its builder only write statements, in the dialect of the warehouse; the client runs them, refusing a table with nothing to fold onto, and gives back a `TableBuilt` saying how it went.
`TableListeners` hands each table its own builder, following it through a rename and setting aside the one of a table dropped, keeps what each refused, so a table whose builder refuses a schema change stops alone while the replay goes on for the others, and is itself finished with the catalog the replay left, finishing every builder of a table it is told to build.
What the builders made is logged by a `BuildLogWriter` all at once, after every one of them is finished, so no listener knows of the build log or of the run.

A builder folds what it is told into the SQL it builds as it is told it, and keeps nothing else: no copy of a table model or of a name it can be told again.
What it holds besides the SQL is what it was handed when it was made, such as the artifact, the window and the base, an `EraTracker` for where the era running now began, which no schema change tells it again, and the few things the build log asks for after the replay is over, such as the table as the window opened, each named for when it was learned.

Eras are the builder's own to cut: `EraTracker` closes one at each schema change that changes what a row holds, skips one that would hold nothing, and leaves a builder that needs no eras, as one that refuses a schema change, free not to cut any.

When a builder seems to need something more, look first for the listener call that already tells it.

### The last era ends where the artifact says

No schema change ends the last era, so the builder closes it as it is finished, where the artifact says, a `snapshot` or an `instant` where its window closes, an `scd1` nowhere so as to read the change log up to now.

### An artifact names its tables and says how they are created

`Artifact::target` names a table, and the builder of the artifact assembles the statements creating it, so no other module knows how an artifact names or creates its tables.
An `ArtifactBuilder` selects and creates from what it was told and the `ReplayEnd` it is handed, adding the last era to a copy of what it folded, so it reads the same however often it is asked and finishing it changes nothing in it.
Which instant a shard is named after is the artifact's own, as what its data is read as differs: a `snapshot` of a day is named after the last instant the day holds, an `instant` after the instant it is cut at.
A `snapshot`, an `instant` and an `scd1` are all built by `Scd1Builder`, as each is the state of a table overwritten up to an instant: they differ only in what they fold onto, where they stop reading the change log and whether they are created as a table or a view, which each says through `Overwrite`.

### Every series steps a day at a time

`Timeline` cuts the days and `Stepping` walks them; an artifact only maps a day to its window through `Artifact::window`, so the arithmetic of time stays in `lithify-core::schedule`.
