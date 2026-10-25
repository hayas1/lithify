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

A schema change reaches a builder through `SchemaChangeListener`: each era a schema change closes through `era_boundary`, the table as it is created, dropped, renamed or altered, and, once the replay is over, the last era through `finish`.

A builder folds what it is told into the SQL it builds as it is told it, and keeps nothing else: no copy of a table model, of where an era began or of a name it can be told again.
What it holds besides the SQL is what it was handed when it was made, such as the artifact, the window and the base, and the few things the build log or the runner ask for after the replay is over, such as the table as the window opened, each named for when it was learned.

When a builder seems to need something more, look first for the listener call that already tells it.

### The last era ends where the artifact says

The replay knows where each era begins, but not where the last one ends: it hands that era to `finish` open, and the artifact closes it, a `snapshot` or an `instant` where its window closes, an `scd1` nowhere so as to read the change log up to now.

### An artifact names its tables and says how they are created

`Artifact::target` names a table, and the builder of the artifact assembles the statements creating it, so no other module knows how an artifact names or creates its tables.
Which instant a shard is named after is the artifact's own, as what its data is read as differs: a `snapshot` of a day is named after the last instant the day holds, an `instant` after the instant it is cut at.
A `snapshot`, an `instant` and an `scd1` are all built by `Scd1Builder`, as each is the state of a table overwritten up to an instant: they differ only in what they fold onto, where they stop reading the change log and whether they are created as a table or a view, which each says through `Overwrite`.

### Every series steps a day at a time

`Timeline` cuts the days and `Stepping` walks them; an artifact only maps a day to its window through `Artifact::window`, so the arithmetic of time stays in `lithify-core::schedule`.
