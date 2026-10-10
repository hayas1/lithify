use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Utc};
use semigroup::Semigroup;
use serde::{Deserialize, Serialize};

use event_replay_core::model::TableId;
use event_replay_core::position::RecordedAt;
use event_replay_core::sink::TablePath;

use crate::Artifact;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "C: Deserialize<'de>, W: Deserialize<'de>"))]
#[serde(bound(serialize = "C: Serialize, W: Serialize"))]
pub(crate) struct Wiring<C, W> {
    #[serde(default)]
    default: Defaults<C, W>,
    #[serde(default, rename = "source")]
    sources: Vec<WiredSource<C>>,
    #[serde(default, rename = "sink")]
    sinks: Vec<WiredSink<W>>,
}

impl<C: Clone, W: Clone> Wiring<C, W> {
    pub(crate) fn sources(&self) -> Result<Vec<Source<C>>, Miswiring> {
        if self.sources.is_empty() {
            return Err(Miswiring::WithoutSources);
        }

        let mut sources = self
            .sources
            .iter()
            .map(|wired| wired.clone().source(&self.default.source))
            .collect::<Result<Vec<_>, _>>()?;
        sources.sort_by_key(Source::since);
        Self::distinct(&sources)?;

        Ok(sources)
    }

    fn distinct(sources: &[Source<C>]) -> Result<(), Miswiring> {
        Self::named_once(sources.iter().map(Source::name)).map_err(Miswiring::SourceNamedTwice)?;

        match sources
            .windows(2)
            .find(|pair| pair[0].since() == pair[1].since())
        {
            Some(pair) => Err(Miswiring::SourcesTakingOverTogether(
                pair[0].name().to_owned(),
                pair[1].name().to_owned(),
            )),
            None => Ok(()),
        }
    }

    pub(crate) fn sinks(&self) -> Result<Vec<Sink<W>>, Miswiring> {
        if self.sinks.is_empty() {
            return Err(Miswiring::WithoutSinks);
        }

        let sinks = self
            .sinks
            .iter()
            .enumerate()
            .map(|(at, wired)| wired.clone().sink(at, &self.default.sink))
            .collect::<Result<Vec<_>, _>>()?;

        Self::named_once(sinks.iter().map(Sink::name))
            .map_err(Miswiring::SinkNamedTwice)
            .map(|()| sinks)
    }

    fn named_once<'a>(names: impl Iterator<Item = &'a str>) -> Result<(), String> {
        let mut named = BTreeSet::new();

        for name in names {
            if !named.insert(name) {
                return Err(name.to_owned());
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "C: Deserialize<'de>, W: Deserialize<'de>"))]
#[serde(bound(serialize = "C: Serialize, W: Serialize"))]
struct Defaults<C, W> {
    #[serde(default)]
    source: WiredSource<C>,
    #[serde(default)]
    sink: WiredSink<W>,
}

impl<C, W> Default for Defaults<C, W> {
    fn default() -> Self {
        Self {
            source: WiredSource::default(),
            sink: WiredSink::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Semigroup)]
#[semigroup(with = "semigroup::op::Coalesce")]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "C: Deserialize<'de>"))]
#[serde(bound(serialize = "C: Serialize"))]
struct WiredSource<C> {
    name: Option<String>,
    since: Option<DateTime<Utc>>,
    capture: Option<C>,
    schema_changes: Option<String>,
}

impl<C> Default for WiredSource<C> {
    fn default() -> Self {
        Self {
            name: None,
            since: None,
            capture: None,
            schema_changes: None,
        }
    }
}

impl<C: Clone> WiredSource<C> {
    fn source(self, default: &Self) -> Result<Source<C>, Miswiring> {
        let wired = self.semigroup(default.clone());
        let name = wired.name.ok_or(Miswiring::SourceWithoutName)?;

        Ok(Source {
            since: wired
                .since
                .ok_or_else(|| Miswiring::SourceWithoutSince(name.clone()))?,
            capture: wired
                .capture
                .ok_or_else(|| Miswiring::SourceWithoutCapture(name.clone()))?,
            name,
            schema_changes: wired.schema_changes,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Semigroup)]
#[semigroup(with = "semigroup::op::Coalesce")]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "W: Deserialize<'de>"))]
#[serde(bound(serialize = "W: Serialize"))]
struct WiredSink<W> {
    name: Option<String>,
    change_log: Option<String>,
    warehouse: Option<W>,
    replay: Option<ReplayTargets>,
}

impl<W> Default for WiredSink<W> {
    fn default() -> Self {
        Self {
            name: None,
            change_log: None,
            warehouse: None,
            replay: None,
        }
    }
}

impl<W: Clone> WiredSink<W> {
    fn sink(self, at: usize, default: &Self) -> Result<Sink<W>, Miswiring> {
        let wired = self.semigroup(default.clone());
        let name = wired.name.ok_or(Miswiring::SinkWithoutName(at))?;

        Ok(Sink {
            change_log: wired
                .change_log
                .ok_or_else(|| Miswiring::SinkWithoutChangeLog(name.clone()))?,
            warehouse: wired
                .warehouse
                .ok_or_else(|| Miswiring::SinkWithoutWarehouse(name.clone()))?,
            replay: wired
                .replay
                .ok_or_else(|| Miswiring::SinkWithoutReplay(name.clone()))?,
            name,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Miswiring {
    WithoutSources,
    WithoutSinks,
    SourceWithoutName,
    SourceWithoutSince(String),
    SourceWithoutCapture(String),
    SourceNamedTwice(String),
    SourcesTakingOverTogether(String, String),
    SinkWithoutName(usize),
    SinkWithoutChangeLog(String),
    SinkWithoutWarehouse(String),
    SinkWithoutReplay(String),
    SinkNamedTwice(String),
}

impl Display for Miswiring {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithoutSources => write!(formatter, "no server is captured"),
            Self::WithoutSinks => write!(formatter, "nothing holds the change log"),
            Self::SourceWithoutName => write!(formatter, "a server is captured without a name"),
            Self::SourceWithoutSince(name) => {
                write!(formatter, "{name} does not say since when it is captured")
            }
            Self::SourceWithoutCapture(name) => {
                write!(formatter, "{name} does not say how it is captured")
            }
            Self::SourceNamedTwice(name) => write!(formatter, "{name} is captured twice"),
            Self::SourcesTakingOverTogether(taking, over) => {
                write!(formatter, "{taking} and {over} take over at the same time")
            }
            Self::SinkWithoutName(at) => write!(formatter, "sink {} is not named", at + 1),
            Self::SinkWithoutChangeLog(name) => {
                write!(formatter, "{name} holds no change log")
            }
            Self::SinkWithoutWarehouse(name) => write!(formatter, "{name} names no warehouse"),
            Self::SinkWithoutReplay(name) => write!(formatter, "{name} builds nothing"),
            Self::SinkNamedTwice(name) => write!(formatter, "{name} is sunk into twice"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source<C> {
    name: String,
    since: DateTime<Utc>,
    capture: C,
    schema_changes: Option<String>,
}

impl<C> Source<C> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn capture(&self) -> &C {
        &self.capture
    }

    pub fn since(&self) -> RecordedAt {
        RecordedAt::from(self.since.timestamp_millis())
    }

    pub fn schema_changes(&self) -> Option<&str> {
        self.schema_changes.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sink<W> {
    name: String,
    change_log: String,
    warehouse: W,
    replay: ReplayTargets,
}

impl<W> Sink<W> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn change_log(&self) -> &str {
        &self.change_log
    }

    pub fn schema_changes<C>(&self, source: &Source<C>) -> Option<TablePath> {
        source
            .schema_changes()
            .map(|topic| TablePath::new(&self.change_log, topic))
    }

    pub fn row_changes<C>(&self, source: &Source<C>, table: &TableId) -> Option<TablePath> {
        source.schema_changes().map(|topic| {
            TablePath::new(
                &self.change_log,
                &format!("{topic}.{}.{}", table.schema(), table.table()),
            )
        })
    }

    pub fn warehouse(&self) -> &W {
        &self.warehouse
    }

    pub fn replay(&self) -> &ReplayTargets {
        &self.replay
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayTargets {
    snapshot: String,
    history: String,
    shard: String,
}

impl ReplayTargets {
    pub fn namespace(&self, artifact: Artifact) -> &str {
        match artifact {
            Artifact::Snapshot => &self.snapshot,
            Artifact::History => &self.history,
        }
    }

    pub fn shard(&self) -> &str {
        &self.shard
    }

    pub fn target(&self, artifact: Artifact, table: &TableId, at: DateTime<Utc>) -> TablePath {
        TablePath::new(
            self.namespace(artifact),
            &format!("{}_{}", table.table(), at.format(&self.shard)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Captured {
        Debezium { dialect: String },
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Sunk {
        Files { bucket: String },
        Warehouse { project: String },
    }

    const INFRA: &str = r#"
        [default]
        source.capture.debezium.dialect = "mysql"
        sink.change_log = "mysql_shop_cdc"
        sink.replay.snapshot = "mysql_shop"
        sink.replay.history = "mysql_shop_history"
        sink.replay.shard = "%Y%m%d"

        [[source]]
        name = "primary"
        since = "1970-01-01T00:00:00Z"
        schema_changes = "mysql"

        [[source]]
        name = "replica"
        since = "2026-09-21T00:00:00Z"

        capture.debezium.dialect = "postgres"

        [[sink]]
        name = "minio"
        warehouse.files.bucket = "event-replay"

        [[sink]]
        name = "warehouse"
        change_log = "mysql_shop_archive"
        warehouse.warehouse.project = "local-project"
    "#;

    const SERVER: &str = r#"
        [[source]]
        name = "primary"
        since = "1970-01-01T00:00:00Z"

        capture.debezium.dialect = "mysql"
    "#;

    const WAREHOUSE: &str = r#"
        warehouse.files.bucket = "event-replay"
    "#;

    const NAMED: &str = r#"
        name = "minio"
    "#;

    const REPLAY: &str = r#"
        replay.snapshot = "mysql_shop"
        replay.history = "mysql_shop_history"
        replay.shard = "%Y%m%d"
    "#;

    fn wiring(written: &str) -> Wiring<Captured, Sunk> {
        toml::from_str(written).unwrap()
    }

    fn wired(server: &str, sink: &str) -> Wiring<Captured, Sunk> {
        wiring(&format!("{server}\n[[sink]]\n{NAMED}{sink}"))
    }

    fn sources() -> Vec<Source<Captured>> {
        wiring(INFRA).sources().unwrap()
    }

    fn sinks() -> Vec<Sink<Sunk>> {
        wiring(INFRA).sinks().unwrap()
    }

    #[test]
    fn a_source_is_captured_as_the_dialect_it_names() {
        assert_eq!(
            sources()[1].capture(),
            &Captured::Debezium {
                dialect: "postgres".to_owned()
            }
        );
    }

    #[test]
    fn a_source_that_names_no_capture_is_captured_like_the_others() {
        assert_eq!(
            sources()[0].capture(),
            &Captured::Debezium {
                dialect: "mysql".to_owned()
            }
        );
    }

    #[test]
    fn the_servers_of_a_pipeline_are_read_in_the_order_they_took_over() {
        let written = INFRA.replace("1970-01-01T00:00:00Z", "2030-01-01T00:00:00Z");

        let sources = wiring(&written).sources().unwrap();

        assert_eq!(
            sources.iter().map(Source::name).collect::<Vec<_>>(),
            ["replica", "primary"]
        );
        assert!(sources[0].since() < sources[1].since());
    }

    #[test]
    fn a_server_that_took_over_at_an_unreadable_time_is_rejected() {
        let unknown = INFRA.replace("\"2026-09-21T00:00:00Z\"", "\"yesterday afternoon\"");

        assert!(toml::from_str::<Wiring<Captured, Sunk>>(&unknown).is_err());
    }

    #[test]
    fn a_source_without_a_name_is_rejected() {
        let nameless = INFRA.replace("name = \"replica\"\n", "");

        assert_eq!(
            wiring(&nameless).sources().unwrap_err(),
            Miswiring::SourceWithoutName
        );
    }

    #[test]
    fn a_source_named_twice_is_rejected() {
        let twice = INFRA.replace("name = \"replica\"", "name = \"primary\"");

        assert_eq!(
            wiring(&twice).sources().unwrap_err(),
            Miswiring::SourceNamedTwice("primary".to_owned())
        );
    }

    #[test]
    fn a_pipeline_that_captures_no_server_is_rejected() {
        let wiring = wiring(
            "[[sink]]\nname = \"minio\"\nchange_log = \"mysql_shop_cdc\"\nwarehouse.files.bucket = \"event-replay\"\nreplay.snapshot = \"mysql_shop\"\nreplay.history = \"mysql_shop_history\"\nreplay.shard = \"%Y%m%d\"",
        );

        assert_eq!(wiring.sources().unwrap_err(), Miswiring::WithoutSources);
    }

    #[test]
    fn a_pipeline_sunk_nowhere_is_rejected() {
        let wiring = wiring(
            "[[source]]\nname = \"primary\"\nsince = \"1970-01-01T00:00:00Z\"\ncapture.debezium.dialect = \"mysql\"",
        );

        assert_eq!(wiring.sinks().unwrap_err(), Miswiring::WithoutSinks);
    }

    #[test]
    fn a_sink_holds_the_change_log_the_pipeline_names() {
        assert_eq!(sinks()[0].name(), "minio");
        assert_eq!(sinks()[0].change_log(), "mysql_shop_cdc");
    }

    #[test]
    fn a_sink_that_names_its_own_change_log_keeps_it() {
        assert_eq!(sinks()[1].change_log(), "mysql_shop_archive");
    }

    #[test]
    fn a_sink_says_where_the_schema_changes_of_a_source_landed() {
        assert_eq!(
            sinks()[0].schema_changes(&sources()[0]),
            Some(TablePath::new("mysql_shop_cdc", "mysql"))
        );
    }

    #[test]
    fn a_sink_says_where_the_row_changes_of_a_table_landed_under_the_topic_of_its_source() {
        assert_eq!(
            sinks()[0].row_changes(&sources()[0], &TableId::new("shop", "customers")),
            Some(TablePath::new("mysql_shop_cdc", "mysql.shop.customers"))
        );
    }

    #[test]
    fn a_snapshot_is_built_into_its_namespace_under_the_shard_of_the_day_it_covers() {
        let at = DateTime::parse_from_rfc3339("2026-09-20T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(
            sinks()[0]
                .replay()
                .target(Artifact::Snapshot, &TableId::new("shop", "customers"), at),
            TablePath::new("mysql_shop", "customers_20260920")
        );
    }

    #[test]
    fn a_source_captured_without_a_schema_change_topic_lands_no_schema_changes() {
        assert_eq!(sinks()[0].schema_changes(&sources()[1]), None);
        assert_eq!(
            sinks()[0].row_changes(&sources()[1], &TableId::new("shop", "customers")),
            None
        );
    }

    #[test]
    fn a_sink_builds_each_artifact_in_the_namespace_the_pipeline_names() {
        let replay = sinks()[1].replay().clone();

        assert_eq!(replay.namespace(Artifact::Snapshot), "mysql_shop");
        assert_eq!(replay.namespace(Artifact::History), "mysql_shop_history");
        assert_eq!(replay.shard(), "%Y%m%d");
    }

    #[test]
    fn a_source_that_says_since_when_nowhere_is_rejected() {
        let wiring = wired(
            "[[source]]\nname = \"primary\"\ncapture.debezium.dialect = \"mysql\"",
            &format!("change_log = \"mysql_shop_cdc\"\n{WAREHOUSE}{REPLAY}"),
        );

        assert_eq!(
            wiring.sources().unwrap_err(),
            Miswiring::SourceWithoutSince("primary".to_owned())
        );
    }

    #[test]
    fn a_source_captured_in_no_named_way_is_rejected() {
        let wiring = wired(
            "[[source]]\nname = \"primary\"\nsince = \"1970-01-01T00:00:00Z\"",
            &format!("change_log = \"mysql_shop_cdc\"\n{WAREHOUSE}{REPLAY}"),
        );

        assert_eq!(
            wiring.sources().unwrap_err(),
            Miswiring::SourceWithoutCapture("primary".to_owned())
        );
    }

    #[test]
    fn two_servers_taking_over_at_the_same_time_are_rejected() {
        let together = INFRA.replace("2026-09-21T00:00:00Z", "1970-01-01T00:00:00Z");

        assert_eq!(
            wiring(&together).sources().unwrap_err(),
            Miswiring::SourcesTakingOverTogether("primary".to_owned(), "replica".to_owned())
        );
    }

    #[test]
    fn a_sink_holding_no_change_log_anywhere_is_rejected() {
        let wiring = wired(SERVER, &format!("{WAREHOUSE}{REPLAY}"));

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SinkWithoutChangeLog("minio".to_owned())
        );
    }

    #[test]
    fn a_sink_into_no_warehouse_anywhere_is_rejected() {
        let wiring = wired(
            SERVER,
            &format!("change_log = \"mysql_shop_cdc\"\n{REPLAY}"),
        );

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SinkWithoutWarehouse("minio".to_owned())
        );
    }

    #[test]
    fn a_sink_that_builds_nothing_anywhere_is_rejected() {
        let wiring = wired(
            SERVER,
            &format!("change_log = \"mysql_shop_cdc\"\n{WAREHOUSE}"),
        );

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SinkWithoutReplay("minio".to_owned())
        );
    }

    #[test]
    fn a_sink_named_twice_is_rejected() {
        let sink = format!("{NAMED}change_log = \"mysql_shop_cdc\"\n{WAREHOUSE}{REPLAY}");
        let wiring = wiring(&format!("{SERVER}\n[[sink]]\n{sink}\n[[sink]]\n{sink}"));

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SinkNamedTwice("minio".to_owned())
        );
    }

    #[test]
    fn a_sink_without_a_name_is_rejected() {
        let wiring = wiring(&format!(
            "{SERVER}\n[[sink]]\nchange_log = \"mysql_shop_cdc\"\n{WAREHOUSE}{REPLAY}"
        ));

        assert_eq!(wiring.sinks().unwrap_err(), Miswiring::SinkWithoutName(0));
    }

    #[test]
    fn a_sink_carries_the_warehouse_configuration_as_it_stands() {
        assert_eq!(
            sinks()[0].warehouse(),
            &Sunk::Files {
                bucket: "event-replay".to_owned()
            }
        );
    }

    #[test]
    fn a_pipeline_sunk_into_two_warehouses_keeps_each_configuration() {
        assert_eq!(
            sinks()[1].warehouse(),
            &Sunk::Warehouse {
                project: "local-project".to_owned()
            }
        );
    }

    #[test]
    fn a_sink_into_a_warehouse_the_application_does_not_know_is_rejected() {
        let unknown = INFRA.replace("warehouse.files", "warehouse.redshift");

        assert!(toml::from_str::<Wiring<Captured, Sunk>>(&unknown).is_err());
    }
}
