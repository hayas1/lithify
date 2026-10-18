use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Utc};
use semigroup::Semigroup;
use serde::{Deserialize, Serialize};

use lithify_core::schedule::Schedule;
use lithify_core::sink::SinkWarehouse;
use lithify_core::ts::{Event, Ts};

use crate::ArtifactKind;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(bound(
    deserialize = "C: Deserialize<'de>, W: Deserialize<'de>, W::Location: Deserialize<'de>"
))]
#[serde(bound(serialize = "C: Serialize, W: Serialize, W::Location: Serialize"))]
pub(crate) struct Wiring<C, W: SinkWarehouse> {
    #[serde(default)]
    default: Defaults<C, W>,
    #[serde(default, rename = "source")]
    sources: Vec<WiredSource<C>>,
    #[serde(default, rename = "sink")]
    sinks: Vec<WiredSink<W>>,
}

impl<C: Clone, W: Clone + SinkWarehouse> Wiring<C, W> {
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
#[serde(bound(
    deserialize = "C: Deserialize<'de>, W: Deserialize<'de>, W::Location: Deserialize<'de>"
))]
#[serde(bound(serialize = "C: Serialize, W: Serialize, W::Location: Serialize"))]
struct Defaults<C, W: SinkWarehouse> {
    #[serde(default)]
    source: WiredSource<C>,
    #[serde(default)]
    sink: WiredSink<W>,
}

impl<C, W: SinkWarehouse> Default for Defaults<C, W> {
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
    topic_prefix: Option<String>,
}

impl<C> Default for WiredSource<C> {
    fn default() -> Self {
        Self {
            name: None,
            since: None,
            capture: None,
            topic_prefix: None,
        }
    }
}

impl<C: Clone> WiredSource<C> {
    fn source(self, default: &Self) -> Result<Source<C>, Miswiring> {
        let wired = self.semigroup(default.clone());
        let name = wired.name.ok_or(Miswiring::SourceWithoutName)?;

        Ok(Source {
            since: wired.since.unwrap_or(DateTime::UNIX_EPOCH),
            capture: wired
                .capture
                .ok_or_else(|| Miswiring::SourceWithoutCapture(name.clone()))?,
            name,
            topic_prefix: wired.topic_prefix,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Semigroup)]
#[semigroup(with = "semigroup::op::Coalesce")]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "W: Deserialize<'de>, W::Location: Deserialize<'de>"))]
#[serde(bound(serialize = "W: Serialize, W::Location: Serialize"))]
struct WiredSink<W: SinkWarehouse> {
    name: Option<String>,
    change_log: Option<W::Location>,
    warehouse: Option<W>,
    artifacts: Option<ArtifactTargets>,
}

impl<W: SinkWarehouse> Default for WiredSink<W> {
    fn default() -> Self {
        Self {
            name: None,
            change_log: None,
            warehouse: None,
            artifacts: None,
        }
    }
}

impl<W: Clone + SinkWarehouse> WiredSink<W> {
    fn sink(self, at: usize, default: &Self) -> Result<Sink<W>, Miswiring> {
        let wired = self.semigroup(default.clone());
        let name = wired.name.ok_or(Miswiring::SinkWithoutName(at))?;
        let change_log = wired
            .change_log
            .ok_or_else(|| Miswiring::SinkWithoutChangeLog(name.clone()))?;
        let warehouse = wired
            .warehouse
            .ok_or_else(|| Miswiring::SinkWithoutWarehouse(name.clone()))?;
        if !warehouse.reaches(&change_log) {
            return Err(Miswiring::ChangeLogUnreached(name));
        }

        Ok(Sink {
            change_log,
            warehouse,
            artifacts: wired
                .artifacts
                .ok_or_else(|| Miswiring::SinkWithoutArtifacts(name.clone()))?,
            name,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Miswiring {
    WithoutSources,
    WithoutSinks,
    SourceWithoutName,
    SourceWithoutCapture(String),
    SourceNamedTwice(String),
    SourcesTakingOverTogether(String, String),
    SinkWithoutName(usize),
    SinkWithoutChangeLog(String),
    ChangeLogUnreached(String),
    SinkWithoutWarehouse(String),
    SinkWithoutArtifacts(String),
    SinkNamedTwice(String),
}

impl Display for Miswiring {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithoutSources => write!(formatter, "no server is captured"),
            Self::WithoutSinks => write!(formatter, "nothing holds the change log"),
            Self::SourceWithoutName => write!(formatter, "a server is captured without a name"),
            Self::SourceWithoutCapture(name) => {
                write!(formatter, "{name} does not say how it is captured")
            }
            Self::SourceNamedTwice(name) => write!(formatter, "{name} is captured twice"),
            Self::SourcesTakingOverTogether(taking, over) => {
                write!(
                    formatter,
                    "{taking} and {over} take over at the same time; does one of them forget to say since when it is captured?"
                )
            }
            Self::SinkWithoutName(at) => write!(formatter, "sink {} is not named", at + 1),
            Self::SinkWithoutChangeLog(name) => {
                write!(formatter, "{name} holds no change log")
            }
            Self::ChangeLogUnreached(name) => write!(
                formatter,
                "{name} holds its change log in a warehouse other than the one it is sunk into"
            ),
            Self::SinkWithoutWarehouse(name) => write!(formatter, "{name} names no warehouse"),
            Self::SinkWithoutArtifacts(name) => write!(formatter, "{name} builds nothing"),
            Self::SinkNamedTwice(name) => write!(formatter, "{name} is sunk into twice"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source<C> {
    name: String,
    since: DateTime<Utc>,
    capture: C,
    topic_prefix: Option<String>,
}

impl<C> Source<C> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn capture(&self) -> &C {
        &self.capture
    }

    pub fn since(&self) -> Ts<Event> {
        Ts::from(self.since)
    }

    pub fn topic_prefix(&self) -> Option<&str> {
        self.topic_prefix.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sink<W: SinkWarehouse> {
    name: String,
    change_log: W::Location,
    warehouse: W,
    artifacts: ArtifactTargets,
}

impl<W: SinkWarehouse> Sink<W> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn change_log(&self) -> &W::Location {
        &self.change_log
    }

    pub fn warehouse(&self) -> &W {
        &self.warehouse
    }

    pub fn artifacts(&self) -> &ArtifactTargets {
        &self.artifacts
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactTargets {
    snapshot: String,
    history: String,
    shard: String,
}

impl ArtifactTargets {
    pub fn namespace(&self, kind: ArtifactKind) -> &str {
        match kind {
            ArtifactKind::Snapshot => &self.snapshot,
            ArtifactKind::History => &self.history,
        }
    }

    pub fn shard(&self) -> &str {
        &self.shard
    }

    pub fn schedule(&self) -> Schedule {
        Schedule::Daily
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

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Lying {
        Files { prefix: String },
        Warehouse { dataset: String },
    }

    impl SinkWarehouse for Sunk {
        type Location = Lying;
        type Credential = ();
        type Error = fmt::Error;

        fn reaches(&self, location: &Lying) -> bool {
            matches!(
                (self, location),
                (Self::Files { .. }, Lying::Files { .. })
                    | (Self::Warehouse { .. }, Lying::Warehouse { .. })
            )
        }

        fn with_client<F: lithify_core::sink::WarehouseFn>(
            &self,
            _: &Lying,
            _: &(),
            _: F,
        ) -> Result<F::Output, fmt::Error> {
            Err(fmt::Error)
        }
    }

    fn prefix(prefix: &str) -> Lying {
        Lying::Files {
            prefix: prefix.to_owned(),
        }
    }

    const INFRA: &str = r#"
        [default]
        source.capture.debezium.dialect = "mysql"
        sink.change_log.files.prefix = "mysql_shop_cdc"
        sink.artifacts.snapshot = "mysql_shop"
        sink.artifacts.history = "mysql_shop_history"
        sink.artifacts.shard = "%Y%m%d"

        [[source]]
        name = "primary"
        since = "1970-01-01T00:00:00Z"
        topic_prefix = "mysql"

        [[source]]
        name = "replica"
        since = "2026-09-21T00:00:00Z"

        capture.debezium.dialect = "postgres"

        [[sink]]
        name = "rustfs"
        warehouse.files.bucket = "lithify"

        [[sink]]
        name = "warehouse"
        change_log.warehouse.dataset = "mysql_shop_archive"
        warehouse.warehouse.project = "local-project"
    "#;

    const SERVER: &str = r#"
        [[source]]
        name = "primary"
        since = "1970-01-01T00:00:00Z"

        capture.debezium.dialect = "mysql"
    "#;

    const WAREHOUSE: &str = r#"
        warehouse.files.bucket = "lithify"
    "#;

    const NAMED: &str = r#"
        name = "rustfs"
    "#;

    const ARTIFACTS: &str = r#"
        artifacts.snapshot = "mysql_shop"
        artifacts.history = "mysql_shop_history"
        artifacts.shard = "%Y%m%d"
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
            "[[sink]]\nname = \"rustfs\"\nchange_log.files.prefix = \"mysql_shop_cdc\"\nwarehouse.files.bucket = \"lithify\"\nartifacts.snapshot = \"mysql_shop\"\nartifacts.history = \"mysql_shop_history\"\nartifacts.shard = \"%Y%m%d\"",
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
        assert_eq!(sinks()[0].name(), "rustfs");
        assert_eq!(sinks()[0].change_log(), &prefix("mysql_shop_cdc"));
    }

    #[test]
    fn a_sink_that_names_its_own_change_log_keeps_it() {
        assert_eq!(
            sinks()[1].change_log(),
            &Lying::Warehouse {
                dataset: "mysql_shop_archive".to_owned()
            }
        );
    }

    #[test]
    fn a_sink_says_the_namespace_its_change_logs_landed_in() {
        assert_eq!(sinks()[0].change_log(), &prefix("mysql_shop_cdc"));
    }

    #[test]
    fn a_source_says_the_topic_prefix_of_its_connector_unless_it_captures_none() {
        assert_eq!(sources()[0].topic_prefix(), Some("mysql"));
        assert_eq!(sources()[1].topic_prefix(), None);
    }

    #[test]
    fn a_sink_builds_each_artifact_in_the_namespace_the_pipeline_names() {
        let artifacts = sinks()[1].artifacts().clone();

        assert_eq!(artifacts.namespace(ArtifactKind::Snapshot), "mysql_shop");
        assert_eq!(
            artifacts.namespace(ArtifactKind::History),
            "mysql_shop_history"
        );
        assert_eq!(artifacts.shard(), "%Y%m%d");
    }

    #[test]
    fn a_sink_builds_its_artifacts_daily() {
        assert_eq!(sinks()[1].artifacts().schedule(), Schedule::Daily);
    }

    #[test]
    fn a_schedule_written_in_the_pipeline_is_rejected() {
        let written = toml::from_str::<Wiring<Captured, Sunk>>(&format!(
            "{SERVER}\n[[sink]]\n{NAMED}{WAREHOUSE}{ARTIFACTS}artifacts.schedule = \"daily\"\n"
        ));

        assert!(written.is_err());
    }

    #[test]
    fn a_source_that_says_since_when_nowhere_is_captured_since_the_epoch() {
        let wiring = wired(
            "[[source]]\nname = \"primary\"\ncapture.debezium.dialect = \"mysql\"",
            &format!("change_log.files.prefix = \"mysql_shop_cdc\"\n{WAREHOUSE}{ARTIFACTS}"),
        );

        assert_eq!(
            wiring.sources().unwrap()[0].since(),
            Ts::try_from(0).unwrap()
        );
    }

    #[test]
    fn a_source_captured_in_no_named_way_is_rejected() {
        let wiring = wired(
            "[[source]]\nname = \"primary\"\nsince = \"1970-01-01T00:00:00Z\"",
            &format!("change_log.files.prefix = \"mysql_shop_cdc\"\n{WAREHOUSE}{ARTIFACTS}"),
        );

        assert_eq!(
            wiring.sources().unwrap_err(),
            Miswiring::SourceWithoutCapture("primary".to_owned())
        );
    }

    #[test]
    fn two_servers_that_say_since_when_nowhere_take_over_together() {
        let sinceless = INFRA
            .replace("since = \"1970-01-01T00:00:00Z\"\n", "")
            .replace("since = \"2026-09-21T00:00:00Z\"\n", "");

        assert_eq!(
            wiring(&sinceless).sources().unwrap_err(),
            Miswiring::SourcesTakingOverTogether("primary".to_owned(), "replica".to_owned())
        );
    }

    #[test]
    fn two_servers_taking_over_together_are_asked_whether_one_forgot_since_when() {
        assert!(
            Miswiring::SourcesTakingOverTogether("primary".to_owned(), "replica".to_owned())
                .to_string()
                .contains("since")
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
        let wiring = wired(SERVER, &format!("{WAREHOUSE}{ARTIFACTS}"));

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SinkWithoutChangeLog("rustfs".to_owned())
        );
    }

    #[test]
    fn a_sink_holding_its_change_log_in_another_warehouse_is_rejected() {
        let elsewhere = INFRA.replace(
            "change_log.warehouse.dataset = \"mysql_shop_archive\"",
            "change_log.files.prefix = \"mysql_shop_archive\"",
        );

        assert_eq!(
            wiring(&elsewhere).sinks().unwrap_err(),
            Miswiring::ChangeLogUnreached("warehouse".to_owned())
        );
    }

    #[test]
    fn a_sink_into_no_warehouse_anywhere_is_rejected() {
        let wiring = wired(
            SERVER,
            &format!("change_log.files.prefix = \"mysql_shop_cdc\"\n{ARTIFACTS}"),
        );

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SinkWithoutWarehouse("rustfs".to_owned())
        );
    }

    #[test]
    fn a_sink_that_builds_nothing_anywhere_is_rejected() {
        let wiring = wired(
            SERVER,
            &format!("change_log.files.prefix = \"mysql_shop_cdc\"\n{WAREHOUSE}"),
        );

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SinkWithoutArtifacts("rustfs".to_owned())
        );
    }

    #[test]
    fn a_sink_named_twice_is_rejected() {
        let sink =
            format!("{NAMED}change_log.files.prefix = \"mysql_shop_cdc\"\n{WAREHOUSE}{ARTIFACTS}");
        let wiring = wiring(&format!("{SERVER}\n[[sink]]\n{sink}\n[[sink]]\n{sink}"));

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SinkNamedTwice("rustfs".to_owned())
        );
    }

    #[test]
    fn a_sink_without_a_name_is_rejected() {
        let wiring = wiring(&format!(
            "{SERVER}\n[[sink]]\nchange_log.files.prefix = \"mysql_shop_cdc\"\n{WAREHOUSE}{ARTIFACTS}"
        ));

        assert_eq!(wiring.sinks().unwrap_err(), Miswiring::SinkWithoutName(0));
    }

    #[test]
    fn a_sink_carries_the_warehouse_configuration_as_it_stands() {
        assert_eq!(
            sinks()[0].warehouse(),
            &Sunk::Files {
                bucket: "lithify".to_owned()
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
