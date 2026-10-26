use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Utc};
use semigroup::Semigroup;
use serde::{Deserialize, Serialize};

use chrono_tz::Tz;
use serde::de::DeserializeOwned;

use lithify_core::sink::SinkWarehouse;
use lithify_core::ts::{Event, Ts};

use crate::artifact::{Declaration, Declarations};
use crate::place::{Place, Places, Placing};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "C: Deserialize<'de>, W: Deserialize<'de>, A: DeserializeOwned"))]
pub(crate) struct Wiring<C, W, A> {
    #[serde(default)]
    time_zone: Option<Tz>,
    #[serde(default)]
    artifact: Declarations<A>,
    #[serde(default)]
    default: Defaults<C, W>,
    #[serde(default, rename = "source")]
    sources: Vec<WiredSource<C>>,
    #[serde(default, rename = "sink")]
    sinks: Vec<WiredSink<W>>,
}

impl<C, W, A> Wiring<C, W, A>
where
    C: Clone,
    W: Clone + Placing,
    A: Clone + Declaration,
{
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

    pub(crate) fn sinks(&self) -> Result<Vec<Sink<W, A>>, Miswiring> {
        if self.sinks.is_empty() {
            return Err(Miswiring::WithoutSinks);
        }

        let sinks = self
            .sinks
            .iter()
            .enumerate()
            .map(|(at, wired)| {
                wired.clone().sink(
                    at,
                    &self.default.sink,
                    &self.artifact,
                    self.time_zone.unwrap_or(Tz::UTC),
                )
            })
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "C: Deserialize<'de>, W: Deserialize<'de>"))]
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

#[derive(Debug, Clone, Deserialize, Semigroup)]
#[semigroup(with = "semigroup::op::Coalesce")]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "W: Deserialize<'de>"))]
struct WiredSink<W> {
    name: Option<String>,
    warehouse: Option<W>,
}

impl<W> Default for WiredSink<W> {
    fn default() -> Self {
        Self {
            name: None,
            warehouse: None,
        }
    }
}

impl<W: Clone + Placing> WiredSink<W> {
    fn sink<A: Clone + Declaration>(
        self,
        at: usize,
        default: &Self,
        declarations: &Declarations<A>,
        time_zone: Tz,
    ) -> Result<Sink<W, A>, Miswiring> {
        let wired = self.semigroup(default.clone());
        let name = wired.name.ok_or(Miswiring::SinkWithoutName(at))?;
        let warehouse = wired
            .warehouse
            .ok_or_else(|| Miswiring::SinkWithoutWarehouse(name.clone()))?;
        let places = warehouse.places();
        if places.is_empty() {
            return Err(Miswiring::SinkWithoutArtifacts(name));
        }
        for (artifact, place) in places.iter() {
            Self::placed(&name, artifact, place, declarations, &places)?;
        }

        Ok(Sink {
            change_log: warehouse.change_log(),
            artifacts: declarations.placed(&places),
            places,
            warehouse,
            time_zone,
            name,
        })
    }

    fn placed<A: Declaration>(
        sink: &str,
        artifact: &str,
        place: &Place<W::Location>,
        declarations: &Declarations<A>,
        places: &Places<W::Location>,
    ) -> Result<(), Miswiring> {
        let misplaced = |misplacing: fn(String, String) -> Miswiring| {
            misplacing(sink.to_owned(), artifact.to_owned())
        };
        let declared = declarations
            .get(artifact)
            .ok_or_else(|| misplaced(Miswiring::Undeclared))?;
        match (declared.cuts(), place) {
            (None, Place::Whole(_)) => {}
            (Some(cuts), Place::Cuts(placed)) => {
                if let Some(uncut) = placed.keys().find(|cut| !cuts.contains(cut)) {
                    return Err(Miswiring::Uncut {
                        sink: sink.to_owned(),
                        artifact: artifact.to_owned(),
                        cut: uncut.to_owned(),
                    });
                }
            }
            (None, Place::Cuts(_)) | (Some(_), Place::Whole(_)) => {
                return Err(misplaced(Miswiring::Misplaced));
            }
        }
        if place.lists().iter().any(|listed| listed.is_empty()) {
            return Err(misplaced(Miswiring::Unplaced));
        }
        match declared.source() {
            Some(source) if places.get(source).is_none() => Err(Miswiring::SourceUnplaced {
                sink: sink.to_owned(),
                artifact: artifact.to_owned(),
                source: source.to_owned(),
            }),
            _ => Ok(()),
        }
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
    SinkWithoutWarehouse(String),
    SinkWithoutArtifacts(String),
    Undeclared(String, String),
    Misplaced(String, String),
    Uncut {
        sink: String,
        artifact: String,
        cut: String,
    },
    Unplaced(String, String),
    SourceUnplaced {
        sink: String,
        artifact: String,
        source: String,
    },
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
            Self::SinkWithoutWarehouse(name) => write!(formatter, "{name} names no warehouse"),
            Self::SinkWithoutArtifacts(name) => write!(formatter, "{name} places nothing"),
            Self::Undeclared(sink, artifact) => write!(
                formatter,
                "{sink} places {artifact}, which is declared nowhere under [artifact]"
            ),
            Self::Misplaced(sink, artifact) => write!(
                formatter,
                "{sink} places {artifact} as a list where it is placed by each instant it is cut at, or the other way round"
            ),
            Self::Uncut {
                sink,
                artifact,
                cut,
            } => write!(
                formatter,
                "{sink} places {artifact} cut at {cut}, which is declared nowhere under [artifact]"
            ),
            Self::Unplaced(sink, artifact) => {
                write!(formatter, "{sink} places {artifact} nowhere")
            }
            Self::SourceUnplaced {
                sink,
                artifact,
                source,
            } => write!(
                formatter,
                "{sink} places {artifact} without {source} it is built from"
            ),
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
pub struct Sink<W: SinkWarehouse, A> {
    name: String,
    change_log: W::Location,
    warehouse: W,
    artifacts: Declarations<A>,
    places: Places<W::Location>,
    time_zone: Tz,
}

impl<W: SinkWarehouse, A> Sink<W, A> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn change_log(&self) -> &W::Location {
        &self.change_log
    }

    pub fn warehouse(&self) -> &W {
        &self.warehouse
    }

    pub fn artifact(&self, artifact: &str) -> Option<&A> {
        self.artifacts.get(artifact)
    }

    pub fn place(&self, artifact: &str) -> Option<&Place<W::Location>> {
        self.places.get(artifact)
    }

    pub fn time_zone(&self) -> Tz {
        self.time_zone
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::place::Placed;

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Captured {
        Debezium { dialect: String },
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Bucket {
        bucket: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Project {
        project: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Prefix {
        prefix: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Dataset {
        dataset: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Sunk {
        Files(Placed<Bucket, Prefix>),
        Warehouse(Placed<Project, Dataset>),
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Lying {
        Files(Prefix),
        Warehouse(Dataset),
    }

    impl From<Lying> for lithify_core::sink::Namespace {
        fn from(location: Lying) -> Self {
            match location {
                Lying::Files(Prefix { prefix }) => Self::from(prefix.as_str()),
                Lying::Warehouse(Dataset { dataset }) => Self::from(dataset.as_str()),
            }
        }
    }

    impl SinkWarehouse for Sunk {
        type Location = Lying;
        type Credential = ();
        type Error = fmt::Error;

        fn reaches(&self, location: &Lying) -> bool {
            matches!(
                (self, location),
                (Self::Files(_), Lying::Files(_)) | (Self::Warehouse(_), Lying::Warehouse(_))
            )
        }

        async fn with_client<F: lithify_core::sink::WarehouseFn>(
            &self,
            _: &Lying,
            _: &(),
            _: F,
        ) -> Result<F::Output, fmt::Error> {
            Err(fmt::Error)
        }
    }

    impl Placing for Sunk {
        fn change_log(&self) -> Lying {
            match self {
                Self::Files(placed) => Lying::Files(placed.change_log().clone()),
                Self::Warehouse(placed) => Lying::Warehouse(placed.change_log().clone()),
            }
        }

        fn places(&self) -> Places<Lying> {
            match self {
                Self::Files(placed) => placed.places().clone().map(Lying::Files),
                Self::Warehouse(placed) => placed.places().clone().map(Lying::Warehouse),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Cut {
        at: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Made {
        Snapshot { shard: String },
        Copy {},
        Instant(Vec<Cut>),
    }

    impl Declaration for Made {
        fn source(&self) -> Option<&'static str> {
            match self {
                Self::Snapshot { .. } => None,
                Self::Copy {} | Self::Instant(_) => Some("snapshot"),
            }
        }

        fn cuts(&self) -> Option<Vec<String>> {
            match self {
                Self::Snapshot { .. } | Self::Copy {} => None,
                Self::Instant(cuts) => Some(cuts.iter().map(|cut| cut.at.clone()).collect()),
            }
        }
    }

    fn prefix(prefix: &str) -> Lying {
        Lying::Files(Prefix {
            prefix: prefix.to_owned(),
        })
    }

    fn dataset(dataset: &str) -> Lying {
        Lying::Warehouse(Dataset {
            dataset: dataset.to_owned(),
        })
    }

    const INFRA: &str = r#"
        [artifact.snapshot]
        shard = "%Y%m%d"

        [artifact.copy]

        [[artifact.instant]]
        at = "09:00"

        [[artifact.instant]]
        at = "18:00"

        [default]
        source.capture.debezium.dialect = "mysql"

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

        [sink.warehouse.files]
        bucket = "lithify"
        change_log = { prefix = "mysql_shop_cdc" }
        place.snapshot = [{ prefix = "mysql_shop" }]
        place.instant."09:00" = [{ prefix = "mysql_shop_0900" }]

        [[sink]]
        name = "warehouse"

        [sink.warehouse.warehouse]
        project = "local-project"
        change_log = { dataset = "mysql_shop_archive" }
        place.snapshot = [{ dataset = "mysql_shop" }]
        place.copy = [{ dataset = "mysql_shop_copy" }]
    "#;

    const SERVER: &str = r#"
        [artifact.snapshot]
        shard = "%Y%m%d"

        [artifact.copy]

        [[source]]
        name = "primary"
        since = "1970-01-01T00:00:00Z"

        capture.debezium.dialect = "mysql"
    "#;

    const WAREHOUSE: &str = r#"
        [sink.warehouse.files]
        bucket = "lithify"
        change_log = { prefix = "mysql_shop_cdc" }
    "#;

    const NAMED: &str = r#"
        name = "rustfs"
    "#;

    const ARTIFACTS: &str = r#"
        place.snapshot = [{ prefix = "mysql_shop" }]
    "#;

    fn wiring(written: &str) -> Wiring<Captured, Sunk, Made> {
        toml::from_str(written).unwrap()
    }

    fn wired(server: &str, sink: &str) -> Wiring<Captured, Sunk, Made> {
        wiring(&format!("{server}\n[[sink]]\n{NAMED}{sink}"))
    }

    fn sources() -> Vec<Source<Captured>> {
        wiring(INFRA).sources().unwrap()
    }

    fn sinks() -> Vec<Sink<Sunk, Made>> {
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

        assert!(toml::from_str::<Wiring<Captured, Sunk, Made>>(&unknown).is_err());
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
        let wiring = wiring(&format!("[[sink]]\n{NAMED}{WAREHOUSE}{ARTIFACTS}"));

        assert_eq!(wiring.sources().unwrap_err(), Miswiring::WithoutSources);
    }

    #[test]
    fn a_pipeline_sunk_nowhere_is_rejected() {
        let wiring = wiring(SERVER);

        assert_eq!(wiring.sinks().unwrap_err(), Miswiring::WithoutSinks);
    }

    #[test]
    fn a_sink_holds_the_change_log_its_warehouse_places() {
        assert_eq!(sinks()[0].name(), "rustfs");
        assert_eq!(sinks()[0].change_log(), &prefix("mysql_shop_cdc"));
        assert_eq!(sinks()[1].change_log(), &dataset("mysql_shop_archive"));
    }

    #[test]
    fn a_source_says_the_topic_prefix_of_its_connector_unless_it_captures_none() {
        assert_eq!(sources()[0].topic_prefix(), Some("mysql"));
        assert_eq!(sources()[1].topic_prefix(), None);
    }

    #[test]
    fn a_sink_reads_the_instants_of_a_day_in_the_time_zone_of_the_pipeline() {
        let zoned = format!("time_zone = \"Asia/Tokyo\"\n{INFRA}");

        assert_eq!(
            wiring(&zoned).sinks().unwrap()[0].time_zone(),
            chrono_tz::Asia::Tokyo
        );
    }

    #[test]
    fn a_pipeline_that_names_no_time_zone_reads_the_instants_of_a_day_in_utc() {
        assert_eq!(sinks()[0].time_zone(), Tz::UTC);
    }

    #[test]
    fn a_time_zone_nobody_keeps_is_rejected() {
        let unknown = format!("time_zone = \"Mars/Olympus\"\n{INFRA}");

        assert!(toml::from_str::<Wiring<Captured, Sunk, Made>>(&unknown).is_err());
    }

    #[test]
    fn a_sink_builds_each_artifact_declared_for_the_pipeline_where_its_warehouse_places_it() {
        let sinks = sinks();

        assert_eq!(
            sinks[0].artifact("snapshot"),
            Some(&Made::Snapshot {
                shard: "%Y%m%d".to_owned()
            })
        );
        assert_eq!(
            sinks[0].place("snapshot").and_then(Place::as_whole),
            Some([prefix("mysql_shop")].as_slice())
        );
        assert_eq!(
            sinks[1].place("snapshot").and_then(Place::as_whole),
            Some([dataset("mysql_shop")].as_slice())
        );
    }

    #[test]
    fn a_sink_builds_no_artifact_its_warehouse_places_nowhere() {
        let sinks = sinks();

        assert_eq!(sinks[0].artifact("copy"), None);
        assert_eq!(sinks[1].artifact("instant"), None);
    }

    #[test]
    fn a_sink_places_each_instant_an_artifact_is_cut_at_on_its_own() {
        let sinks = sinks();
        let instant = sinks[0].place("instant").unwrap();

        assert_eq!(
            instant.cut("09:00"),
            Some([prefix("mysql_shop_0900")].as_slice())
        );
        assert_eq!(instant.cut("18:00"), None);
    }

    #[test]
    fn an_artifact_placed_but_declared_nowhere_is_rejected() {
        let undeclared = INFRA.replace("[artifact.copy]\n", "");

        assert_eq!(
            wiring(&undeclared).sinks().unwrap_err(),
            Miswiring::Undeclared("warehouse".to_owned(), "copy".to_owned())
        );
    }

    #[test]
    fn an_instant_placed_but_declared_nowhere_is_rejected() {
        let uncut = INFRA.replace("place.instant.\"09:00\"", "place.instant.\"12:00\"");

        assert_eq!(
            wiring(&uncut).sinks().unwrap_err(),
            Miswiring::Uncut {
                sink: "rustfs".to_owned(),
                artifact: "instant".to_owned(),
                cut: "12:00".to_owned(),
            }
        );
    }

    #[test]
    fn an_artifact_cut_at_several_instants_placed_as_one_list_is_rejected() {
        let listed = INFRA.replace("place.instant.\"09:00\"", "place.instant");

        assert_eq!(
            wiring(&listed).sinks().unwrap_err(),
            Miswiring::Misplaced("rustfs".to_owned(), "instant".to_owned())
        );
    }

    #[test]
    fn an_artifact_placed_by_instants_it_is_not_cut_at_is_rejected() {
        let by_instant = INFRA.replace(
            "place.copy = [{ dataset = \"mysql_shop_copy\" }]",
            "place.copy.\"09:00\" = [{ dataset = \"mysql_shop_copy\" }]",
        );

        assert_eq!(
            wiring(&by_instant).sinks().unwrap_err(),
            Miswiring::Misplaced("warehouse".to_owned(), "copy".to_owned())
        );
    }

    #[test]
    fn an_artifact_placed_nowhere_is_rejected() {
        let wiring = wired(SERVER, &format!("{WAREHOUSE}place.snapshot = []\n"));

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::Unplaced("rustfs".to_owned(), "snapshot".to_owned())
        );
    }

    #[test]
    fn an_artifact_placed_without_the_one_it_is_built_from_is_rejected() {
        let wiring = wired(
            SERVER,
            &format!("{WAREHOUSE}place.copy = [{{ prefix = \"mysql_shop_copy\" }}]\n"),
        );

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SourceUnplaced {
                sink: "rustfs".to_owned(),
                artifact: "copy".to_owned(),
                source: "snapshot".to_owned(),
            }
        );
    }

    #[test]
    fn a_place_written_the_way_of_another_warehouse_is_rejected() {
        let elsewhere = INFRA.replace(
            "place.copy = [{ dataset = \"mysql_shop_copy\" }]",
            "place.copy = [{ prefix = \"mysql_shop_copy\" }]",
        );

        assert!(toml::from_str::<Wiring<Captured, Sunk, Made>>(&elsewhere).is_err());
    }

    #[test]
    fn a_source_that_says_since_when_nowhere_is_captured_since_the_epoch() {
        let wiring = wired(
            "[artifact.snapshot]\nshard = \"%Y%m%d\"\n[[source]]\nname = \"primary\"\ncapture.debezium.dialect = \"mysql\"",
            &format!("{WAREHOUSE}{ARTIFACTS}"),
        );

        assert_eq!(
            wiring.sources().unwrap()[0].since(),
            Ts::try_from(0).unwrap()
        );
    }

    #[test]
    fn a_source_captured_in_no_named_way_is_rejected() {
        let wiring = wired(
            "[artifact.snapshot]\nshard = \"%Y%m%d\"\n[[source]]\nname = \"primary\"\nsince = \"1970-01-01T00:00:00Z\"",
            &format!("{WAREHOUSE}{ARTIFACTS}"),
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
    fn a_sink_holding_no_change_log_is_rejected() {
        let unlogged = INFRA.replace("change_log = { prefix = \"mysql_shop_cdc\" }\n", "");

        assert!(toml::from_str::<Wiring<Captured, Sunk, Made>>(&unlogged).is_err());
    }

    #[test]
    fn a_sink_into_no_warehouse_anywhere_is_rejected() {
        let wiring = wiring(&format!("{SERVER}\n[[sink]]\n{NAMED}"));

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SinkWithoutWarehouse("rustfs".to_owned())
        );
    }

    #[test]
    fn a_sink_into_the_warehouse_every_sink_is_sunk_into_by_default_places_what_it_places() {
        let wiring = wiring(&format!(
            "{SERVER}\n[default.sink.warehouse.files]\nbucket = \"lithify\"\nchange_log = {{ prefix = \"mysql_shop_cdc\" }}\n{ARTIFACTS}\n[[sink]]\n{NAMED}"
        ));

        assert_eq!(
            wiring.sinks().unwrap()[0]
                .place("snapshot")
                .and_then(Place::as_whole),
            Some([prefix("mysql_shop")].as_slice())
        );
    }

    #[test]
    fn a_sink_that_places_nothing_is_rejected() {
        let wiring = wired(SERVER, WAREHOUSE);

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SinkWithoutArtifacts("rustfs".to_owned())
        );
    }

    #[test]
    fn a_sink_named_twice_is_rejected() {
        let sink = format!("{NAMED}{WAREHOUSE}{ARTIFACTS}");
        let wiring = wiring(&format!("{SERVER}\n[[sink]]\n{sink}\n[[sink]]\n{sink}"));

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SinkNamedTwice("rustfs".to_owned())
        );
    }

    #[test]
    fn a_sink_without_a_name_is_rejected() {
        let wiring = wiring(&format!("{SERVER}\n[[sink]]\n{WAREHOUSE}{ARTIFACTS}"));

        assert_eq!(wiring.sinks().unwrap_err(), Miswiring::SinkWithoutName(0));
    }

    #[test]
    fn a_sink_carries_the_warehouse_configuration_as_it_stands() {
        assert!(matches!(
            sinks()[0].warehouse(),
            Sunk::Files(placed) if placed.warehouse().bucket == "lithify"
        ));
        assert!(matches!(
            sinks()[1].warehouse(),
            Sunk::Warehouse(placed) if placed.warehouse().project == "local-project"
        ));
    }

    #[test]
    fn a_sink_into_a_warehouse_the_application_does_not_know_is_rejected() {
        let unknown = INFRA.replace("sink.warehouse.files", "sink.warehouse.redshift");

        assert!(toml::from_str::<Wiring<Captured, Sunk, Made>>(&unknown).is_err());
    }
}
