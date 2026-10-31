use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Utc};
use semigroup::Semigroup;
use serde::{Deserialize, Serialize};

use chrono_tz::Tz;

use crate::warehouse::SinkWarehouse;
use lithify_core::artifact::ArtifactName;
use lithify_core::ts::{Event, Ts};

use crate::artifact::{Declaration, Declarations};
use crate::place::{Places, Placing};
use crate::schedule::Schedule;
use crate::stepping::SeriesLog;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(bound(deserialize = "C: Deserialize<'de>, W: Deserialize<'de>"))]
pub(crate) struct Wiring<C, W> {
    #[serde(default)]
    time_zone: Option<Tz>,
    #[serde(default)]
    default: Defaults<C, W>,
    #[serde(default, rename = "source")]
    sources: Vec<WiredSource<C>>,
    #[serde(default, rename = "sink")]
    sinks: Vec<WiredSink<W>>,
}

impl<C, W> Wiring<C, W>
where
    C: Clone,
    W: Clone + Placing,
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

    pub(crate) fn sinks<A: Clone + Declaration>(
        &self,
        declarations: &Declarations<A>,
    ) -> Result<Vec<Sink<W, A>>, Miswiring> {
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
                    declarations,
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
        for (artifact, _) in places.iter() {
            Self::placed(&name, artifact, declarations, &places)?;
        }
        let artifacts = declarations.placed(&places);
        let (scheduled, schedule) = Self::scheduled(&name, &artifacts, &places, time_zone)?;

        Ok(Sink {
            change_log: warehouse.change_log(),
            artifacts,
            places,
            scheduled,
            schedule,
            warehouse,
            time_zone,
            name,
        })
    }

    fn scheduled<A: Declaration>(
        sink: &str,
        artifacts: &Declarations<A>,
        places: &Places<W::Location>,
        time_zone: Tz,
    ) -> Result<(SeriesLog, Schedule), Miswiring> {
        let mut scheduled = artifacts.iter().filter_map(|(name, declared)| {
            Some((
                Sink::<W, A>::logged(places, name, declared, time_zone)?,
                declared.schedule(time_zone)?,
            ))
        });
        let found = scheduled
            .next()
            .ok_or_else(|| Miswiring::Unscheduled(sink.to_owned()))?;

        match scheduled.next() {
            Some(_) => Err(Miswiring::ScheduledTwice(sink.to_owned())),
            None => Ok(found),
        }
    }

    fn placed<A: Declaration>(
        sink: &str,
        artifact: &ArtifactName,
        declarations: &Declarations<A>,
        places: &Places<W::Location>,
    ) -> Result<(), Miswiring> {
        let declared = declarations
            .get(artifact)
            .ok_or_else(|| Miswiring::Undeclared(sink.to_owned(), artifact.clone()))?;
        match declared.base() {
            Some(base) if places.get(base).is_none() => Err(Miswiring::BaseUnplaced {
                sink: sink.to_owned(),
                artifact: artifact.clone(),
                base: base.clone(),
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
    Undeclared(String, ArtifactName),
    BaseUnplaced {
        sink: String,
        artifact: ArtifactName,
        base: ArtifactName,
    },
    Unscheduled(String),
    ScheduledTwice(String),
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
                "{sink} places {artifact}, which artifact.toml declares nowhere"
            ),
            Self::BaseUnplaced {
                sink,
                artifact,
                base,
            } => write!(
                formatter,
                "{sink} places {artifact} without {base} it is built from"
            ),
            Self::Unscheduled(sink) => {
                write!(formatter, "{sink} places nothing its days close by")
            }
            Self::ScheduledTwice(sink) => write!(
                formatter,
                "{sink} places more than one artifact its days close by"
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

impl<C: Serialize> Source<C> {
    pub fn written_capture(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.capture)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sink<W: SinkWarehouse, A> {
    name: String,
    change_log: W::Location,
    warehouse: W,
    artifacts: Declarations<A>,
    places: Places<W::Location>,
    scheduled: SeriesLog,
    schedule: Schedule,
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

    pub fn artifact(&self, artifact: &ArtifactName) -> Option<&A> {
        self.artifacts.get(artifact)
    }

    pub fn artifacts(&self) -> impl Iterator<Item = (&ArtifactName, &A)> {
        self.artifacts.iter()
    }

    pub fn place(&self, artifact: &ArtifactName) -> Option<&W::Location> {
        self.places.get(artifact)
    }

    pub fn scheduled(&self) -> &SeriesLog {
        &self.scheduled
    }

    pub fn schedule(&self) -> Schedule {
        self.schedule
    }

    pub fn time_zone(&self) -> Tz {
        self.time_zone
    }
}

impl<W: SinkWarehouse, A: Declaration> Sink<W, A> {
    pub fn log(&self, artifact: &ArtifactName) -> Option<SeriesLog> {
        Self::logged(
            &self.places,
            artifact,
            self.artifact(artifact)?,
            self.time_zone,
        )
    }

    pub fn base(&self, artifact: &ArtifactName) -> Option<SeriesLog> {
        self.log(self.artifact(artifact)?.base()?)
    }

    fn logged(
        places: &Places<W::Location>,
        artifact: &ArtifactName,
        declared: &A,
        time_zone: Tz,
    ) -> Option<SeriesLog> {
        Some(SeriesLog::new(
            places.get(artifact)?.clone().into(),
            declared.series(artifact, time_zone),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::build_log::Series;
    use crate::place::Placed;
    use lithify_core::sink::Namespace;

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

        async fn with_client<F: crate::warehouse::WarehouseFn>(
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
    #[serde(deny_unknown_fields, tag = "kind", rename_all = "lowercase")]
    enum Made {
        Snapshot { shard: String },
        Copy { base: ArtifactName },
        History { shard: String },
    }

    impl Declaration for Made {
        fn kind(&self) -> &'static str {
            match self {
                Self::Snapshot { .. } => "snapshot",
                Self::Copy { .. } => "copy",
                Self::History { .. } => "history",
            }
        }

        fn built_from(&self) -> &'static str {
            match self {
                Self::Snapshot { .. } | Self::Copy { .. } => "snapshot",
                Self::History { .. } => "history",
            }
        }

        fn base(&self) -> Option<&ArtifactName> {
            match self {
                Self::Snapshot { .. } | Self::History { .. } => None,
                Self::Copy { base } => Some(base),
            }
        }

        fn series(&self, name: &ArtifactName, _: Tz) -> Series {
            match self {
                Self::Snapshot { .. } | Self::History { .. } => Series::new(name.clone(), "daily"),
                Self::Copy { .. } => Series::new(name.clone(), "copied"),
            }
        }

        fn schedule(&self, time_zone: Tz) -> Option<Schedule> {
            match self {
                Self::Snapshot { .. } => Some(Schedule::daily(chrono::NaiveTime::MIN, time_zone)),
                Self::Copy { .. } | Self::History { .. } => None,
            }
        }
    }

    fn declarations() -> Declarations<Made> {
        Declarations::try_from(
            toml::from_str::<crate::artifact::Declaring>(include_str!(
                "../testdata/infra/declarations.toml"
            ))
            .unwrap(),
        )
        .unwrap()
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

    const INFRA: &str = include_str!("../testdata/infra/infra.toml");

    const SERVER: &str = include_str!("../testdata/infra/server.toml");

    const WAREHOUSE: &str = include_str!("../testdata/infra/warehouse.toml");

    const NAMED: &str = include_str!("../testdata/infra/named.toml");

    const ARTIFACTS: &str = include_str!("../testdata/infra/artifacts.toml");

    fn wiring(written: &str) -> Wiring<Captured, Sunk> {
        toml::from_str(written).unwrap()
    }

    fn wired(server: &str, sink: &str) -> Wiring<Captured, Sunk> {
        wiring(&format!("{server}\n[[sink]]\n{NAMED}{sink}"))
    }

    fn sources() -> Vec<Source<Captured>> {
        wiring(INFRA).sources().unwrap()
    }

    fn sinks_of(wiring: &Wiring<Captured, Sunk>) -> Result<Vec<Sink<Sunk, Made>>, Miswiring> {
        wiring.sinks(&declarations())
    }

    fn sinks() -> Vec<Sink<Sunk, Made>> {
        sinks_of(&wiring(INFRA)).unwrap()
    }

    #[test]
    fn a_source_is_captured_as_the_dialect_it_names() {
        assert_eq!(
            sources()[1].capture(),
            &Captured::Debezium {
                dialect: "postgres".into()
            }
        );
    }

    #[test]
    fn a_source_that_names_no_capture_is_captured_like_the_others() {
        assert_eq!(
            sources()[0].capture(),
            &Captured::Debezium {
                dialect: "mysql".into()
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
            Miswiring::SourceNamedTwice("primary".into())
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

        assert_eq!(
            wiring.sinks(&declarations()).unwrap_err(),
            Miswiring::WithoutSinks
        );
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
            wiring(&zoned).sinks(&declarations()).unwrap()[0].time_zone(),
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

        assert!(toml::from_str::<Wiring<Captured, Sunk>>(&unknown).is_err());
    }

    #[test]
    fn a_sink_builds_each_artifact_declared_for_the_pipeline_where_its_warehouse_places_it() {
        let sinks = sinks();

        assert_eq!(
            sinks[0].artifact(&"snapshot".into()),
            Some(&Made::Snapshot {
                shard: "%Y%m%d".to_owned()
            })
        );
        assert_eq!(
            sinks[0].place(&"snapshot".into()),
            Some(&prefix("mysql_shop"))
        );
        assert_eq!(
            sinks[1].place(&"snapshot".into()),
            Some(&dataset("mysql_shop"))
        );
    }

    #[test]
    fn a_sink_places_each_artifact_by_its_name() {
        assert_eq!(
            sinks()[0].place(&"nine".into()),
            Some(&prefix("mysql_shop_0900"))
        );
    }

    #[test]
    fn a_sink_builds_the_artifacts_it_places_in_the_order_they_are_declared() {
        assert_eq!(
            sinks()[1]
                .artifacts()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["snapshot", "copy"]
        );
    }

    #[test]
    fn a_sink_builds_no_artifact_its_warehouse_places_nowhere() {
        let sinks = sinks();

        assert_eq!(sinks[0].artifact(&"copy".into()), None);
        assert_eq!(sinks[1].artifact(&"nine".into()), None);
    }

    #[test]
    fn an_artifact_placed_but_declared_nowhere_is_rejected() {
        let undeclared = INFRA.replace("place.copy", "place.history");

        assert_eq!(
            wiring(&undeclared).sinks(&declarations()).unwrap_err(),
            Miswiring::Undeclared("warehouse".into(), "history".into())
        );
    }

    #[test]
    fn an_artifact_placed_without_the_one_it_is_built_from_is_rejected() {
        let wiring = wired(
            SERVER,
            &format!("{WAREHOUSE}place.copy = {{ prefix = \"mysql_shop_copy\" }}\n"),
        );

        assert_eq!(
            wiring.sinks(&declarations()).unwrap_err(),
            Miswiring::BaseUnplaced {
                sink: "rustfs".into(),
                artifact: "copy".into(),
                base: "snapshot".into(),
            }
        );
    }

    #[test]
    fn a_place_written_the_way_of_another_warehouse_is_rejected() {
        let elsewhere = INFRA.replace(
            "place.copy = { dataset = \"mysql_shop_copy\" }",
            "place.copy = { prefix = \"mysql_shop_copy\" }",
        );

        assert!(toml::from_str::<Wiring<Captured, Sunk>>(&elsewhere).is_err());
    }

    #[test]
    fn a_source_that_says_since_when_nowhere_is_captured_since_the_epoch() {
        let wiring = wired(
            "[[source]]\nname = \"primary\"\ncapture.debezium.dialect = \"mysql\"",
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
            "[[source]]\nname = \"primary\"\nsince = \"1970-01-01T00:00:00Z\"",
            &format!("{WAREHOUSE}{ARTIFACTS}"),
        );

        assert_eq!(
            wiring.sources().unwrap_err(),
            Miswiring::SourceWithoutCapture("primary".into())
        );
    }

    #[test]
    fn two_servers_that_say_since_when_nowhere_take_over_together() {
        let sinceless = INFRA
            .replace("since = \"1970-01-01T00:00:00Z\"\n", "")
            .replace("since = \"2026-09-21T00:00:00Z\"\n", "");

        assert_eq!(
            wiring(&sinceless).sources().unwrap_err(),
            Miswiring::SourcesTakingOverTogether("primary".into(), "replica".into())
        );
    }

    #[test]
    fn two_servers_taking_over_together_are_asked_whether_one_forgot_since_when() {
        assert!(
            Miswiring::SourcesTakingOverTogether("primary".into(), "replica".into())
                .to_string()
                .contains("since")
        );
    }

    #[test]
    fn two_servers_taking_over_at_the_same_time_are_rejected() {
        let together = INFRA.replace("2026-09-21T00:00:00Z", "1970-01-01T00:00:00Z");

        assert_eq!(
            wiring(&together).sources().unwrap_err(),
            Miswiring::SourcesTakingOverTogether("primary".into(), "replica".into())
        );
    }

    #[test]
    fn a_sink_holding_no_change_log_is_rejected() {
        let unlogged = INFRA.replace("change_log = { prefix = \"mysql_shop_cdc\" }\n", "");

        assert!(toml::from_str::<Wiring<Captured, Sunk>>(&unlogged).is_err());
    }

    #[test]
    fn a_sink_into_no_warehouse_anywhere_is_rejected() {
        let wiring = wiring(&format!("{SERVER}\n[[sink]]\n{NAMED}"));

        assert_eq!(
            wiring.sinks(&declarations()).unwrap_err(),
            Miswiring::SinkWithoutWarehouse("rustfs".into())
        );
    }

    #[test]
    fn a_sink_into_the_warehouse_every_sink_is_sunk_into_by_default_places_what_it_places() {
        let wiring = wiring(&format!(
            "{SERVER}\n[default.sink.warehouse.files]\nbucket = \"lithify\"\nchange_log = {{ prefix = \"mysql_shop_cdc\" }}\n{ARTIFACTS}\n[[sink]]\n{NAMED}"
        ));

        assert_eq!(
            wiring.sinks(&declarations()).unwrap()[0].place(&"snapshot".into()),
            Some(&prefix("mysql_shop"))
        );
    }

    #[test]
    fn a_sink_advances_by_the_series_of_the_one_artifact_it_places_on_a_schedule() {
        assert_eq!(
            sinks()[1].scheduled(),
            &SeriesLog::new(
                Namespace::from("mysql_shop"),
                Series::new("snapshot".into(), "daily")
            )
        );
    }

    #[test]
    fn a_sink_closes_its_days_as_the_artifact_it_places_on_a_schedule_is_cut() {
        let zoned = format!("time_zone = \"Asia/Tokyo\"\n{INFRA}");

        assert_eq!(
            wiring(&zoned).sinks(&declarations()).unwrap()[0].schedule(),
            Schedule::daily(chrono::NaiveTime::MIN, chrono_tz::Asia::Tokyo)
        );
    }

    #[test]
    fn a_sink_logs_each_artifact_it_places_in_the_namespace_its_place_names() {
        assert_eq!(
            sinks()[1].log(&"copy".into()),
            Some(SeriesLog::new(
                Namespace::from("mysql_shop_copy"),
                Series::new("copy".into(), "copied")
            ))
        );
    }

    #[test]
    fn a_sink_builds_an_artifact_onto_the_series_its_base_advances_where_the_base_lies() {
        assert_eq!(
            sinks()[1].base(&"copy".into()),
            Some(SeriesLog::new(
                Namespace::from("mysql_shop"),
                Series::new("snapshot".into(), "daily")
            ))
        );
    }

    #[test]
    fn a_sink_builds_an_artifact_named_no_base_onto_nothing_it_logs() {
        assert_eq!(sinks()[1].base(&"snapshot".into()), None);
    }

    #[test]
    fn a_sink_lines_up_each_artifact_it_places_in_order_with_its_log_and_its_base() {
        let lineup = crate::lineup::Lineup::of(&sinks()[1], |name, _| {
            Ok::<_, std::convert::Infallible>(name.clone())
        })
        .unwrap();

        assert_eq!(
            lineup
                .members()
                .map(|member| (
                    member.declared().as_str(),
                    member.log().clone(),
                    member.base().cloned()
                ))
                .collect::<Vec<_>>(),
            [
                (
                    "snapshot",
                    sinks()[1].log(&"snapshot".into()).unwrap(),
                    None
                ),
                (
                    "copy",
                    sinks()[1].log(&"copy".into()).unwrap(),
                    sinks()[1].log(&"snapshot".into())
                ),
            ]
        );
    }

    #[test]
    fn a_sink_placing_nothing_on_a_schedule_is_rejected() {
        let wiring = wired(
            SERVER,
            &format!("{WAREHOUSE}place.versions = {{ prefix = \"mysql_shop_versions\" }}\n"),
        );

        assert_eq!(
            wiring.sinks(&declarations()).unwrap_err(),
            Miswiring::Unscheduled("rustfs".into())
        );
    }

    #[test]
    fn a_sink_placing_two_artifacts_on_a_schedule_is_rejected() {
        let twice = INFRA.replace(
            "place.nine = { prefix = \"mysql_shop_0900\" }",
            "place.weekly = { prefix = \"mysql_shop_weekly\" }",
        );
        let declarations = Declarations::<Made>::try_from(
            toml::from_str::<crate::artifact::Declaring>(&format!(
                "{}\n[[artifact]]\nname = \"weekly\"\nkind = \"snapshot\"\nshard = \"%Y%m%d\"\n",
                include_str!("../testdata/infra/declarations.toml")
            ))
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            wiring(&twice).sinks(&declarations).unwrap_err(),
            Miswiring::ScheduledTwice("rustfs".into())
        );
    }

    #[test]
    fn a_sink_that_places_nothing_is_rejected() {
        let wiring = wired(SERVER, WAREHOUSE);

        assert_eq!(
            wiring.sinks(&declarations()).unwrap_err(),
            Miswiring::SinkWithoutArtifacts("rustfs".into())
        );
    }

    #[test]
    fn a_sink_named_twice_is_rejected() {
        let sink = format!("{NAMED}{WAREHOUSE}{ARTIFACTS}");
        let wiring = wiring(&format!("{SERVER}\n[[sink]]\n{sink}\n[[sink]]\n{sink}"));

        assert_eq!(
            wiring.sinks(&declarations()).unwrap_err(),
            Miswiring::SinkNamedTwice("rustfs".into())
        );
    }

    #[test]
    fn a_sink_without_a_name_is_rejected() {
        let wiring = wiring(&format!("{SERVER}\n[[sink]]\n{WAREHOUSE}{ARTIFACTS}"));

        assert_eq!(
            wiring.sinks(&declarations()).unwrap_err(),
            Miswiring::SinkWithoutName(0)
        );
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

        assert!(toml::from_str::<Wiring<Captured, Sunk>>(&unknown).is_err());
    }
}
