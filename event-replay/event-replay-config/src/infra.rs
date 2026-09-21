use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Utc};
use semigroup::Semigroup;
use serde::{Deserialize, Serialize};

use event_replay_core::position::RecordedAt;
use event_replay_core::sink::TablePath;

use crate::Artifact;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Wiring {
    #[serde(default)]
    default: Defaults,
    #[serde(default, rename = "source")]
    sources: Vec<WiredSource>,
    #[serde(default, rename = "sink")]
    sinks: Vec<WiredSink>,
}

impl Wiring {
    pub(crate) fn sources(&self) -> Result<Vec<Source>, Miswiring> {
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

    fn distinct(sources: &[Source]) -> Result<(), Miswiring> {
        let mut named = BTreeSet::new();

        for source in sources {
            if !named.insert(source.name()) {
                return Err(Miswiring::SourceNamedTwice(source.name().to_owned()));
            }
        }

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

    pub(crate) fn sinks(&self) -> Result<Vec<Sink>, Miswiring> {
        if self.sinks.is_empty() {
            return Err(Miswiring::WithoutSinks);
        }

        let sinks = self
            .sinks
            .iter()
            .enumerate()
            .map(|(at, wired)| wired.clone().sink(at, &self.default.sink))
            .collect::<Result<Vec<_>, _>>()?;

        match sinks
            .iter()
            .enumerate()
            .find(|(at, sink)| sinks[..*at].contains(sink))
        {
            Some((at, _)) => Err(Miswiring::SinkConfiguredTwice(at)),
            None => Ok(sinks),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Defaults {
    #[serde(default)]
    source: WiredSource,
    #[serde(default)]
    sink: WiredSink,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize, Semigroup)]
#[semigroup(with = "semigroup::op::Coalesce")]
#[serde(deny_unknown_fields)]
struct WiredSource {
    name: Option<String>,
    since: Option<DateTime<Utc>>,
    capture: Option<Capture>,
    schema_changes: Option<String>,
}

impl WiredSource {
    fn source(self, default: &Self) -> Result<Source, Miswiring> {
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize, Semigroup)]
#[semigroup(with = "semigroup::op::Coalesce")]
#[serde(deny_unknown_fields)]
struct WiredSink {
    change_log: Option<String>,
    warehouse: Option<Warehouse>,
    replay: Option<Replay>,
}

impl WiredSink {
    fn sink(self, at: usize, default: &Self) -> Result<Sink, Miswiring> {
        let wired = self.semigroup(default.clone());

        Ok(Sink {
            change_log: wired
                .change_log
                .ok_or(Miswiring::SinkWithoutChangeLog(at))?,
            warehouse: wired.warehouse.ok_or(Miswiring::SinkWithoutWarehouse(at))?,
            replay: wired.replay.ok_or(Miswiring::SinkWithoutReplay(at))?,
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
    SinkWithoutChangeLog(usize),
    SinkWithoutWarehouse(usize),
    SinkWithoutReplay(usize),
    SinkConfiguredTwice(usize),
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
            Self::SinkWithoutChangeLog(at) => {
                write!(formatter, "sink {} holds no change log", at + 1)
            }
            Self::SinkWithoutWarehouse(at) => {
                write!(formatter, "sink {} names no warehouse", at + 1)
            }
            Self::SinkWithoutReplay(at) => write!(formatter, "sink {} builds nothing", at + 1),
            Self::SinkConfiguredTwice(at) => {
                write!(formatter, "sink {} repeats another sink", at + 1)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum Capture {
    Debezium { dialect: Dialect },
}

impl Capture {
    pub fn dialect(&self) -> Dialect {
        match self {
            Self::Debezium { dialect } => *dialect,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    MySql,
    Postgres,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    name: String,
    since: DateTime<Utc>,
    capture: Capture,
    schema_changes: Option<String>,
}

impl Source {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn capture(&self) -> &Capture {
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
pub struct Sink {
    change_log: String,
    warehouse: Warehouse,
    replay: Replay,
}

impl Sink {
    pub fn change_log(&self) -> &str {
        &self.change_log
    }

    pub fn schema_changes(&self, source: &Source) -> Option<TablePath> {
        source
            .schema_changes()
            .map(|table| TablePath::new(&self.change_log, table))
    }

    pub fn warehouse(&self) -> &Warehouse {
        &self.warehouse
    }

    pub fn replay(&self) -> &Replay {
        &self.replay
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Replay {
    snapshot: String,
    history: String,
    shard: String,
}

impl Replay {
    pub fn namespace(&self, artifact: Artifact) -> &str {
        match artifact {
            Artifact::Snapshot => &self.snapshot,
            Artifact::History => &self.history,
        }
    }

    pub fn shard(&self) -> &str {
        &self.shard
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum Warehouse {
    DuckDb(Store),
    BigQuery(BigQuery),
}

impl Warehouse {
    pub fn as_duck_db(&self) -> Option<&Store> {
        match self {
            Self::DuckDb(duckdb) => Some(duckdb),
            Self::BigQuery(_) => None,
        }
    }

    pub fn as_big_query(&self) -> Option<&BigQuery> {
        match self {
            Self::BigQuery(bigquery) => Some(bigquery),
            Self::DuckDb(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum Store {
    S3(S3),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct S3 {
    endpoint: String,
    region: String,
    bucket: String,
    format: Format,
}

impl S3 {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn format(&self) -> Format {
        self.format
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Parquet,
}

impl Format {
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BigQuery {
    project: String,
    endpoint: Option<String>,
}

impl BigQuery {
    pub fn project(&self) -> &str {
        &self.project
    }

    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        [source.capture.debezium]
        dialect = "postgres"

        [[sink]]
        warehouse.duckdb.s3.endpoint = "http://minio:9000"
        warehouse.duckdb.s3.region = "us-east-1"
        warehouse.duckdb.s3.bucket = "event-replay"
        warehouse.duckdb.s3.format = "parquet"

        [[sink]]
        change_log = "mysql_shop_archive"
        warehouse.bigquery.project = "local-project"
        warehouse.bigquery.endpoint = "http://bigquery-emulator:9050"
    "#;

    const SERVER: &str = r#"
        [[source]]
        name = "primary"
        since = "1970-01-01T00:00:00Z"

        [source.capture.debezium]
        dialect = "mysql"
    "#;

    const WAREHOUSE: &str = r#"
        warehouse.duckdb.s3.endpoint = "http://minio:9000"
        warehouse.duckdb.s3.region = "us-east-1"
        warehouse.duckdb.s3.bucket = "event-replay"
        warehouse.duckdb.s3.format = "parquet"
    "#;

    const REPLAY: &str = r#"
        replay.snapshot = "mysql_shop"
        replay.history = "mysql_shop_history"
        replay.shard = "%Y%m%d"
    "#;

    fn wiring(written: &str) -> Wiring {
        toml::from_str(written).unwrap()
    }

    fn wired(server: &str, sink: &str) -> Wiring {
        wiring(&format!("{server}\n[[sink]]\n{sink}"))
    }

    fn sources() -> Vec<Source> {
        wiring(INFRA).sources().unwrap()
    }

    fn sinks() -> Vec<Sink> {
        wiring(INFRA).sinks().unwrap()
    }

    #[test]
    fn a_source_is_captured_as_the_dialect_it_names() {
        assert_eq!(sources()[1].capture().dialect(), Dialect::Postgres);
    }

    #[test]
    fn a_source_that_names_no_capture_is_captured_like_the_others() {
        assert_eq!(sources()[0].capture().dialect(), Dialect::MySql);
    }

    #[test]
    fn the_servers_of_a_host_are_read_in_the_order_they_took_over() {
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

        assert!(toml::from_str::<Wiring>(&unknown).is_err());
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
    fn a_host_captured_from_no_server_is_rejected() {
        let wiring = wiring(
            "[[sink]]\nchange_log = \"mysql_shop_cdc\"\nwarehouse.duckdb.s3.endpoint = \"http://minio:9000\"\nwarehouse.duckdb.s3.region = \"us-east-1\"\nwarehouse.duckdb.s3.bucket = \"event-replay\"\nwarehouse.duckdb.s3.format = \"parquet\"\nreplay.snapshot = \"mysql_shop\"\nreplay.history = \"mysql_shop_history\"\nreplay.shard = \"%Y%m%d\"",
        );

        assert_eq!(wiring.sources().unwrap_err(), Miswiring::WithoutSources);
    }

    #[test]
    fn a_host_sunk_nowhere_is_rejected() {
        let wiring = wiring(
            "[[source]]\nname = \"primary\"\nsince = \"1970-01-01T00:00:00Z\"\n\n[source.capture.debezium]\ndialect = \"mysql\"",
        );

        assert_eq!(wiring.sinks().unwrap_err(), Miswiring::WithoutSinks);
    }

    #[test]
    fn a_sink_holds_the_change_log_the_host_names() {
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
    fn a_source_captured_without_a_schema_change_topic_lands_no_schema_changes() {
        assert_eq!(sinks()[0].schema_changes(&sources()[1]), None);
    }

    #[test]
    fn a_sink_builds_each_artifact_in_the_namespace_the_host_names() {
        let replay = sinks()[1].replay().clone();

        assert_eq!(replay.namespace(Artifact::Snapshot), "mysql_shop");
        assert_eq!(replay.namespace(Artifact::History), "mysql_shop_history");
        assert_eq!(replay.shard(), "%Y%m%d");
    }

    #[test]
    fn a_source_that_says_since_when_nowhere_is_rejected() {
        let wiring = wired(
            "[[source]]\nname = \"primary\"\n\n[source.capture.debezium]\ndialect = \"mysql\"",
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
            Miswiring::SinkWithoutChangeLog(0)
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
            Miswiring::SinkWithoutWarehouse(0)
        );
    }

    #[test]
    fn a_sink_that_builds_nothing_anywhere_is_rejected() {
        let wiring = wired(
            SERVER,
            &format!("change_log = \"mysql_shop_cdc\"\n{WAREHOUSE}"),
        );

        assert_eq!(wiring.sinks().unwrap_err(), Miswiring::SinkWithoutReplay(0));
    }

    #[test]
    fn a_sink_repeated_as_it_stands_is_rejected() {
        let sink = format!("change_log = \"mysql_shop_cdc\"\n{WAREHOUSE}{REPLAY}");
        let wiring = wiring(&format!("{SERVER}\n[[sink]]\n{sink}\n[[sink]]\n{sink}"));

        assert_eq!(
            wiring.sinks().unwrap_err(),
            Miswiring::SinkConfiguredTwice(1)
        );
    }

    #[test]
    fn a_warehouse_reading_object_storage_says_where_the_change_log_is_written() {
        let Store::S3(s3) = sinks()[0].warehouse().as_duck_db().unwrap().clone();

        assert!(sinks()[0].warehouse().as_big_query().is_none());
        assert_eq!(s3.endpoint(), "http://minio:9000");
        assert_eq!(s3.region(), "us-east-1");
        assert_eq!(s3.bucket(), "event-replay");
        assert_eq!(s3.format(), Format::Parquet);
        assert_eq!(s3.format().extension(), "parquet");
    }

    #[test]
    fn a_change_log_written_in_an_unknown_format_is_rejected() {
        let unknown = INFRA.replace("format = \"parquet\"", "format = \"protobuf\"");

        assert!(toml::from_str::<Wiring>(&unknown).is_err());
    }

    #[test]
    fn a_change_log_written_into_an_unknown_store_is_rejected() {
        let unknown = INFRA.replace("warehouse.duckdb.s3", "warehouse.duckdb.gcs");

        assert!(toml::from_str::<Wiring>(&unknown).is_err());
    }

    #[test]
    fn a_bigquery_warehouse_says_which_project_holds_it() {
        let bigquery = sinks()[1].warehouse().as_big_query().unwrap().clone();

        assert!(sinks()[1].warehouse().as_duck_db().is_none());
        assert_eq!(bigquery.project(), "local-project");
        assert_eq!(bigquery.endpoint(), Some("http://bigquery-emulator:9050"));
    }

    #[test]
    fn a_capture_by_an_unknown_dialect_is_rejected() {
        let unknown = INFRA.replace("dialect = \"mysql\"", "dialect = \"oracle\"");

        assert!(toml::from_str::<Wiring>(&unknown).is_err());
    }

    #[test]
    fn a_sink_into_an_unknown_warehouse_is_rejected() {
        let unknown = INFRA.replace("warehouse.duckdb", "warehouse.redshift");

        assert!(toml::from_str::<Wiring>(&unknown).is_err());
    }
}
