use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::Deserialize;

use lithify_core::model::TableId;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pipeline: PipelineLocation,
    #[serde(default, rename = "source")]
    sources: BTreeMap<String, SourceConnection>,
    #[serde(default, rename = "sink")]
    sinks: BTreeMap<String, SinkConnection>,
    #[serde(default)]
    setup: Actions,
    #[serde(default)]
    cleanup: Actions,
    #[serde(default, rename = "step")]
    steps: Vec<Step>,
}

impl FromStr for Scenario {
    type Err = Unwritable;

    fn from_str(written: &str) -> Result<Self, Self::Err> {
        let scenario: Self = toml::from_str(written)?;

        scenario.setup.addressed(&scenario)?;
        scenario.cleanup.addressed(&scenario)?;

        for step in &scenario.steps {
            step.addressed(&scenario)?;
        }

        Ok(scenario)
    }
}

impl Scenario {
    pub fn pipeline(&self) -> &Path {
        &self.pipeline.path
    }

    pub fn sources(&self) -> impl Iterator<Item = (&str, &SourceConnection)> {
        self.sources
            .iter()
            .map(|(name, connection)| (name.as_str(), connection))
    }

    pub fn sinks(&self) -> impl Iterator<Item = (&str, &SinkConnection)> {
        self.sinks
            .iter()
            .map(|(name, connection)| (name.as_str(), connection))
    }

    pub fn setup(&self) -> &Actions {
        &self.setup
    }

    pub fn cleanup(&self) -> &Actions {
        &self.cleanup
    }

    pub fn steps(&self) -> impl Iterator<Item = &Step> {
        self.steps.iter()
    }

    fn connects_source(&self, name: &str) -> Result<(), Unwritable> {
        match self.sources.contains_key(name) {
            true => Ok(()),
            false => Err(Unwritable::UnconnectedSource(name.to_owned())),
        }
    }

    fn connects_sink(&self, name: &str) -> Result<(), Unwritable> {
        match self.sinks.contains_key(name) {
            true => Ok(()),
            false => Err(Unwritable::UnconnectedSink(name.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct PipelineLocation {
    path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConnection {
    url: String,
}

impl SourceConnection {
    pub fn url(&self) -> &str {
        &self.url
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkConnection {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Actions {
    #[serde(default)]
    source: BTreeMap<String, Action>,
    #[serde(default)]
    sink: BTreeMap<String, Action>,
}

impl Actions {
    pub fn source(&self) -> impl Iterator<Item = (&str, &Action)> {
        self.source
            .iter()
            .map(|(name, action)| (name.as_str(), action))
    }

    pub fn sink(&self) -> impl Iterator<Item = (&str, &Action)> {
        self.sink
            .iter()
            .map(|(name, action)| (name.as_str(), action))
    }

    fn addressed(&self, scenario: &Scenario) -> Result<(), Unwritable> {
        for name in self.source.keys() {
            scenario.connects_source(name)?;
        }

        for name in self.sink.keys() {
            scenario.connects_sink(name)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Action {
    #[serde(default)]
    query: Vec<String>,
}

impl Action {
    pub fn queries(&self) -> impl Iterator<Item = &str> {
        self.query.iter().map(String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    name: String,
    #[serde(flatten)]
    actions: Actions,
    #[serde(default, rename = "build")]
    builds: Vec<Build>,
    #[serde(default, rename = "assert")]
    assertions: Vec<Assertion>,
}

impl Step {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn actions(&self) -> &Actions {
        &self.actions
    }

    pub fn builds(&self) -> impl Iterator<Item = &Build> {
        self.builds.iter()
    }

    pub fn assertions(&self) -> impl Iterator<Item = &Assertion> {
        self.assertions.iter()
    }

    fn addressed(&self, scenario: &Scenario) -> Result<(), Unwritable> {
        self.actions.addressed(scenario)?;
        self.builds
            .iter()
            .try_for_each(|build| build.addressed(scenario))?;
        self.assertions
            .iter()
            .try_for_each(|assertion| assertion.addressed(scenario))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub enum Build {
    Snapshot(SnapshotBuild),
}

impl Build {
    fn addressed(&self, scenario: &Scenario) -> Result<(), Unwritable> {
        match self {
            Self::Snapshot(snapshot) => {
                scenario.connects_sink(&snapshot.sink)?;
                scenario.connects_source(&snapshot.source)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotBuild {
    sink: String,
    source: String,
    table: Table,
}

impl SnapshotBuild {
    pub fn sink(&self) -> &str {
        &self.sink
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn table(&self) -> &TableId {
        &self.table.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub enum Assertion {
    Snapshot(SnapshotAssertion),
}

impl Assertion {
    fn addressed(&self, scenario: &Scenario) -> Result<(), Unwritable> {
        match self {
            Self::Snapshot(snapshot) => {
                scenario.connects_sink(&snapshot.sink)?;
                scenario.connects_source(&snapshot.source)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotAssertion {
    sink: String,
    source: String,
    table: Table,
}

impl SnapshotAssertion {
    pub fn sink(&self) -> &str {
        &self.sink
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn table(&self) -> &TableId {
        &self.table.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
struct Table(TableId);

impl TryFrom<String> for Table {
    type Error = Unwritable;

    fn try_from(table: String) -> Result<Self, Self::Error> {
        TableId::try_from(table.as_str())
            .map(Self)
            .map_err(|_| Unwritable::UnknownTable(table))
    }
}

#[derive(Debug)]
pub enum Unwritable {
    Malformed(toml::de::Error),
    UnknownTable(String),
    UnconnectedSource(String),
    UnconnectedSink(String),
}

impl Display for Unwritable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(cause) => write!(formatter, "{cause}"),
            Self::UnknownTable(table) => {
                write!(formatter, "{table} does not name a schema and a table")
            }
            Self::UnconnectedSource(name) => {
                write!(
                    formatter,
                    "{name} is acted on, a source the scenario does not connect"
                )
            }
            Self::UnconnectedSink(name) => {
                write!(
                    formatter,
                    "{name} is acted on, a sink the scenario does not connect"
                )
            }
        }
    }
}

impl std::error::Error for Unwritable {}

impl From<toml::de::Error> for Unwritable {
    fn from(cause: toml::de::Error) -> Self {
        Self::Malformed(cause)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCENARIO: &str = r#"
        [pipeline]
        path = "../compose/lithify/mysql"

        [source.primary]
        url = "mysql://root:root@mysql:3306/shop"

        [sink.minio]

        [sink.warehouse]

        [setup]
        source.primary.query = ["CREATE TABLE IF NOT EXISTS shop.people (id BIGINT PRIMARY KEY)"]

        [cleanup]
        source.primary.query = ["DROP TABLE IF EXISTS shop.people"]
        sink.minio.query = ["DROP TABLE IF EXISTS mysql_shop.people"]
        sink.warehouse.query = ["DROP TABLE IF EXISTS `mysql_shop.people`"]

        [[step]]
        name = "rows the scenario inserts"
        source.primary.query = ["INSERT INTO shop.people VALUES (1)"]

        [[step]]
        name = "the snapshot of the rows"
        build = [{ snapshot = { sink = "minio", source = "primary", table = "shop.people" } }]
        assert = [{ snapshot = { sink = "minio", source = "primary", table = "shop.people" } }]
    "#;

    fn scenario() -> Scenario {
        SCENARIO.parse().unwrap()
    }

    fn step(at: usize) -> Step {
        scenario().steps().nth(at).unwrap().clone()
    }

    #[test]
    fn a_scenario_connects_the_sources_and_sinks_of_a_pipeline_by_their_names() {
        let scenario = scenario();

        assert_eq!(scenario.pipeline(), Path::new("../compose/lithify/mysql"));
        assert_eq!(
            scenario.sources().map(|(name, _)| name).collect::<Vec<_>>(),
            ["primary"]
        );
        assert_eq!(
            scenario.sinks().map(|(name, _)| name).collect::<Vec<_>>(),
            ["minio", "warehouse"]
        );
    }

    #[test]
    fn a_source_is_reached_at_the_url_the_scenario_names() {
        let scenario = scenario();
        let (_, primary) = scenario.sources().next().unwrap();

        assert_eq!(primary.url(), "mysql://root:root@mysql:3306/shop");
    }

    #[test]
    fn queries_are_addressed_to_the_source_or_sink_they_are_written_for() {
        let scenario = scenario();

        assert_eq!(
            scenario
                .cleanup()
                .source()
                .flat_map(|(name, action)| action.queries().map(move |query| (name, query)))
                .collect::<Vec<_>>(),
            [("primary", "DROP TABLE IF EXISTS shop.people")]
        );
        assert_eq!(
            scenario
                .cleanup()
                .sink()
                .flat_map(|(name, action)| action.queries().map(move |query| (name, query)))
                .collect::<Vec<_>>(),
            [
                ("minio", "DROP TABLE IF EXISTS mysql_shop.people"),
                ("warehouse", "DROP TABLE IF EXISTS `mysql_shop.people`")
            ]
        );
    }

    #[test]
    fn a_step_builds_nothing_and_asserts_nothing_unless_it_says_so() {
        let step = step(0);

        assert_eq!(step.name(), "rows the scenario inserts");
        assert!(step.builds().next().is_none());
        assert!(step.assertions().next().is_none());
    }

    #[test]
    fn a_step_builds_a_snapshot_in_a_sink_from_the_changes_of_a_source() {
        let step = step(1);
        let Some(Build::Snapshot(snapshot)) = step.builds().next() else {
            panic!("the step builds no snapshot");
        };

        assert_eq!(snapshot.sink(), "minio");
        assert_eq!(snapshot.source(), "primary");
        assert_eq!(snapshot.table(), &TableId::new("shop", "people"));
    }

    #[test]
    fn a_step_asserts_that_the_snapshot_in_a_sink_holds_what_a_source_holds() {
        let step = step(1);
        let Some(Assertion::Snapshot(snapshot)) = step.assertions().next() else {
            panic!("the step asserts nothing of a snapshot");
        };

        assert_eq!(snapshot.sink(), "minio");
        assert_eq!(snapshot.source(), "primary");
        assert_eq!(snapshot.table(), &TableId::new("shop", "people"));
    }

    #[test]
    fn acting_on_a_source_the_scenario_does_not_connect_is_rejected() {
        let written = SCENARIO.replace(
            "source.primary.query = [\"INSERT",
            "source.replica.query = [\"INSERT",
        );

        assert!(matches!(
            written.parse::<Scenario>().unwrap_err(),
            Unwritable::UnconnectedSource(name) if name == "replica"
        ));
    }

    #[test]
    fn building_in_a_sink_the_scenario_does_not_connect_is_rejected() {
        let written = SCENARIO.replace(
            "build = [{ snapshot = { sink = \"minio\"",
            "build = [{ snapshot = { sink = \"lake\"",
        );

        assert!(matches!(
            written.parse::<Scenario>().unwrap_err(),
            Unwritable::UnconnectedSink(name) if name == "lake"
        ));
    }

    #[test]
    fn building_from_a_source_the_scenario_does_not_connect_is_rejected() {
        let written = SCENARIO.replace(
            "sink = \"minio\", source = \"primary\", table = \"shop.people\" } }]\n        assert",
            "sink = \"minio\", source = \"replica\", table = \"shop.people\" } }]\n        assert",
        );

        assert!(matches!(
            written.parse::<Scenario>().unwrap_err(),
            Unwritable::UnconnectedSource(name) if name == "replica"
        ));
    }

    #[test]
    fn asserting_on_a_sink_the_scenario_does_not_connect_is_rejected() {
        let written = SCENARIO.replace(
            "assert = [{ snapshot = { sink = \"minio\"",
            "assert = [{ snapshot = { sink = \"lake\"",
        );

        assert!(matches!(
            written.parse::<Scenario>().unwrap_err(),
            Unwritable::UnconnectedSink(name) if name == "lake"
        ));
    }

    #[test]
    fn a_table_that_names_no_schema_is_rejected() {
        let written = SCENARIO.replace(
            "{ sink = \"minio\", source = \"primary\", table = \"shop.people\" }",
            "{ sink = \"minio\", source = \"primary\", table = \"people\" }",
        );

        assert!(matches!(
            written.parse::<Scenario>().unwrap_err(),
            Unwritable::Malformed(cause) if cause.message() == "people does not name a schema and a table"
        ));
    }
}
