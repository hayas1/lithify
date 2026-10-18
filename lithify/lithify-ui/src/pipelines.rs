use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use lithify_config::infra::{Sink, Source};
use lithify_config::{Declaration, Pipeline};
use lithify_core::build_log::{BuildLog, BuildLogQuery, Latest};
use lithify_core::check::{Expectation, Inspection};
use lithify_core::schedule::Schedule;
use lithify_core::sink::{Namespace, SinkWarehouse, WarehouseClient, WarehouseFn};
use lithify_core::ts::Ts;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineLocation {
    name: String,
    directory: String,
}

impl PipelineLocation {
    pub fn new(name: &str, directory: &str) -> Self {
        Self {
            name: name.to_owned(),
            directory: directory.to_owned(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn directory(&self) -> &str {
        &self.directory
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineOverview {
    location: PipelineLocation,
    sources: Result<Vec<SourceOverview>, String>,
}

impl PipelineOverview {
    pub fn new(location: PipelineLocation, sources: Result<Vec<SourceOverview>, String>) -> Self {
        Self { location, sources }
    }

    pub fn location(&self) -> &PipelineLocation {
        &self.location
    }

    pub fn sources(&self) -> Result<&[SourceOverview], &str> {
        self.sources.as_deref().map_err(String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceOverview {
    name: String,
    sinks: Vec<SinkOverview>,
}

impl SourceOverview {
    pub fn new(name: &str, sinks: Vec<SinkOverview>) -> Self {
        Self {
            name: name.to_owned(),
            sinks,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sinks(&self) -> &[SinkOverview] {
        &self.sinks
    }

    pub fn shown(&self, sink_name: Option<&str>) -> Option<(usize, &SinkOverview)> {
        self.sinks
            .iter()
            .enumerate()
            .find(|(_, sink)| Some(sink.name()) == sink_name)
            .or_else(|| self.sinks.first().map(|sink| (0, sink)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkOverview {
    name: String,
    found: Result<SinkInspection, String>,
}

impl SinkOverview {
    pub fn new(name: &str, found: Result<SinkInspection, String>) -> Self {
        Self {
            name: name.to_owned(),
            found,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn found(&self) -> Result<&SinkInspection, &str> {
        self.found.as_ref().map_err(String::as_str)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BuildLogView {
    #[default]
    Latest,
    All,
}

impl BuildLogView {
    pub const ALL: [Self; 2] = [Self::Latest, Self::All];

    pub fn name(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::All => "all",
        }
    }
}

impl From<BuildLogView> for BuildLogQuery {
    fn from(view: BuildLogView) -> Self {
        match view {
            BuildLogView::Latest => Latest::Attempted.into(),
            BuildLogView::All => Self::All,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkInspection {
    schedule: Schedule,
    logs: Vec<BuildLog>,
    discrepancies: Vec<String>,
}

impl SinkInspection {
    pub fn new(schedule: Schedule, logs: Vec<BuildLog>, discrepancies: Vec<String>) -> Self {
        Self {
            schedule,
            logs,
            discrepancies,
        }
    }

    pub fn schedule(&self) -> Schedule {
        self.schedule
    }

    pub fn logs(&self) -> &[BuildLog] {
        &self.logs
    }

    pub fn showing(self, logs: Vec<BuildLog>) -> Self {
        Self { logs, ..self }
    }

    pub fn discrepancies(&self) -> &[String] {
        &self.discrepancies
    }
}

impl From<Inspection> for SinkInspection {
    fn from(inspection: Inspection) -> Self {
        let mut latest = inspection.latest().to_vec();
        latest.sort_by(|left, right| left.table().cmp(&right.table()));

        Self::new(
            inspection.schedule(),
            latest,
            inspection
                .discrepancies()
                .iter()
                .map(ToString::to_string)
                .collect(),
        )
    }
}

pub struct Pipelines<C, W: SinkWarehouse, A> {
    root: PathBuf,
    credential: W::Credential,
    configured: std::marker::PhantomData<fn() -> (C, A)>,
}

impl<C, W, A> Pipelines<C, W, A>
where
    C: Clone + DeserializeOwned + Serialize,
    W: Clone + DeserializeOwned + SinkWarehouse<Location: DeserializeOwned>,
    A: Clone + DeserializeOwned + Declaration<Location = W::Location>,
{
    const SNAPSHOT: &str = "snapshot";

    pub fn new(root: PathBuf, credential: W::Credential) -> Self {
        Self {
            root,
            credential,
            configured: std::marker::PhantomData,
        }
    }

    pub fn locations(&self) -> Result<Vec<PipelineLocation>, String> {
        Ok(self
            .directories()?
            .iter()
            .map(|directory| Self::location(directory))
            .collect())
    }

    pub fn inspect(
        &self,
        name: &str,
        view: BuildLogView,
    ) -> Result<Option<PipelineOverview>, String> {
        Ok(self
            .directories()?
            .iter()
            .find(|directory| Self::location(directory).name() == name)
            .map(|directory| self.overview(directory, view)))
    }

    fn directories(&self) -> Result<Vec<PathBuf>, String> {
        let directories = Pipeline::<C, W, A>::directories(&self.root)
            .map_err(|unreadable| unreadable.to_string())?;

        match directories.is_empty() {
            true => Err(format!("{} configures no pipeline", self.root.display())),
            false => Ok(directories),
        }
    }

    fn location(directory: &Path) -> PipelineLocation {
        let name = directory
            .file_name()
            .unwrap_or(directory.as_os_str())
            .to_string_lossy();

        PipelineLocation::new(&name, &directory.display().to_string())
    }

    fn overview(&self, directory: &Path, view: BuildLogView) -> PipelineOverview {
        let sources = Pipeline::<C, W, A>::open(directory)
            .map_err(|unreadable| unreadable.to_string())
            .and_then(|pipeline| {
                pipeline
                    .sources()
                    .map(|source| self.source(&pipeline, source, view))
                    .collect()
            });

        PipelineOverview::new(Self::location(directory), sources)
    }

    fn source(
        &self,
        pipeline: &Pipeline<C, W, A>,
        source: &Source<C>,
        view: BuildLogView,
    ) -> Result<SourceOverview, String> {
        let capture =
            serde_json::to_string(source.capture()).map_err(|unwritten| unwritten.to_string())?;
        let now = Ts::from(Utc::now());

        Ok(SourceOverview::new(
            source.name(),
            pipeline
                .sinks()
                .map(|sink| {
                    let expectation =
                        Expectation::new(Schedule::Daily, source.since(), &capture, now);

                    SinkOverview::new(sink.name(), self.inspection(sink, &expectation, view))
                })
                .collect(),
        ))
    }

    fn inspection(
        &self,
        sink: &Sink<W, A>,
        expectation: &Expectation,
        view: BuildLogView,
    ) -> Result<SinkInspection, String> {
        let reading = Reading {
            expectation,
            namespace: sink
                .artifact(Self::SNAPSHOT)
                .and_then(|declared| declared.places().next())
                .map(|place| place.clone().into())
                .ok_or_else(|| format!("{} declares no snapshot placed anywhere", sink.name()))?,
            view,
        };

        sink.warehouse()
            .with_client(sink.change_log(), &self.credential, reading)
            .map_err(|unopened| unopened.to_string())?
    }
}

struct Reading<'a> {
    expectation: &'a Expectation,
    namespace: Namespace,
    view: BuildLogView,
}

impl Reading<'_> {
    fn read<K: WarehouseClient>(&self, client: &K) -> Result<SinkInspection, K::Error> {
        let inspection = self.expectation.read(client, &self.namespace)?;
        let unbuilt = inspection.latest().is_empty();
        let inspection = SinkInspection::from(inspection);

        match (self.view, unbuilt) {
            (BuildLogView::Latest, _) | (BuildLogView::All, true) => Ok(inspection),
            (BuildLogView::All, false) => Ok(inspection
                .showing(client.build_logs(&self.namespace, &BuildLogQuery::from(self.view))?)),
        }
    }
}

impl WarehouseFn for Reading<'_> {
    type Output = Result<SinkInspection, String>;

    fn call<K: WarehouseClient>(self, client: &K) -> Self::Output {
        self.read(client).map_err(|unread| unread.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{INFRA, configure, logged, scratch, under};

    fn tables(root: &Path, view: BuildLogView) -> Vec<String> {
        under(root)
            .inspect("mysql", view)
            .unwrap()
            .unwrap()
            .sources()
            .unwrap()[0]
            .sinks()[0]
            .found()
            .unwrap()
            .logs()
            .iter()
            .filter_map(BuildLog::table)
            .map(|table| table.table().to_owned())
            .collect()
    }

    #[test]
    fn the_pipelines_under_a_directory_are_located_in_the_order_of_their_names() {
        let root = scratch("ordered");
        configure(&root.join("postgres"), INFRA);
        configure(&root.join("mysql"), INFRA);

        assert_eq!(
            under(&root).locations().unwrap(),
            [
                PipelineLocation::new("mysql", &root.join("mysql").display().to_string()),
                PipelineLocation::new("postgres", &root.join("postgres").display().to_string())
            ]
        );
    }

    #[test]
    fn each_sink_is_inspected_on_its_own_so_one_unreached_leaves_the_others_read() {
        let root = scratch("inspected");
        configure(&root.join("mysql"), INFRA);

        let pipeline = under(&root)
            .inspect("mysql", BuildLogView::Latest)
            .unwrap()
            .unwrap();
        let sources = pipeline.sources().unwrap();
        let sinks = sources[0].sinks();

        assert_eq!(sources[0].name(), "primary");
        assert!(sinks[0].found().unwrap().logs().is_empty());
        assert_eq!(
            sinks[0].found().unwrap().discrepancies(),
            ["nothing is built since 1970-01-01T00:00:00+00:00"]
        );
        assert_eq!(sinks[1].found(), Err("JobCreationMode"));
    }

    #[test]
    fn a_sink_shows_its_latest_build_of_each_table_or_every_build_newest_first() {
        let root = scratch("viewed");
        configure(&root.join("mysql"), &logged());

        assert_eq!(
            tables(&root, BuildLogView::Latest),
            ["people", "people", "pets"]
        );
        assert_eq!(
            tables(&root, BuildLogView::All),
            ["pets", "people", "people"]
        );
    }

    #[test]
    fn a_pipeline_that_could_not_be_opened_is_inspected_beside_the_ones_that_were() {
        let root = scratch("unopened");
        configure(&root.join("mysql"), INFRA);
        configure(&root.join("postgres"), "[[source]]\nname = \"primary\"");

        let pipelines = under(&root);

        assert!(
            pipelines
                .inspect("mysql", BuildLogView::Latest)
                .unwrap()
                .unwrap()
                .sources()
                .is_ok()
        );
        assert!(
            pipelines
                .inspect("postgres", BuildLogView::Latest)
                .unwrap()
                .unwrap()
                .sources()
                .is_err()
        );
        assert_eq!(
            pipelines.inspect("oracle", BuildLogView::Latest).unwrap(),
            None
        );
    }

    #[test]
    fn a_directory_configuring_no_pipeline_locates_none() {
        assert!(under(&scratch("empty")).locations().is_err());
    }

    #[test]
    fn a_source_shows_the_sink_named_or_else_its_first() {
        let source = SourceOverview::new(
            "primary",
            vec![
                SinkOverview::new("rustfs", Err("unread".to_owned())),
                SinkOverview::new("warehouse", Err("unread".to_owned())),
            ],
        );
        let shown = |sink_name| source.shown(sink_name).map(|(at, sink)| (at, sink.name()));

        assert_eq!(shown(Some("warehouse")), Some((1, "warehouse")));
        assert_eq!(shown(Some("lake")), Some((0, "rustfs")));
        assert_eq!(shown(None), Some((0, "rustfs")));
        assert!(SourceOverview::new("primary", vec![]).shown(None).is_none());
    }
}
