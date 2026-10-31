use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use lithify_core::ts::{Replay, Ts};
use lithify_pipeline::build_log::{BuildLog, BuildLogQuery, Latest};
use lithify_pipeline::check::{Inspecting, Inspection};
use lithify_pipeline::infra::{Sink, Source};
use lithify_pipeline::pipelines::{PipelineLocation, Pipelines};
use lithify_pipeline::schedule::Schedule;
use lithify_pipeline::warehouse::{SinkWarehouse, WarehouseControl, WarehouseFn};
use lithify_pipeline::{Declaration, Pipeline, Placing};

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

pub struct Survey<C, W: SinkWarehouse, A> {
    pipelines: Pipelines<C, W, A>,
    credential: W::Credential,
}

impl<C, W, A> Survey<C, W, A>
where
    C: Clone + DeserializeOwned + Serialize,
    W: Clone + DeserializeOwned + Placing,
    A: Clone + DeserializeOwned + Declaration,
{
    pub fn new(root: PathBuf, credential: W::Credential) -> Self {
        Self {
            pipelines: Pipelines::new(root),
            credential,
        }
    }

    pub fn locations(&self) -> Result<Vec<PipelineLocation>, String> {
        self.pipelines
            .locations()
            .map_err(|unlisted| unlisted.to_string())
    }

    pub async fn inspect(
        &self,
        name: &str,
        view: BuildLogView,
    ) -> Result<Option<PipelineOverview>, String> {
        let located = self
            .pipelines
            .located(name)
            .map_err(|unlisted| unlisted.to_string())?;

        Ok(match located {
            Some(location) => Some(self.overview(location, view).await),
            None => None,
        })
    }

    async fn overview(&self, location: PipelineLocation, view: BuildLogView) -> PipelineOverview {
        let sources = match self.pipelines.open(&location) {
            Ok(pipeline) => self.sources(&pipeline, view).await,
            Err(unreadable) => Err(unreadable.to_string()),
        };

        PipelineOverview::new(location, sources)
    }

    async fn sources(
        &self,
        pipeline: &Pipeline<C, W, A>,
        view: BuildLogView,
    ) -> Result<Vec<SourceOverview>, String> {
        let mut sources = Vec::new();

        for source in pipeline.sources() {
            sources.push(self.source(pipeline, source, view).await?);
        }

        Ok(sources)
    }

    async fn source(
        &self,
        pipeline: &Pipeline<C, W, A>,
        source: &Source<C>,
        view: BuildLogView,
    ) -> Result<SourceOverview, String> {
        let now = Ts::now();
        let mut sinks = Vec::new();

        for sink in pipeline.sinks() {
            sinks.push(SinkOverview::new(
                sink.name(),
                self.inspection(sink, source, now, view).await,
            ));
        }

        Ok(SourceOverview::new(source.name(), sinks))
    }

    async fn inspection(
        &self,
        sink: &Sink<W, A>,
        source: &Source<C>,
        now: Ts<Replay>,
        view: BuildLogView,
    ) -> Result<SinkInspection, String> {
        let reading = Reading {
            inspecting: Inspecting::of(sink, source, now)
                .map_err(|uninspectable| uninspectable.to_string())?,
            view,
        };

        sink.warehouse()
            .with_client(sink.change_log(), &self.credential, reading)
            .await
            .map_err(|unopened| unopened.to_string())?
    }
}

struct Reading {
    inspecting: Inspecting,
    view: BuildLogView,
}

impl Reading {
    async fn read<K: WarehouseControl>(&self, client: &K) -> Result<SinkInspection, Misread> {
        let inspection = self.inspecting.read::<K, Misread>(client).await?;
        let unbuilt = inspection.latest().is_empty();
        let inspection = SinkInspection::from(inspection);

        match (self.view, unbuilt) {
            (BuildLogView::Latest, _) | (BuildLogView::All, true) => Ok(inspection),
            (BuildLogView::All, false) => Ok(inspection.showing(
                client
                    .build_logs::<Misread>(
                        self.inspecting.namespace(),
                        self.inspecting.series(),
                        &BuildLogQuery::from(self.view),
                    )
                    .await?,
            )),
        }
    }
}

impl WarehouseFn for Reading {
    type Output = Result<SinkInspection, String>;

    async fn call<K: WarehouseControl>(self, client: &K) -> Self::Output {
        self.read(client).await.map_err(|Misread(unread)| unread)
    }
}

struct Misread(String);

impl<E: std::error::Error> From<E> for Misread {
    fn from(cause: E) -> Self {
        Self(cause.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use lithify_testing::pipeline::{Captured, INFRA, Kept, Made, configure, logged, scratch};

    fn under(root: &Path) -> Survey<Captured, Kept, Made> {
        Survey::new(root.to_owned(), ())
    }

    async fn tables(root: &Path, view: BuildLogView) -> Vec<String> {
        under(root)
            .inspect("mysql", view)
            .await
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

    #[tokio::test]
    async fn each_sink_is_inspected_on_its_own_so_one_unreached_leaves_the_others_read() {
        let root = scratch("inspected");
        configure(&root.join("mysql"), INFRA);

        let pipeline = under(&root)
            .inspect("mysql", BuildLogView::Latest)
            .await
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

    #[tokio::test]
    async fn a_sink_shows_its_latest_build_of_each_table_or_every_build_newest_first() {
        let root = scratch("viewed");
        configure(&root.join("mysql"), &logged());

        assert_eq!(
            tables(&root, BuildLogView::Latest).await,
            ["people", "people", "pets"]
        );
        assert_eq!(
            tables(&root, BuildLogView::All).await,
            ["pets", "people", "people"]
        );
    }

    #[tokio::test]
    async fn a_pipeline_that_could_not_be_opened_is_inspected_beside_the_ones_that_were() {
        let root = scratch("unopened");
        configure(&root.join("mysql"), INFRA);
        configure(&root.join("postgres"), "[[source]]\nname = \"primary\"");

        let pipelines = under(&root);

        assert!(
            pipelines
                .inspect("mysql", BuildLogView::Latest)
                .await
                .unwrap()
                .unwrap()
                .sources()
                .is_ok()
        );
        assert!(
            pipelines
                .inspect("postgres", BuildLogView::Latest)
                .await
                .unwrap()
                .unwrap()
                .sources()
                .is_err()
        );
        assert_eq!(
            pipelines
                .inspect("oracle", BuildLogView::Latest)
                .await
                .unwrap(),
            None
        );
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
