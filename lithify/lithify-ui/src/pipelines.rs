use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::Serialize;
use serde::de::DeserializeOwned;

use lithify_config::infra::{Sink, Source};
use lithify_config::{ArtifactKind, Pipeline};
use lithify_core::build_log::BuildLogQuery;
use lithify_core::check::Expectation;
use lithify_core::sink::{SinkWarehouse, WarehouseClient, WarehouseFn};
use lithify_core::ts::Ts;

use crate::overview::{
    BuildLogView, PipelineLocation, PipelineOverview, SinkInspection, SinkOverview, SourceOverview,
};
use crate::server::Survey;

pub struct Pipelines<C, W: SinkWarehouse> {
    root: PathBuf,
    credential: W::Credential,
    capture: std::marker::PhantomData<fn() -> C>,
}

impl<C, W> Pipelines<C, W>
where
    C: Clone + DeserializeOwned + Serialize,
    W: Clone + DeserializeOwned + SinkWarehouse,
{
    pub fn new(root: PathBuf, credential: W::Credential) -> Self {
        Self {
            root,
            credential,
            capture: std::marker::PhantomData,
        }
    }

    fn directories(&self) -> Result<Vec<PathBuf>, String> {
        let directories = Pipeline::<C, W>::directories(&self.root)
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
        let sources = Pipeline::<C, W>::open(directory)
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
        pipeline: &Pipeline<C, W>,
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
                    let expectation = Expectation::new(
                        sink.artifacts().schedule(),
                        source.since(),
                        &capture,
                        now,
                    );

                    SinkOverview::new(sink.name(), self.inspection(sink, &expectation, view))
                })
                .collect(),
        ))
    }

    fn inspection(
        &self,
        sink: &Sink<W>,
        expectation: &Expectation,
        view: BuildLogView,
    ) -> Result<SinkInspection, String> {
        let reading = Reading {
            expectation,
            namespace: sink.artifacts().namespace(ArtifactKind::Snapshot),
            view,
        };

        sink.warehouse()
            .with_client(sink.change_log(), &self.credential, reading)
            .map_err(|unopened| unopened.to_string())?
    }
}

impl<C, W> Survey for Pipelines<C, W>
where
    C: Clone + DeserializeOwned + Serialize + 'static,
    W: Clone + DeserializeOwned + SinkWarehouse + Send + Sync + 'static,
    W::Credential: Send + Sync,
{
    fn pipelines(&self) -> Result<Vec<PipelineLocation>, String> {
        Ok(self
            .directories()?
            .iter()
            .map(|directory| Self::location(directory))
            .collect())
    }

    fn inspect(&self, name: &str, view: BuildLogView) -> Result<Option<PipelineOverview>, String> {
        Ok(self
            .directories()?
            .iter()
            .find(|directory| Self::location(directory).name() == name)
            .map(|directory| self.overview(directory, view)))
    }
}

struct Reading<'a> {
    expectation: &'a Expectation,
    namespace: &'a str,
    view: BuildLogView,
}

impl Reading<'_> {
    fn read<K: WarehouseClient>(&self, client: &K) -> Result<SinkInspection, K::Error> {
        let inspection = self.expectation.read(client, self.namespace)?;
        let unbuilt = inspection.latest().is_empty();
        let inspection = SinkInspection::from(inspection);

        match (self.view, unbuilt) {
            (BuildLogView::Latest, _) | (BuildLogView::All, true) => Ok(inspection),
            (BuildLogView::All, false) => Ok(inspection
                .showing(client.build_logs(self.namespace, &BuildLogQuery::from(self.view))?)),
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
    use std::path::{Path, PathBuf};

    use serde::{Deserialize, Serialize};

    use chrono::DateTime;
    use lithify_core::sink::{SinkWarehouse, WarehouseFn};
    use ulid::Ulid;

    use lithify_core::build_log::{BuildLog, Outcome, Run, Status};
    use lithify_core::model::TableId;
    use lithify_core::sink::TablePath;
    use lithify_core::testing::{Engine, Recorder};
    use lithify_core::window::Window;

    use super::*;
    use crate::server::Survey;

    #[derive(Debug, Clone, Deserialize, Serialize)]
    #[serde(rename_all = "lowercase")]
    enum Captured {
        Debezium { dialect: String },
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "lowercase")]
    enum Kept {
        Memory {},
        Logged {},
        Unreachable { cause: String },
    }

    #[derive(Debug)]
    struct Unreached(String);

    impl std::fmt::Display for Unreached {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str(&self.0)
        }
    }

    impl std::error::Error for Unreached {}

    impl Kept {
        fn logs() -> Vec<BuildLog> {
            let built = |table: &str, from: i64| {
                BuildLog::new(
                    &Run::new(
                        Ulid::nil(),
                        DateTime::UNIX_EPOCH,
                        "snapshot",
                        "daily",
                        "primary",
                        "{}",
                        "0.1.0",
                    ),
                    Some(TableId::new("shop", table)),
                    Some(TableId::new("shop", table)),
                    None,
                    Some(TablePath::new("mysql_shop", &format!("{table}_{from}"))),
                    Window::new(
                        Ts::try_from(from).unwrap(),
                        Ts::try_from(from + 86_400_000).unwrap(),
                    ),
                    Outcome::new(
                        Status::Succeeded,
                        DateTime::from_timestamp_millis(from).unwrap(),
                    ),
                )
            };

            vec![
                built("people", 0),
                built("people", 86_400_000),
                built("pets", 86_400_000),
            ]
        }
    }

    impl SinkWarehouse for Kept {
        type Credential = ();
        type Error = Unreached;

        fn with_client<F: WarehouseFn>(
            &self,
            _: &str,
            _: &(),
            f: F,
        ) -> Result<F::Output, Unreached> {
            match self {
                Self::Memory {} => Ok(f.call(&Recorder::new(Engine::default()))),
                Self::Logged {} => Ok(
                    f.call(&Recorder::new(Engine::default()).logging("mysql_shop", &Self::logs()))
                ),
                Self::Unreachable { cause } => Err(Unreached(cause.clone())),
            }
        }
    }

    const INFRA: &str = r#"
        [default]
        source.capture.debezium.dialect = "mysql"
        sink.change_log = "mysql_shop_cdc"
        sink.artifacts.snapshot = "mysql_shop"
        sink.artifacts.history = "mysql_shop_history"
        sink.artifacts.shard = "%Y%m%d"

        [[source]]
        name = "primary"

        [[sink]]
        name = "rustfs"
        warehouse.memory = {}

        [[sink]]
        name = "warehouse"
        warehouse.unreachable.cause = "JobCreationMode"
    "#;

    fn scratch(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("lithify-ui-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        root
    }

    fn configure(directory: &Path, infra: &str) {
        std::fs::create_dir_all(directory).unwrap();
        std::fs::write(directory.join("infra.toml"), infra).unwrap();
        std::fs::write(
            directory.join("table.toml"),
            "[default]\nartifacts = [\"snapshot\"]\n",
        )
        .unwrap();
    }

    fn surveyed(root: &Path) -> Pipelines<Captured, Kept> {
        Pipelines::new(root.to_owned(), ())
    }

    #[test]
    fn the_pipelines_under_a_directory_are_surveyed_in_the_order_of_their_names() {
        let root = scratch("ordered");
        configure(&root.join("postgres"), INFRA);
        configure(&root.join("mysql"), INFRA);

        assert_eq!(
            surveyed(&root).pipelines().unwrap(),
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

        let pipeline = surveyed(&root)
            .inspect("mysql", BuildLogView::Latest)
            .unwrap()
            .unwrap();
        let sources = pipeline.sources().unwrap();
        let sinks = sources[0].sinks();

        assert_eq!(sources[0].name(), "primary");
        assert!(sinks[0].found().unwrap().rows().is_empty());
        assert_eq!(
            sinks[0].found().unwrap().discrepancies(),
            ["nothing is built since 1970-01-01T00:00:00+00:00"]
        );
        assert_eq!(sinks[1].found(), Err("JobCreationMode"));
    }

    #[test]
    fn a_sink_shows_its_latest_build_of_each_table_or_every_build_newest_first() {
        let root = scratch("viewed");
        configure(
            &root.join("mysql"),
            &INFRA.replace("warehouse.memory = {}", "warehouse.logged = {}"),
        );
        let tables = |view| {
            surveyed(&root)
                .inspect("mysql", view)
                .unwrap()
                .unwrap()
                .sources()
                .unwrap()[0]
                .sinks()[0]
                .found()
                .unwrap()
                .rows()
                .iter()
                .map(|row| row.table.table().to_owned())
                .collect::<Vec<_>>()
        };

        assert_eq!(tables(BuildLogView::Latest), ["people", "people", "pets"]);
        assert_eq!(tables(BuildLogView::All), ["pets", "people", "people"]);
    }

    #[test]
    fn a_pipeline_that_could_not_be_opened_is_surveyed_beside_the_ones_that_were() {
        let root = scratch("unopened");
        configure(&root.join("mysql"), INFRA);
        configure(&root.join("postgres"), "[[source]]\nname = \"primary\"");

        let surveyed = surveyed(&root);

        assert!(
            surveyed
                .inspect("mysql", BuildLogView::Latest)
                .unwrap()
                .unwrap()
                .sources()
                .is_ok()
        );
        assert!(
            surveyed
                .inspect("postgres", BuildLogView::Latest)
                .unwrap()
                .unwrap()
                .sources()
                .is_err()
        );
        assert_eq!(
            surveyed.inspect("oracle", BuildLogView::Latest).unwrap(),
            None
        );
    }

    #[test]
    fn a_directory_configuring_no_pipeline_is_not_surveyed() {
        assert!(surveyed(&scratch("empty")).pipelines().is_err());
    }
}
