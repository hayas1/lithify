use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::Serialize;
use serde::de::DeserializeOwned;

use lithify_config::infra::{Sink, Source};
use lithify_config::{ArtifactKind, Pipeline};
use lithify_core::check::{Expectation, Inspection};
use lithify_core::position::RecordedAt;
use lithify_core::sink::{SinkWarehouse, WarehouseClient, WarehouseFn};

use crate::overview::{
    PipelineLocation, PipelineOverview, SinkInspection, SinkOverview, SourceOverview,
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

    fn overview(&self, directory: &Path) -> PipelineOverview {
        let sources = Pipeline::<C, W>::open(directory)
            .map_err(|unreadable| unreadable.to_string())
            .and_then(|pipeline| {
                pipeline
                    .sources()
                    .map(|source| self.source(&pipeline, source))
                    .collect()
            });

        PipelineOverview::new(Self::location(directory), sources)
    }

    fn source(
        &self,
        pipeline: &Pipeline<C, W>,
        source: &Source<C>,
    ) -> Result<SourceOverview, String> {
        let capture =
            serde_json::to_string(source.capture()).map_err(|unwritten| unwritten.to_string())?;
        let now = RecordedAt::from(Utc::now());

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

                    SinkOverview::new(sink.name(), self.inspection(sink, &expectation))
                })
                .collect(),
        ))
    }

    fn inspection(
        &self,
        sink: &Sink<W>,
        expectation: &Expectation,
    ) -> Result<SinkInspection, String> {
        let reading = Reading {
            expectation,
            namespace: sink.artifacts().namespace(ArtifactKind::Snapshot),
        };

        sink.warehouse()
            .with_client(sink.change_log(), &self.credential, reading)
            .map_err(|unopened| unopened.to_string())?
            .map(SinkInspection::from)
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

    fn inspect(&self, name: &str) -> Result<Option<PipelineOverview>, String> {
        Ok(self
            .directories()?
            .iter()
            .find(|directory| Self::location(directory).name() == name)
            .map(|directory| self.overview(directory)))
    }
}

struct Reading<'a> {
    expectation: &'a Expectation,
    namespace: &'a str,
}

impl WarehouseFn for Reading<'_> {
    type Output = Result<Inspection, String>;

    fn call<K: WarehouseClient>(self, client: &K) -> Self::Output {
        self.expectation
            .read(client, self.namespace)
            .map_err(|unread| unread.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use serde::{Deserialize, Serialize};

    use lithify_core::sink::{SinkWarehouse, WarehouseFn};
    use lithify_core::testing::{Engine, Recorder};

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

        let pipeline = surveyed(&root).inspect("mysql").unwrap().unwrap();
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
    fn a_pipeline_that_could_not_be_opened_is_surveyed_beside_the_ones_that_were() {
        let root = scratch("unopened");
        configure(&root.join("mysql"), INFRA);
        configure(&root.join("postgres"), "[[source]]\nname = \"primary\"");

        let surveyed = surveyed(&root);

        assert!(
            surveyed
                .inspect("mysql")
                .unwrap()
                .unwrap()
                .sources()
                .is_ok()
        );
        assert!(
            surveyed
                .inspect("postgres")
                .unwrap()
                .unwrap()
                .sources()
                .is_err()
        );
        assert_eq!(surveyed.inspect("oracle").unwrap(), None);
    }

    #[test]
    fn a_directory_configuring_no_pipeline_is_not_surveyed() {
        assert!(surveyed(&scratch("empty")).pipelines().is_err());
    }
}
