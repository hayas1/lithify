use std::error::Error;

use chrono::NaiveDate;
use ulid::Ulid;

use lithify_build::artifact::ArtifactName;
use lithify_cli::pipeline::WarehouseConfig;
use lithify_pipeline::Pipeline;
use lithify_pipeline::build_log::{BuildLog, BuildLogQuery};
use lithify_pipeline::check::{Expectation, Inspecting, Inspection};
use lithify_pipeline::infra::Sink;
use lithify_pipeline::runner::BuildLogWrite;
use lithify_pipeline::stepping::{Stepping, Target};
use lithify_pipeline::warehouse::WarehouseControl;
use lithify_protocol::DuckDbWarehouse;
use lithify_schema::ts::{Replay, Ts};

use crate::pipeline::{ArtifactConfig, CaptureConfig};
use crate::replica::{Replica, Row};
use crate::shards::Shards;

pub struct Deployment {
    pipeline: Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
    shards: Shards,
}

impl Deployment {
    const INFRA: &str = include_str!("../testdata/deployment/infra.toml");
    const ARTIFACTS: &str = include_str!("../testdata/deployment/artifact.toml");
    const TABLES: &str = include_str!("../testdata/deployment/table.toml");

    pub async fn over(replica: &Replica) -> Self {
        let pipeline = Self::opened();
        let warehouse = DuckDbWarehouse::new(Self::sink(&pipeline).change_log().clone());
        let shards = Shards::landed(replica, warehouse).await;

        Self { pipeline, shards }
    }

    pub async fn catch_up(
        &self,
        target: &Target,
        to: Option<NaiveDate>,
        now: Ts<Replay>,
    ) -> Result<Vec<BuildLog>, Box<dyn Error>> {
        let source = self.pipeline.sources().next().unwrap();
        let sink = Self::sink(&self.pipeline);
        let writer = BuildLogWrite::of(source, now, env!("CARGO_PKG_VERSION"))?;
        let stepping = Stepping::<_, _, Box<dyn Error>>::new(
            self.shards.client(),
            source,
            Expectation::of(sink, source, now)?,
            self.pipeline.tables(),
        );
        let stepping = match to {
            Some(date) => stepping.up_to(date, sink.scheduled()).await?,
            None => stepping,
        };
        let mut built = Vec::new();

        stepping
            .catch_up(sink, target, &writer, |_, _, output| {
                built.extend(output?);
                Ok(())
            })
            .await?;

        Ok(built)
    }

    pub async fn build_log(&self, artifact: &ArtifactName) -> Vec<BuildLog> {
        let materialization = Self::sink(&self.pipeline)
            .materialization(artifact)
            .unwrap();
        let mut logged = self
            .shards
            .client()
            .build_logs::<Box<dyn Error>>(
                materialization.namespace(),
                materialization.series(),
                &BuildLogQuery::All,
            )
            .await
            .unwrap();
        logged.reverse();

        logged
    }

    pub async fn inspect(&self, now: Ts<Replay>) -> Inspection {
        let source = self.pipeline.sources().next().unwrap();

        Inspecting::of(Self::sink(&self.pipeline), source, now)
            .unwrap()
            .read::<_, Box<dyn Error>>(self.shards.client())
            .await
            .unwrap()
    }

    pub fn shard(&self, built: &BuildLog) -> Vec<Row> {
        self.shards.rows(built)
    }

    fn sink(
        pipeline: &Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
    ) -> &Sink<WarehouseConfig, ArtifactConfig> {
        pipeline.sinks().next().unwrap()
    }

    fn opened() -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        let directory =
            std::env::temp_dir().join(format!("lithify-deployment-{}", Ulid::generate()));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("infra.toml"), Self::INFRA).unwrap();
        std::fs::write(directory.join("artifact.toml"), Self::ARTIFACTS).unwrap();
        std::fs::write(directory.join("table.toml"), Self::TABLES).unwrap();
        let pipeline = Pipeline::open(&directory).unwrap();
        std::fs::remove_dir_all(&directory).unwrap();

        pipeline
    }
}
