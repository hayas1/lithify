use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use ulid::Ulid;

use lithify_build::sink::{Namespace, TablePath};
use lithify_pipeline::build_log::{BuildLog, Run, Series, Status, TableBuilt};
use lithify_pipeline::warehouse::{SinkWarehouse, WarehouseFn};
use lithify_pipeline::{Placed, Places, Placing};
use lithify_schema::model::TableId;
use lithify_schema::ts::Ts;
use lithify_schema::window::Window;

use crate::{Engine, Recorder};

pub use lithify_cli::artifacts::ArtifactConfig;
pub use lithify_cli::pipeline::CaptureConfig;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Memory {}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cause {
    cause: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kept {
    Memory(Placed<Memory, Shelf>),
    Logged(Placed<Memory, Shelf>),
    Unreachable(Placed<Cause, Shelf>),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shelf {
    prefix: String,
}

impl From<Shelf> for Namespace {
    fn from(shelf: Shelf) -> Self {
        Namespace::from(shelf.prefix.as_str())
    }
}

#[derive(Debug)]
pub struct Unreached(String);

impl std::fmt::Display for Unreached {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Unreached {}

impl Kept {
    pub fn logs() -> Vec<BuildLog> {
        let at = |rfc3339: &str| rfc3339.parse::<DateTime<Utc>>().unwrap();
        let built = |table: &str, opened: &str, closed: &str, base: Option<&str>, status| {
            TableBuilt::new(
                Some(TableId::new("shop", table)),
                Some(TableId::new("shop", table)),
                base.map(|base| TablePath::new("mysql_shop", base)),
                Some(TablePath::new(
                    "mysql_shop",
                    &format!("{table}_{}", at(opened).format("%Y%m%d")),
                )),
                Window::new(Ts::from(at(opened)), Ts::from(at(closed))),
                status,
                Ts::from(at(closed)),
            )
            .logged(
                &Run::new(
                    Ulid::from_parts(1, 0),
                    Ts::from(DateTime::UNIX_EPOCH),
                    "primary",
                    "{}",
                    "0.1.0",
                ),
                &Series::new("snapshot".into(), "daily"),
            )
        };

        vec![
            built(
                "people",
                "2026-09-19T00:00:00Z",
                "2026-09-20T00:00:00Z",
                None,
                Status::Succeeded,
            ),
            built(
                "people",
                "2026-09-20T00:00:00Z",
                "2026-09-21T00:00:00Z",
                Some("people_20260919"),
                Status::Succeeded,
            ),
            built(
                "pets",
                "2026-09-20T00:00:00Z",
                "2026-09-21T00:00:00Z",
                None,
                Status::Failed("no such column".to_owned()),
            ),
        ]
    }
}

impl SinkWarehouse for Kept {
    type Location = Shelf;
    type Credential = ();
    type Error = Unreached;

    fn reaches(&self, _: &Shelf) -> bool {
        true
    }

    async fn with_client<F: WarehouseFn>(
        &self,
        _: &Shelf,
        _: &(),
        f: F,
    ) -> Result<F::Output, Unreached> {
        match self {
            Self::Memory(_) => Ok(f.call(&Recorder::new(Engine::default())).await),
            Self::Logged(_) => Ok(f
                .call(
                    &Recorder::new(Engine::default())
                        .logging(&Namespace::from("mysql_shop"), &Self::logs()),
                )
                .await),
            Self::Unreachable(placed) => Err(Unreached(placed.warehouse().cause.clone())),
        }
    }
}

impl Placing for Kept {
    fn change_log(&self) -> Shelf {
        match self {
            Self::Memory(placed) | Self::Logged(placed) => placed.change_log().clone(),
            Self::Unreachable(placed) => placed.change_log().clone(),
        }
    }

    fn places(&self) -> Places<Shelf> {
        match self {
            Self::Memory(placed) | Self::Logged(placed) => placed.places().clone(),
            Self::Unreachable(placed) => placed.places().clone(),
        }
    }
}

pub const INFRA: &str = include_str!("../testdata/pipeline/infra.toml");

pub fn logged() -> String {
    INFRA.replace("[sink.warehouse.memory]", "[sink.warehouse.logged]")
}

pub fn scratch(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("lithify-ui-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    root
}

pub fn configure(directory: &Path, infra: &str) {
    std::fs::create_dir_all(directory).unwrap();
    std::fs::write(directory.join("infra.toml"), infra).unwrap();
    std::fs::write(
        directory.join("artifact.toml"),
        "[[artifact]]\nname = \"snapshot\"\nkind = \"snapshot\"\nshard = \"%Y%m%d\"\n",
    )
    .unwrap();
    std::fs::write(
        directory.join("table.toml"),
        "[default]\nartifacts = [\"snapshot\"]\n",
    )
    .unwrap();
}
