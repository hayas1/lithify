use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use lithify_core::build_log::{BuildLog, Outcome, Run, Status};
use lithify_core::model::TableId;
use lithify_core::sink::{Namespace, SinkWarehouse, TablePath, WarehouseFn};
use lithify_core::testing::{Engine, Recorder};
use lithify_core::ts::Ts;
use lithify_core::window::Window;

use crate::pipelines::Pipelines;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Captured {
    Debezium { dialect: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kept {
    Memory {},
    Logged {},
    Unreachable { cause: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shelf {
    prefix: String,
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
            BuildLog::new(
                &Run::new(
                    Ulid::from_parts(1, 0),
                    DateTime::UNIX_EPOCH,
                    "snapshot",
                    "daily",
                    "primary",
                    "{}",
                    "0.1.0",
                ),
                Some(TableId::new("shop", table)),
                Some(TableId::new("shop", table)),
                base.map(|base| TablePath::new("mysql_shop", base)),
                Some(TablePath::new(
                    "mysql_shop",
                    &format!("{table}_{}", at(opened).format("%Y%m%d")),
                )),
                Window::new(Ts::from(at(opened)), Ts::from(at(closed))),
                Outcome::new(status, at(closed)),
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

    fn with_client<F: WarehouseFn>(&self, _: &Shelf, _: &(), f: F) -> Result<F::Output, Unreached> {
        match self {
            Self::Memory {} => Ok(f.call(&Recorder::new(Engine::default()))),
            Self::Logged {} => Ok(f.call(
                &Recorder::new(Engine::default())
                    .logging(&Namespace::from("mysql_shop"), &Self::logs()),
            )),
            Self::Unreachable { cause } => Err(Unreached(cause.clone())),
        }
    }
}

pub const INFRA: &str = r#"
    [default]
    source.capture.debezium.dialect = "mysql"
    sink.change_log.prefix = "mysql_shop_cdc"
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

pub fn logged() -> String {
    INFRA.replace("warehouse.memory = {}", "warehouse.logged = {}")
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
        directory.join("table.toml"),
        "[default]\nartifacts = [\"snapshot\"]\n",
    )
    .unwrap();
}

pub fn under(root: &Path) -> Pipelines<Captured, Kept> {
    Pipelines::new(root.to_owned(), ())
}
