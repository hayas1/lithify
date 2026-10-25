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

use lithify_config::{Declaration, Placed, Places, Placing};
use lithify_core::schedule::Schedule;

use crate::pipelines::Pipelines;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Captured {
    Debezium { dialect: String },
}

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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum Made {
    Snapshot {},
}

impl Declaration for Made {
    fn source(&self) -> Option<&'static str> {
        None
    }

    fn schedule(&self, time_zone: chrono_tz::Tz) -> Option<Schedule> {
        Some(Schedule::daily(chrono::NaiveTime::MIN, time_zone))
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
            BuildLog::new(
                &Run::new(
                    Ulid::from_parts(1, 0),
                    Ts::from(DateTime::UNIX_EPOCH),
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
                Outcome::new(status, Ts::from(at(closed))),
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
            Self::Memory(_) => Ok(f.call(&Recorder::new(Engine::default()))),
            Self::Logged(_) => Ok(f.call(
                &Recorder::new(Engine::default())
                    .logging(&Namespace::from("mysql_shop"), &Self::logs()),
            )),
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

pub const INFRA: &str = r#"
    [artifact.snapshot]

    [default]
    source.capture.debezium.dialect = "mysql"

    [[source]]
    name = "primary"

    [[sink]]
    name = "rustfs"

    [sink.warehouse.memory]
    change_log = { prefix = "mysql_shop_cdc" }
    place.snapshot = [{ prefix = "mysql_shop" }]

    [[sink]]
    name = "warehouse"

    [sink.warehouse.unreachable]
    cause = "JobCreationMode"
    change_log = { prefix = "mysql_shop_cdc" }
    place.snapshot = [{ prefix = "mysql_shop" }]
"#;

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
        directory.join("table.toml"),
        "[default]\nartifacts = [\"snapshot\"]\n",
    )
    .unwrap();
}

pub fn under(root: &Path) -> Pipelines<Captured, Kept, Made> {
    Pipelines::new(root.to_owned(), ())
}
