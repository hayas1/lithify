use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

use lithify_core::sink::SinkWarehouse;

use crate::artifact::{Declaration, Declarations, Declaring};
use crate::infra::{self, Sink, Source};
use crate::place::Placing;
use crate::table::{self, Mislisting, Tables};
use crate::unreadable::Unreadable;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pipeline<C, W: SinkWarehouse, A> {
    name: String,
    sources: Vec<Source<C>>,
    sinks: Vec<Sink<W, A>>,
    tables: Tables,
}

impl<C, W, A> Pipeline<C, W, A>
where
    C: Clone + DeserializeOwned,
    W: Clone + DeserializeOwned + Placing,
    A: Clone + DeserializeOwned + Declaration,
{
    const WIRING: &str = "infra.toml";
    const DECLARING: &str = "artifact.toml";
    const LISTING: &str = "table.toml";

    pub fn open(directory: impl AsRef<Path>) -> Result<Self, Unreadable> {
        let directory = directory.as_ref();
        let wiring = directory.join(Self::WIRING);
        let declaring = directory.join(Self::DECLARING);
        let listing = directory.join(Self::LISTING);
        let written: infra::Wiring<C, W> = Self::read(&wiring)?;
        let miswired = |miswiring| Unreadable::at(&wiring, miswiring);
        let mislisted = |mislisting| Unreadable::at(&listing, mislisting);
        let declarations = Declarations::try_from(Self::read::<Declaring>(&declaring)?)
            .map_err(|misdeclaring| Unreadable::at(&declaring, misdeclaring))?;
        let sources = written.sources().map_err(miswired)?;
        let sinks = written.sinks(&declarations).map_err(miswired)?;
        let tables =
            Tables::try_from(Self::read::<table::Listing>(&listing)?).map_err(mislisted)?;
        Self::declared(&sinks, &tables).map_err(mislisted)?;

        Ok(Self {
            name: Self::named(directory),
            sources,
            sinks,
            tables,
        })
    }

    fn declared(sinks: &[Sink<W, A>], tables: &Tables) -> Result<(), Mislisting> {
        for (table, listed) in tables.listed() {
            for artifact in listed.artifacts() {
                let declared = sinks
                    .iter()
                    .find_map(|sink| sink.artifact(artifact))
                    .ok_or_else(|| Mislisting::Undeclared {
                        table: table.cloned(),
                        artifact: artifact.to_owned(),
                    })?;
                if let Some(base) = declared.base()
                    && !listed.builds(base)
                {
                    return Err(Mislisting::WithoutBase {
                        table: table.cloned(),
                        artifact: artifact.to_owned(),
                        base: base.to_owned(),
                    });
                }
            }
        }

        Ok(())
    }

    pub fn directories(root: impl AsRef<Path>) -> Result<Vec<PathBuf>, Unreadable> {
        let root = root.as_ref();

        if Self::configures(root) {
            return Ok(vec![root.to_owned()]);
        }

        let mut directories = std::fs::read_dir(root)
            .map_err(|unlisted| Unreadable::at(root, unlisted))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|unlisted| Unreadable::at(root, unlisted))?;
        directories.retain(|directory| Self::configures(directory));
        directories.sort();

        Ok(directories)
    }

    fn configures(directory: &Path) -> bool {
        directory.join(Self::WIRING).is_file()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sources(&self) -> impl Iterator<Item = &Source<C>> {
        self.sources.iter()
    }

    pub fn sinks(&self) -> impl Iterator<Item = &Sink<W, A>> {
        self.sinks.iter()
    }

    pub fn tables(&self) -> &Tables {
        &self.tables
    }

    fn named(directory: &Path) -> String {
        directory
            .file_name()
            .unwrap_or(directory.as_os_str())
            .to_string_lossy()
            .into_owned()
    }

    fn read<T: DeserializeOwned>(path: &Path) -> Result<T, Unreadable> {
        let written =
            std::fs::read_to_string(path).map_err(|missing| Unreadable::at(path, missing))?;

        toml::from_str(&written).map_err(|malformed| Unreadable::at(path, malformed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::Miswiring;
    use crate::place::{Placed, Places};
    use crate::table::Mask;
    use crate::unreadable::Cause;
    use lithify_core::artifact::ArtifactName;
    use lithify_core::model::TableId;
    use lithify_protocol::sink::bigquery::config as bigquery;
    use lithify_protocol::sink::duckdb::config as duckdb;
    use lithify_protocol::source::debezium::config as debezium;

    #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Sunk {
        DuckDb(Placed<duckdb::Config, duckdb::Location>),
        BigQuery(Placed<bigquery::Config, bigquery::Location>),
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Lying {
        DuckDb(duckdb::Location),
        BigQuery(bigquery::Location),
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
    #[serde(deny_unknown_fields, tag = "kind", rename_all = "lowercase")]
    enum Made {
        Snapshot {
            at: String,
            shard: String,
        },
        Instant {
            base: ArtifactName,
            at: String,
            shard: String,
        },
        Scd1 {
            base: ArtifactName,
        },
        Scd2 {
            shard: String,
        },
        Copy {
            base: ArtifactName,
        },
    }

    impl Declaration for Made {
        fn kind(&self) -> &'static str {
            match self {
                Self::Snapshot { .. } => "snapshot",
                Self::Instant { .. } => "instant",
                Self::Scd1 { .. } => "scd1",
                Self::Scd2 { .. } => "scd2",
                Self::Copy { .. } => "copy",
            }
        }

        fn built_from(&self) -> &'static str {
            match self {
                Self::Scd2 { .. } => "scd2",
                Self::Snapshot { .. }
                | Self::Instant { .. }
                | Self::Scd1 { .. }
                | Self::Copy { .. } => "snapshot",
            }
        }

        fn base(&self) -> Option<&ArtifactName> {
            match self {
                Self::Snapshot { .. } | Self::Scd2 { .. } => None,
                Self::Instant { base, .. } | Self::Scd1 { base } | Self::Copy { base } => {
                    Some(base)
                }
            }
        }
    }

    impl From<Lying> for lithify_core::sink::Namespace {
        fn from(location: Lying) -> Self {
            match location {
                Lying::DuckDb(location) => Self::from(location),
                Lying::BigQuery(location) => Self::from(location),
            }
        }
    }

    impl SinkWarehouse for Sunk {
        type Location = Lying;
        type Credential = ();
        type Error = std::fmt::Error;

        fn reaches(&self, location: &Lying) -> bool {
            matches!(
                (self, location),
                (Self::DuckDb(_), Lying::DuckDb(_)) | (Self::BigQuery(_), Lying::BigQuery(_))
            )
        }

        async fn with_client<F: lithify_core::sink::WarehouseFn>(
            &self,
            _: &Lying,
            _: &(),
            _: F,
        ) -> Result<F::Output, std::fmt::Error> {
            Err(std::fmt::Error)
        }
    }

    impl Placing for Sunk {
        fn change_log(&self) -> Lying {
            match self {
                Self::DuckDb(placed) => Lying::DuckDb(placed.change_log().clone()),
                Self::BigQuery(placed) => Lying::BigQuery(placed.change_log().clone()),
            }
        }

        fn places(&self) -> Places<Lying> {
            match self {
                Self::DuckDb(placed) => placed.places().clone().map(Lying::DuckDb),
                Self::BigQuery(placed) => placed.places().clone().map(Lying::BigQuery),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Capture {
        Debezium(debezium::Config),
    }

    const ARTIFACTS: &str = "[[artifact]]\nname = \"snapshot\"\nkind = \"snapshot\"\nat = \"00:00\"\nshard = \"%Y%m%d\"\n\n[[artifact]]\nname = \"copy\"\nkind = \"copy\"\nbase = \"snapshot\"\n";

    const SOURCE: &str = "[[source]]\nname = \"primary\"\ncapture.debezium.dialect = \"mysql\"\n";

    const SINK: &str = "[[sink]]\nname = \"rustfs\"\n[sink.warehouse.duckdb]\ns3 = { endpoint = \"http://rustfs:9000\", region = \"us-east-1\", bucket = \"lithify\", format = \"parquet\" }\nchange_log = { prefix = \"mysql_shop_cdc\" }\nplace.snapshot = [{ prefix = \"mysql_shop\" }]\n";

    fn configure(directory: &Path, infra: &str, listed: &str) {
        std::fs::write(directory.join("infra.toml"), infra).unwrap();
        std::fs::write(directory.join("artifact.toml"), ARTIFACTS).unwrap();
        std::fs::write(directory.join("table.toml"), listed).unwrap();
    }

    fn compose(directory: &Path, file: &str) {
        std::fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../compose/lithify/mysql")
                .join(file),
            directory.join(file),
        )
        .unwrap();
    }

    fn pipeline(name: &str) -> Pipeline<Capture, Sunk, Made> {
        Pipeline::open(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../compose/lithify")
                .join(name),
        )
        .unwrap()
    }

    fn scratch(name: &str) -> PathBuf {
        let directory = std::env::temp_dir().join(format!("lithify-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();

        directory
    }

    #[test]
    fn a_pipeline_is_named_after_the_directory_that_configures_it() {
        assert_eq!(pipeline("mysql").name(), "mysql");
        assert_eq!(pipeline("postgres").name(), "postgres");
    }

    #[test]
    fn a_directory_that_configures_a_pipeline_is_the_one_pipeline_it_holds() {
        let mysql = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql");

        assert_eq!(
            Pipeline::<Capture, Sunk, Made>::directories(&mysql).unwrap(),
            [mysql]
        );
    }

    #[test]
    fn a_directory_holding_pipelines_holds_each_one_configured_in_it_in_the_order_of_their_names() {
        let root = scratch("pipelines");
        for configured in ["postgres", "mysql"] {
            std::fs::create_dir_all(root.join(configured)).unwrap();
            std::fs::write(root.join(configured).join("infra.toml"), "").unwrap();
        }
        std::fs::create_dir_all(root.join("dry-run")).unwrap();

        assert_eq!(
            Pipeline::<Capture, Sunk, Made>::directories(&root).unwrap(),
            [root.join("mysql"), root.join("postgres")]
        );
    }

    #[test]
    fn a_pipeline_says_which_dialect_each_of_its_servers_is_captured_as() {
        assert_eq!(
            pipeline("mysql").sources().next().unwrap().capture(),
            &Capture::Debezium(debezium::Config::of(debezium::Dialect::MySql))
        );
        assert_eq!(
            pipeline("postgres").sources().next().unwrap().capture(),
            &Capture::Debezium(debezium::Config::of(debezium::Dialect::Postgres))
        );
    }

    #[test]
    fn a_pipeline_says_the_namespace_each_sink_holds_the_change_logs_in() {
        let pipeline = pipeline("mysql");

        let change_logs: Vec<_> = pipeline
            .sinks()
            .map(|sink| lithify_core::sink::Namespace::from(sink.change_log().clone()))
            .collect();

        assert_eq!(
            change_logs,
            [
                lithify_core::sink::Namespace::from("mysql_shop_cdc"),
                lithify_core::sink::Namespace::from("mysql_shop_cdc")
            ]
        );
    }

    #[test]
    fn a_table_the_pipeline_does_not_configure_takes_the_defaults() {
        let pipeline = pipeline("mysql");
        let table = pipeline.tables().table(&TableId::new("shop", "heartbeat"));

        assert!(table.builds(&"snapshot".into()));
        assert!(!table.builds(&"scd2".into()));
        assert_eq!(table.mask("beat_at"), Mask::Identity);
    }

    #[test]
    fn a_table_the_pipeline_configures_overrides_the_defaults() {
        let pipeline = pipeline("mysql");
        let table = pipeline.tables().table(&TableId::new("shop", "orders"));

        assert!(table.builds(&"snapshot".into()));
        assert_eq!(table.mask("attributes"), Mask::Null);
        assert_eq!(table.mask("amount"), Mask::Identity);
    }

    #[test]
    fn a_directory_that_configures_no_pipeline_is_reported_with_the_file_it_misses() {
        let unreadable = Pipeline::<Capture, Sunk, Made>::open("/nowhere").unwrap_err();

        assert!(matches!(unreadable.cause(), Cause::Missing(_)));
        assert_eq!(unreadable.path(), Path::new("/nowhere/infra.toml"));
        assert!(unreadable.to_string().contains("/nowhere/infra.toml"));
    }

    #[test]
    fn a_pipeline_that_configures_no_table_is_reported_with_the_file_it_misses() {
        let directory = scratch("pipeline-without-tables");
        compose(&directory, "infra.toml");
        compose(&directory, "artifact.toml");

        let unreadable = Pipeline::<Capture, Sunk, Made>::open(&directory).unwrap_err();

        assert!(matches!(unreadable.cause(), Cause::Missing(_)));
        assert_eq!(unreadable.path(), directory.join("table.toml"));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn a_pipeline_captured_from_no_server_is_reported_with_the_file_that_wires_it() {
        let directory = scratch("pipeline-without-servers");
        configure(&directory, SINK, "");

        let unreadable = Pipeline::<Capture, Sunk, Made>::open(&directory).unwrap_err();

        assert!(matches!(
            unreadable.cause(),
            Cause::Miswiring(Miswiring::WithoutSources)
        ));
        assert_eq!(unreadable.path(), directory.join("infra.toml"));
        assert_eq!(
            unreadable.to_string(),
            format!(
                "{}: no server is captured",
                directory.join("infra.toml").display()
            )
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn a_table_listed_twice_is_reported_with_the_file_that_lists_it() {
        let directory = scratch("pipeline-with-a-table-twice");
        compose(&directory, "infra.toml");
        compose(&directory, "artifact.toml");
        std::fs::write(
            directory.join("table.toml"),
            "[[table]]\nschema = \"shop\"\nname = \"orders\"\n\n[[table]]\nschema = \"shop\"\nname = \"orders\"\n",
        )
        .unwrap();

        let unreadable = Pipeline::<Capture, Sunk, Made>::open(&directory).unwrap_err();

        assert!(matches!(unreadable.cause(), Cause::Mislisting(_)));
        assert_eq!(unreadable.path(), directory.join("table.toml"));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    fn listing(name: &str, listed: &str) -> Result<Pipeline<Capture, Sunk, Made>, Unreadable> {
        let directory = scratch(name);
        configure(
            &directory,
            &format!("{SOURCE}{SINK}place.copy = [{{ prefix = \"mysql_shop_copy\" }}]\n"),
            listed,
        );
        let opened = Pipeline::<Capture, Sunk, Made>::open(&directory);
        std::fs::remove_dir_all(&directory).unwrap();

        opened
    }

    #[test]
    fn a_table_built_into_an_artifact_no_sink_declares_is_rejected() {
        let unreadable = listing(
            "pipeline-with-an-undeclared-artifact",
            "[[table]]\nschema = \"shop\"\nname = \"orders\"\nartifacts = [\"scd2\"]\n",
        )
        .unwrap_err();

        assert!(matches!(
            unreadable.cause(),
            Cause::Mislisting(Mislisting::Undeclared { table: Some(_), artifact }) if artifact.as_str() == "scd2"
        ));
    }

    #[test]
    fn a_table_built_into_an_artifact_declared_but_placed_by_no_sink_is_rejected() {
        let directory = scratch("pipeline-with-an-unplaced-artifact");
        configure(
            &directory,
            &format!("{SOURCE}{SINK}"),
            "[default]\nartifacts = [\"snapshot\", \"copy\"]\n",
        );
        let unreadable = Pipeline::<Capture, Sunk, Made>::open(&directory).unwrap_err();
        std::fs::remove_dir_all(&directory).unwrap();

        assert!(matches!(
            unreadable.cause(),
            Cause::Mislisting(Mislisting::Undeclared { table: None, artifact }) if artifact.as_str() == "copy"
        ));
    }

    #[test]
    fn a_table_built_into_an_artifact_but_not_into_the_one_it_is_built_from_is_rejected() {
        let unreadable = listing(
            "pipeline-without-a-source",
            "[default]\nartifacts = [\"copy\"]\n",
        )
        .unwrap_err();

        assert!(matches!(
            unreadable.cause(),
            Cause::Mislisting(Mislisting::WithoutBase { table: None, artifact, base })
                if artifact.as_str() == "copy" && base.as_str() == "snapshot"
        ));
    }

    #[test]
    fn a_table_built_into_an_artifact_and_the_one_it_is_built_from_is_read() {
        assert!(
            listing(
                "pipeline-with-a-source",
                "[default]\nartifacts = [\"snapshot\", \"copy\"]\n",
            )
            .is_ok()
        );
    }

    #[test]
    fn a_configuration_that_does_not_parse_is_reported_with_its_file() {
        let directory = scratch("unparsable-pipeline");
        std::fs::write(directory.join("infra.toml"), "capture = 1").unwrap();

        let unreadable = Pipeline::<Capture, Sunk, Made>::open(&directory).unwrap_err();

        assert!(matches!(unreadable.cause(), Cause::Malformed(_)));
        assert_eq!(unreadable.path(), directory.join("infra.toml"));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn a_pipeline_that_declares_no_artifact_is_reported_with_the_file_it_misses() {
        let directory = scratch("pipeline-without-artifacts");
        compose(&directory, "infra.toml");
        compose(&directory, "table.toml");

        let unreadable = Pipeline::<Capture, Sunk, Made>::open(&directory).unwrap_err();
        std::fs::remove_dir_all(&directory).unwrap();

        assert!(matches!(unreadable.cause(), Cause::Missing(_)));
        assert_eq!(unreadable.path(), directory.join("artifact.toml"));
    }

    #[test]
    fn an_artifact_declared_twice_is_reported_with_the_file_that_declares_it() {
        let directory = scratch("pipeline-with-an-artifact-twice");
        configure(&directory, &format!("{SOURCE}{SINK}"), "");
        std::fs::write(
            directory.join("artifact.toml"),
            format!(
                "{ARTIFACTS}\n[[artifact]]\nname = \"copy\"\nkind = \"scd1\"\nbase = \"snapshot\"\n"
            ),
        )
        .unwrap();

        let unreadable = Pipeline::<Capture, Sunk, Made>::open(&directory).unwrap_err();
        std::fs::remove_dir_all(&directory).unwrap();

        assert!(matches!(
            unreadable.cause(),
            Cause::Misdeclaring(crate::artifact::Misdeclaring::NamedTwice(artifact)) if artifact.as_str() == "copy"
        ));
        assert_eq!(unreadable.path(), directory.join("artifact.toml"));
    }

    #[test]
    fn a_place_naming_an_artifact_declared_nowhere_is_reported_with_the_file_that_wires_it() {
        let directory = scratch("pipeline-placing-an-undeclared-artifact");
        configure(
            &directory,
            &format!("{SOURCE}{SINK}place.history = [{{ prefix = \"mysql_shop_scd2\" }}]\n"),
            "",
        );

        let unreadable = Pipeline::<Capture, Sunk, Made>::open(&directory).unwrap_err();
        std::fs::remove_dir_all(&directory).unwrap();

        assert!(matches!(
            unreadable.cause(),
            Cause::Miswiring(Miswiring::Undeclared(sink, artifact)) if sink == "rustfs" && artifact.as_str() == "history"
        ));
        assert_eq!(unreadable.path(), directory.join("infra.toml"));
    }
}
