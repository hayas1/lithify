use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

use lithify_core::model::{Mask, TableId};
use lithify_core::sink::SinkWarehouse;

use crate::artifact::Declaration;
use crate::infra::{self, Sink, Source};
use crate::table::{self, Mislisting, Table, Tables};
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
    W: Clone + DeserializeOwned + SinkWarehouse<Location: DeserializeOwned>,
    A: Clone + DeserializeOwned + Declaration<Location = W::Location>,
{
    const WIRING: &str = "infra.toml";
    const LISTING: &str = "table.toml";

    pub fn open(directory: impl AsRef<Path>) -> Result<Self, Unreadable> {
        let directory = directory.as_ref();
        let wiring = directory.join(Self::WIRING);
        let listing = directory.join(Self::LISTING);
        let written: infra::Wiring<C, W, A> = Self::read(&wiring)?;
        let miswired = |miswiring| Unreadable::at(&wiring, miswiring);
        let mislisted = |mislisting| Unreadable::at(&listing, mislisting);
        let sources = written.sources().map_err(miswired)?;
        let sinks = written.sinks().map_err(miswired)?;
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
                if let Some(source) = declared.source()
                    && !listed.builds(source)
                {
                    return Err(Mislisting::WithoutSource {
                        table: table.cloned(),
                        artifact: artifact.to_owned(),
                        source: source.to_owned(),
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

    pub fn table(&self, id: &TableId) -> &Table {
        self.tables.table(id)
    }

    pub fn masks(&self) -> BTreeMap<TableId, BTreeMap<String, Mask>> {
        self.tables.masks()
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
    use crate::table::Mask;
    use crate::unreadable::Cause;
    use lithify_protocol::sink::bigquery::config as bigquery;
    use lithify_protocol::sink::duckdb::config as duckdb;
    use lithify_protocol::source::debezium::config as debezium;

    #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Sunk {
        DuckDb(duckdb::Config),
        BigQuery(bigquery::Config),
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Lying {
        DuckDb(duckdb::Location),
        BigQuery(bigquery::Location),
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Made {
        Snapshot { shard: String, place: Vec<Lying> },
        Copy { place: Vec<Lying> },
    }

    impl Declaration for Made {
        type Location = Lying;

        fn source(&self) -> Option<&'static str> {
            match self {
                Self::Snapshot { .. } => None,
                Self::Copy { .. } => Some("snapshot"),
            }
        }

        fn places(&self) -> impl Iterator<Item = &Lying> {
            match self {
                Self::Snapshot { place, .. } | Self::Copy { place } => place.iter(),
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

        fn with_client<F: lithify_core::sink::WarehouseFn>(
            &self,
            _: &Lying,
            _: &(),
            _: F,
        ) -> Result<F::Output, std::fmt::Error> {
            Err(std::fmt::Error)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Capture {
        Debezium(debezium::Config),
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
        std::fs::create_dir_all(root.join("plans")).unwrap();

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
        let table = pipeline.table(&TableId::new("shop", "order_items"));

        assert!(table.builds("snapshot"));
        assert_eq!(table.mask("sku"), Mask::Identity);
    }

    #[test]
    fn a_table_the_pipeline_configures_overrides_the_defaults() {
        let pipeline = pipeline("mysql");
        let table = pipeline.table(&TableId::new("shop", "orders"));

        assert!(table.builds("snapshot"));
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
        std::fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql/infra.toml"),
            directory.join("infra.toml"),
        )
        .unwrap();

        let unreadable = Pipeline::<Capture, Sunk, Made>::open(&directory).unwrap_err();

        assert!(matches!(unreadable.cause(), Cause::Missing(_)));
        assert_eq!(unreadable.path(), directory.join("table.toml"));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn a_pipeline_captured_from_no_server_is_reported_with_the_file_that_wires_it() {
        let directory = scratch("pipeline-without-servers");
        std::fs::write(
            directory.join("infra.toml"),
            "[[sink]]\nname = \"rustfs\"\nchange_log.duckdb.prefix = \"mysql_shop_cdc\"\nwarehouse.duckdb.s3.endpoint = \"http://rustfs:9000\"\nwarehouse.duckdb.s3.region = \"us-east-1\"\nwarehouse.duckdb.s3.bucket = \"lithify\"\nwarehouse.duckdb.s3.format = \"parquet\"\nartifact.snapshot.shard = \"%Y%m%d\"\nartifact.snapshot.place = [{ duckdb.prefix = \"mysql_shop\" }]\n",
        )
        .unwrap();
        std::fs::write(directory.join("table.toml"), "").unwrap();

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
        std::fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql/infra.toml"),
            directory.join("infra.toml"),
        )
        .unwrap();
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
        std::fs::write(
            directory.join("infra.toml"),
            "[[source]]\nname = \"primary\"\ncapture.debezium.dialect = \"mysql\"\n[[sink]]\nname = \"rustfs\"\nchange_log.duckdb.prefix = \"mysql_shop_cdc\"\nwarehouse.duckdb.s3.endpoint = \"http://rustfs:9000\"\nwarehouse.duckdb.s3.region = \"us-east-1\"\nwarehouse.duckdb.s3.bucket = \"lithify\"\nwarehouse.duckdb.s3.format = \"parquet\"\nartifact.snapshot.shard = \"%Y%m%d\"\nartifact.snapshot.place = [{ duckdb.prefix = \"mysql_shop\" }]\nartifact.copy.place = [{ duckdb.prefix = \"mysql_shop_copy\" }]\n",
        )
        .unwrap();
        std::fs::write(directory.join("table.toml"), listed).unwrap();
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
            Cause::Mislisting(Mislisting::Undeclared { table: Some(_), artifact }) if artifact == "scd2"
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
            Cause::Mislisting(Mislisting::WithoutSource { table: None, artifact, source })
                if artifact == "copy" && source == "snapshot"
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
}
