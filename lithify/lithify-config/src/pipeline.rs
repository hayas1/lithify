use std::path::Path;

use serde::de::DeserializeOwned;

use lithify_core::model::TableId;

use crate::infra::{self, Sink, Source};
use crate::table::{self, Table, Tables};
use crate::unreadable::Unreadable;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pipeline<C, W> {
    name: String,
    sources: Vec<Source<C>>,
    sinks: Vec<Sink<W>>,
    tables: Tables,
}

impl<C: Clone + DeserializeOwned, W: Clone + DeserializeOwned> Pipeline<C, W> {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, Unreadable> {
        let directory = directory.as_ref();
        let wiring = directory.join("infra.toml");
        let listing = directory.join("table.toml");
        let written: infra::Wiring<C, W> = Self::read(&wiring)?;
        let miswired = |miswiring| Unreadable::at(&wiring, miswiring);

        Ok(Self {
            name: Self::named(directory),
            sources: written.sources().map_err(miswired)?,
            sinks: written.sinks().map_err(miswired)?,
            tables: Tables::try_from(Self::read::<table::Listing>(&listing)?)
                .map_err(|mislisting| Unreadable::at(&listing, mislisting))?,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sources(&self) -> impl Iterator<Item = &Source<C>> {
        self.sources.iter()
    }

    pub fn sinks(&self) -> impl Iterator<Item = &Sink<W>> {
        self.sinks.iter()
    }

    pub fn table(&self, id: &TableId) -> &Table {
        self.tables.table(id)
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
    use std::path::PathBuf;

    use super::*;
    use crate::ArtifactKind;
    use crate::infra::Miswiring;
    use crate::table::Mask;
    use crate::unreadable::Cause;
    use lithify_protocol::sink::bigquery::config as bigquery;
    use lithify_protocol::sink::duckdb::config::Store;
    use lithify_protocol::source::debezium::config as debezium;

    #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Sunk {
        DuckDb(Store),
        BigQuery(bigquery::Config),
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Capture {
        Debezium(debezium::Config),
    }

    fn pipeline(name: &str) -> Pipeline<Capture, Sunk> {
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

        let change_logs: Vec<_> = pipeline.sinks().map(|sink| sink.change_log()).collect();

        assert_eq!(change_logs, ["mysql_shop_cdc", "mysql_shop_cdc"]);
    }

    #[test]
    fn a_table_the_pipeline_does_not_configure_takes_the_defaults() {
        let pipeline = pipeline("mysql");
        let table = pipeline.table(&TableId::new("shop", "order_items"));

        assert!(table.builds(ArtifactKind::Snapshot));
        assert!(!table.builds(ArtifactKind::History));
        assert_eq!(table.mask("sku"), Mask::Identity);
    }

    #[test]
    fn a_table_the_pipeline_configures_overrides_the_defaults() {
        let pipeline = pipeline("mysql");
        let table = pipeline.table(&TableId::new("shop", "orders"));

        assert!(table.builds(ArtifactKind::Snapshot));
        assert!(table.builds(ArtifactKind::History));
        assert_eq!(table.mask("attributes"), Mask::Null);
        assert_eq!(table.mask("amount"), Mask::Identity);
    }

    #[test]
    fn a_directory_that_configures_no_pipeline_is_reported_with_the_file_it_misses() {
        let unreadable = Pipeline::<Capture, Sunk>::open("/nowhere").unwrap_err();

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

        let unreadable = Pipeline::<Capture, Sunk>::open(&directory).unwrap_err();

        assert!(matches!(unreadable.cause(), Cause::Missing(_)));
        assert_eq!(unreadable.path(), directory.join("table.toml"));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn a_pipeline_captured_from_no_server_is_reported_with_the_file_that_wires_it() {
        let directory = scratch("pipeline-without-servers");
        std::fs::write(
            directory.join("infra.toml"),
            "[[sink]]\nname = \"rustfs\"\nchange_log = \"mysql_shop_cdc\"\nwarehouse.duckdb.s3.endpoint = \"http://rustfs:9000\"\nwarehouse.duckdb.s3.region = \"us-east-1\"\nwarehouse.duckdb.s3.bucket = \"lithify\"\nwarehouse.duckdb.s3.format = \"parquet\"\nartifacts.snapshot = \"mysql_shop\"\nartifacts.history = \"mysql_shop_history\"\nartifacts.shard = \"%Y%m%d\"\n",
        )
        .unwrap();
        std::fs::write(directory.join("table.toml"), "").unwrap();

        let unreadable = Pipeline::<Capture, Sunk>::open(&directory).unwrap_err();

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

        let unreadable = Pipeline::<Capture, Sunk>::open(&directory).unwrap_err();

        assert!(matches!(unreadable.cause(), Cause::Mislisting(_)));
        assert_eq!(unreadable.path(), directory.join("table.toml"));
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn a_configuration_that_does_not_parse_is_reported_with_its_file() {
        let directory = scratch("unparsable-pipeline");
        std::fs::write(directory.join("infra.toml"), "capture = 1").unwrap();

        let unreadable = Pipeline::<Capture, Sunk>::open(&directory).unwrap_err();

        assert!(matches!(unreadable.cause(), Cause::Malformed(_)));
        assert_eq!(unreadable.path(), directory.join("infra.toml"));
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
