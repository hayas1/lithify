use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use lithify_core::model::TableId;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Listing {
    #[serde(default)]
    default: Defaults,
    #[serde(default, rename = "table")]
    tables: Vec<ListedTable>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Defaults {
    #[serde(default = "Defaults::snapshot")]
    artifacts: BTreeSet<String>,
}

impl Defaults {
    fn snapshot() -> BTreeSet<String> {
        BTreeSet::from(["snapshot".to_owned()])
    }
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            artifacts: Self::snapshot(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ListedTable {
    schema: String,
    name: String,
    artifacts: Option<BTreeSet<String>>,
    #[serde(default, rename = "column")]
    columns: BTreeMap<String, ListedColumn>,
}

impl ListedTable {
    fn id(&self) -> TableId {
        TableId::new(&self.schema, &self.name)
    }

    fn table(&self, default: &Table) -> Table {
        Table {
            artifacts: self
                .artifacts
                .clone()
                .unwrap_or_else(|| default.artifacts.clone()),
            masks: self
                .columns
                .iter()
                .map(|(name, column)| (name.clone(), column.mask))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tables {
    default: Table,
    tables: BTreeMap<TableId, Table>,
}

impl Tables {
    pub fn table(&self, id: &TableId) -> &Table {
        self.tables.get(id).unwrap_or(&self.default)
    }

    pub fn listed(&self) -> impl Iterator<Item = (Option<&TableId>, &Table)> {
        std::iter::once((None, &self.default))
            .chain(self.tables.iter().map(|(id, table)| (Some(id), table)))
    }
}

impl TryFrom<Listing> for Tables {
    type Error = Mislisting;

    fn try_from(listing: Listing) -> Result<Self, Self::Error> {
        let default = Table {
            artifacts: listing.default.artifacts,
            masks: BTreeMap::new(),
        };
        let mut tables = BTreeMap::new();

        for listed in &listing.tables {
            if tables.insert(listed.id(), listed.table(&default)).is_some() {
                return Err(Mislisting::TableListedTwice(listed.id()));
            }
        }

        Ok(Self { default, tables })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mislisting {
    TableListedTwice(TableId),
    Undeclared {
        table: Option<TableId>,
        artifact: String,
    },
    WithoutSource {
        table: Option<TableId>,
        artifact: String,
        source: String,
    },
}

impl Mislisting {
    fn named(table: &Option<TableId>) -> String {
        match table {
            Some(table) => table.to_string(),
            None => "a table listed nowhere".to_owned(),
        }
    }
}

impl Display for Mislisting {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::TableListedTwice(table) => write!(formatter, "{table} is listed twice"),
            Self::Undeclared { table, artifact } => write!(
                formatter,
                "{} is built into {artifact}, which no sink places",
                Self::named(table)
            ),
            Self::WithoutSource {
                table,
                artifact,
                source,
            } => write!(
                formatter,
                "{} is built into {artifact} but not into {source} it is built from",
                Self::named(table)
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    artifacts: BTreeSet<String>,
    masks: BTreeMap<String, Mask>,
}

impl Table {
    pub fn builds(&self, artifact: &str) -> bool {
        self.artifacts.contains(artifact)
    }

    pub fn artifacts(&self) -> impl Iterator<Item = &str> {
        self.artifacts.iter().map(String::as_str)
    }

    pub fn mask(&self, column: &str) -> Mask {
        self.masks.get(column).copied().unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ListedColumn {
    #[serde(default)]
    mask: Mask,
}

// A mask that takes arguments, as a hash will, is listed as a table,
// `mask.hash = { algorithm = "sha256" }`, and before an artifact applies it, it has to settle what a
// null mask gets away with:
// - each warehouse spells it in SQL of its own, and one that cannot is refused like a column type it
//   has no column for;
// - it may change the type of the column in the artifact, as a hash turns any column into text;
// - it cannot apply to a primary key unless it is deterministic, since rows are folded by their keys;
// - a secret it needs, such as a salt, is read from the environment rather than written here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mask {
    #[default]
    Identity,
    Null,
    // TODO: take the algorithm and where the salt is read from as arguments, and decide the type
    // the hashed column takes in the artifact, before any artifact applies it.
    Hash,
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLES: &str = r#"
        [default]
        artifacts = ["snapshot"]

        [[table]]
        schema = "shop"
        name = "orders"
        artifacts = ["snapshot", "scd2"]
        column.attributes.mask = "null"

        [[table]]
        schema = "shop"
        name = "audit_log"
        artifacts = []
    "#;

    fn tables(listed: &str) -> Tables {
        Tables::try_from(toml::from_str::<Listing>(listed).unwrap()).unwrap()
    }

    fn table(schema: &str, name: &str) -> Table {
        tables(TABLES).table(&TableId::new(schema, name)).clone()
    }

    #[test]
    fn a_table_that_is_not_configured_takes_the_defaults() {
        let table = table("shop", "customers");

        assert!(table.builds("snapshot"));
        assert!(!table.builds("scd2"));
    }

    #[test]
    fn a_configured_table_builds_the_artifacts_it_names() {
        let table = table("shop", "orders");

        assert!(table.builds("snapshot"));
        assert!(table.builds("scd2"));
    }

    #[test]
    fn a_table_can_be_left_out_of_every_artifact() {
        let table = table("shop", "audit_log");

        assert!(!table.builds("snapshot"));
        assert!(!table.builds("scd2"));
    }

    #[test]
    fn a_column_masked_with_null_is_filled_with_null() {
        let table = table("shop", "orders");

        assert_eq!(table.mask("attributes"), Mask::Null);
    }

    #[test]
    fn a_column_without_a_mask_is_carried_as_it_is() {
        let table = table("shop", "orders");

        assert_eq!(table.mask("amount"), Mask::Identity);
    }

    #[test]
    fn a_column_can_be_masked_with_a_hash() {
        let hashed = TABLES.replace("mask = \"null\"", "mask = \"hash\"");

        assert_eq!(
            tables(&hashed)
                .table(&TableId::new("shop", "orders"))
                .mask("attributes"),
            Mask::Hash
        );
    }

    #[test]
    fn a_configuration_that_defaults_to_nothing_in_particular_snapshots_every_table() {
        let table = tables("[default]")
            .table(&TableId::new("shop", "customers"))
            .clone();

        assert!(table.builds("snapshot"));
        assert!(!table.builds("scd2"));
    }

    #[test]
    fn a_configuration_without_defaults_snapshots_every_table() {
        let table = tables("").table(&TableId::new("shop", "customers")).clone();

        assert!(table.builds("snapshot"));
        assert!(!table.builds("scd2"));
    }

    #[test]
    fn a_column_masked_in_an_unknown_way_is_rejected() {
        let unknown = TABLES.replace("mask = \"null\"", "mask = \"hashed\"");

        assert!(toml::from_str::<Listing>(&unknown).is_err());
    }

    #[test]
    fn a_table_listed_twice_is_rejected() {
        let twice = format!("{TABLES}\n[[table]]\nschema = \"shop\"\nname = \"orders\"\n");

        let mislisting = Tables::try_from(toml::from_str::<Listing>(&twice).unwrap()).unwrap_err();

        assert_eq!(
            mislisting,
            Mislisting::TableListedTwice(TableId::new("shop", "orders"))
        );
    }

    #[test]
    fn a_column_listed_twice_is_rejected() {
        let twice = TABLES.replace(
            "column.attributes.mask = \"null\"",
            "column.attributes.mask = \"null\"\n        column.attributes.mask = \"null\"",
        );

        assert!(toml::from_str::<Listing>(&twice).is_err());
    }
}
