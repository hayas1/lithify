use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use event_replay_core::model::TableId;

use crate::Artifact;

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
    artifacts: BTreeSet<Artifact>,
}

impl Defaults {
    fn snapshot() -> BTreeSet<Artifact> {
        BTreeSet::from([Artifact::Snapshot])
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
    artifacts: Option<BTreeSet<Artifact>>,
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
            columns: self
                .columns
                .iter()
                .map(|(name, column)| (name.clone(), column.replay))
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
}

impl TryFrom<Listing> for Tables {
    type Error = Mislisting;

    fn try_from(listing: Listing) -> Result<Self, Self::Error> {
        let default = Table {
            artifacts: listing.default.artifacts,
            columns: BTreeMap::new(),
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
}

impl Display for Mislisting {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::TableListedTwice(table) => write!(formatter, "{table} is listed twice"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    artifacts: BTreeSet<Artifact>,
    columns: BTreeMap<String, Replayed>,
}

impl Table {
    pub fn builds(&self, artifact: Artifact) -> bool {
        self.artifacts.contains(&artifact)
    }

    pub fn replayed(&self, column: &str) -> Replayed {
        self.columns.get(column).copied().unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ListedColumn {
    #[serde(default)]
    replay: Replayed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Replayed {
    #[default]
    Value,
    Null,
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
        artifacts = ["snapshot", "history"]
        column.attributes.replay = "null"

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

        assert!(table.builds(Artifact::Snapshot));
        assert!(!table.builds(Artifact::History));
    }

    #[test]
    fn a_configured_table_builds_the_artifacts_it_names() {
        let table = table("shop", "orders");

        assert!(table.builds(Artifact::Snapshot));
        assert!(table.builds(Artifact::History));
    }

    #[test]
    fn a_table_can_be_left_out_of_every_artifact() {
        let table = table("shop", "audit_log");

        assert!(!table.builds(Artifact::Snapshot));
        assert!(!table.builds(Artifact::History));
    }

    #[test]
    fn a_column_the_replay_does_not_carry_is_filled_with_null() {
        let table = table("shop", "orders");

        assert_eq!(table.replayed("attributes"), Replayed::Null);
        assert_eq!(table.replayed("amount"), Replayed::Value);
    }

    #[test]
    fn a_configuration_that_defaults_to_nothing_in_particular_snapshots_every_table() {
        let table = tables("[default]")
            .table(&TableId::new("shop", "customers"))
            .clone();

        assert!(table.builds(Artifact::Snapshot));
        assert!(!table.builds(Artifact::History));
    }

    #[test]
    fn a_configuration_without_defaults_snapshots_every_table() {
        let table = tables("").table(&TableId::new("shop", "customers")).clone();

        assert!(table.builds(Artifact::Snapshot));
        assert!(!table.builds(Artifact::History));
    }

    #[test]
    fn a_column_replayed_in_an_unknown_way_is_rejected() {
        let unknown = TABLES.replace("replay = \"null\"", "replay = \"hashed\"");

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
            "column.attributes.replay = \"null\"",
            "column.attributes.replay = \"null\"\n        column.attributes.replay = \"value\"",
        );

        assert!(toml::from_str::<Listing>(&twice).is_err());
    }

    #[test]
    fn an_unknown_artifact_is_rejected() {
        let unknown = TABLES.replace("\"snapshot\", \"history\"", "\"snapshot\", \"cube\"");

        assert!(toml::from_str::<Listing>(&unknown).is_err());
    }
}
