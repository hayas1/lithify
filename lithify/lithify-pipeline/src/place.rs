use std::collections::BTreeMap;

use serde::Deserialize;
use serde::de::{self, DeserializeOwned, Deserializer};

use lithify_core::artifact::ArtifactName;
use lithify_core::sink::SinkWarehouse;

pub trait Placing: SinkWarehouse {
    fn change_log(&self) -> Self::Location;

    fn places(&self) -> Places<Self::Location>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed<W, L> {
    warehouse: W,
    change_log: L,
    places: Places<L>,
}

impl<W, L> Placed<W, L> {
    const CHANGE_LOG: &str = "change_log";
    const PLACE: &str = "place";

    pub fn warehouse(&self) -> &W {
        &self.warehouse
    }

    pub fn change_log(&self) -> &L {
        &self.change_log
    }

    pub fn places(&self) -> &Places<L> {
        &self.places
    }
}

impl<'de, W: DeserializeOwned, L: DeserializeOwned> Deserialize<'de> for Placed<W, L> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut written = toml::Table::deserialize(deserializer)?;
        let change_log = written
            .remove(Self::CHANGE_LOG)
            .ok_or_else(|| de::Error::missing_field(Self::CHANGE_LOG))?;
        let places = written
            .remove(Self::PLACE)
            .map(Places::deserialize)
            .transpose()
            .map_err(de::Error::custom)?
            .unwrap_or_default();

        Ok(Self {
            warehouse: W::deserialize(toml::Value::Table(written)).map_err(de::Error::custom)?,
            change_log: L::deserialize(change_log).map_err(de::Error::custom)?,
            places,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(bound(deserialize = "L: DeserializeOwned"))]
pub struct Places<L>(BTreeMap<ArtifactName, Vec<L>>);

impl<L> Default for Places<L> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl<L> Places<L> {
    pub fn get(&self, artifact: &ArtifactName) -> Option<&[L]> {
        self.0.get(artifact).map(Vec::as_slice)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ArtifactName, &[L])> {
        self.0
            .iter()
            .map(|(artifact, places)| (artifact, places.as_slice()))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn map<M>(self, located: impl Fn(L) -> M) -> Places<M> {
        Places(
            self.0
                .into_iter()
                .map(|(artifact, places)| (artifact, places.into_iter().map(&located).collect()))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Bucket {
        bucket: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Prefix {
        prefix: String,
    }

    fn prefix(prefix: &str) -> Prefix {
        Prefix {
            prefix: prefix.to_owned(),
        }
    }

    const PLACED: &str = r#"
        bucket = "lithify"
        change_log = { prefix = "mysql_shop_cdc" }
        place.snapshot = [{ prefix = "mysql_shop" }, { prefix = "shop" }]
        place.instant_0900 = [{ prefix = "mysql_shop_0900" }]
    "#;

    fn placed(written: &str) -> Placed<Bucket, Prefix> {
        toml::from_str(written).unwrap()
    }

    #[test]
    fn a_warehouse_is_read_apart_from_where_it_places_the_change_log_and_each_artifact() {
        let placed = placed(PLACED);

        assert_eq!(
            placed.warehouse(),
            &Bucket {
                bucket: "lithify".into()
            }
        );
        assert_eq!(placed.change_log(), &prefix("mysql_shop_cdc"));
    }

    #[test]
    fn an_artifact_is_placed_whole_in_each_place_it_lists() {
        assert_eq!(
            placed(PLACED).places().get(&"snapshot".into()),
            Some([prefix("mysql_shop"), prefix("shop")].as_slice())
        );
    }

    #[test]
    fn a_warehouse_placing_nothing_places_no_artifact() {
        let placed = placed("bucket = \"lithify\"\nchange_log = { prefix = \"mysql_shop_cdc\" }");

        assert!(placed.places().is_empty());
    }

    #[test]
    fn a_warehouse_without_a_change_log_is_rejected() {
        assert!(toml::from_str::<Placed<Bucket, Prefix>>("bucket = \"lithify\"").is_err());
    }

    #[test]
    fn a_place_written_the_way_of_another_warehouse_is_rejected() {
        let elsewhere = PLACED.replace("{ prefix = \"shop\" }", "{ dataset = \"shop\" }");

        assert!(toml::from_str::<Placed<Bucket, Prefix>>(&elsewhere).is_err());
    }

    #[test]
    fn a_warehouse_setting_it_does_not_take_is_rejected() {
        let unknown = format!("region = \"us-east-1\"\n{PLACED}");

        assert!(toml::from_str::<Placed<Bucket, Prefix>>(&unknown).is_err());
    }

    #[test]
    fn a_place_not_listed_is_rejected() {
        let unlisted = PLACED.replace(
            "place.snapshot = [{ prefix = \"mysql_shop\" }, { prefix = \"shop\" }]",
            "place.snapshot = \"mysql_shop\"",
        );

        assert!(toml::from_str::<Placed<Bucket, Prefix>>(&unlisted).is_err());
    }

    #[test]
    fn the_places_are_carried_into_another_way_of_writing_them() {
        let places = placed(PLACED).places().clone().map(|place| place.prefix);

        assert_eq!(
            places.get(&"instant_0900".into()),
            Some(["mysql_shop_0900".into()].as_slice())
        );
    }
}
