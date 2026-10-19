use std::collections::BTreeMap;

use serde::Deserialize;
use serde::de::{self, DeserializeOwned, Deserializer};

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
pub struct Places<L>(BTreeMap<String, Place<L>>);

impl<L> Default for Places<L> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl<L> Places<L> {
    pub fn get(&self, artifact: &str) -> Option<&Place<L>> {
        self.0.get(artifact)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Place<L>)> {
        self.0
            .iter()
            .map(|(artifact, place)| (artifact.as_str(), place))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn map<M>(self, located: impl Fn(L) -> M) -> Places<M> {
        Places(
            self.0
                .into_iter()
                .map(|(artifact, place)| (artifact, place.map(&located)))
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Place<L> {
    Whole(Vec<L>),
    Cuts(BTreeMap<String, Vec<L>>),
}

impl<L> Place<L> {
    pub fn as_whole(&self) -> Option<&[L]> {
        match self {
            Self::Whole(places) => Some(places),
            Self::Cuts(_) => None,
        }
    }

    pub fn cut(&self, cut: &str) -> Option<&[L]> {
        match self {
            Self::Whole(_) => None,
            Self::Cuts(cuts) => cuts.get(cut).map(Vec::as_slice),
        }
    }

    pub(crate) fn lists(&self) -> Vec<&[L]> {
        match self {
            Self::Whole(places) => vec![places],
            Self::Cuts(cuts) => cuts.values().map(Vec::as_slice).collect(),
        }
    }

    fn map<M>(self, located: impl Fn(L) -> M) -> Place<M> {
        let all = |places: Vec<L>| places.into_iter().map(&located).collect();

        match self {
            Self::Whole(places) => Place::Whole(all(places)),
            Self::Cuts(cuts) => Place::Cuts(
                cuts.into_iter()
                    .map(|(cut, places)| (cut, all(places)))
                    .collect(),
            ),
        }
    }
}

impl<'de, L: DeserializeOwned> Deserialize<'de> for Place<L> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match toml::Value::deserialize(deserializer)? {
            toml::Value::Array(places) => Vec::deserialize(toml::Value::Array(places))
                .map(Self::Whole)
                .map_err(de::Error::custom),
            toml::Value::Table(cuts) => BTreeMap::deserialize(toml::Value::Table(cuts))
                .map(Self::Cuts)
                .map_err(de::Error::custom),
            written => Err(de::Error::custom(format!(
                "expected a list of places, or one by each instant an artifact is cut at, found {}",
                written.type_str()
            ))),
        }
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
        place.instant."09:00" = [{ prefix = "mysql_shop_0900" }]
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
                bucket: "lithify".to_owned()
            }
        );
        assert_eq!(placed.change_log(), &prefix("mysql_shop_cdc"));
    }

    #[test]
    fn an_artifact_is_placed_whole_in_each_place_it_lists() {
        assert_eq!(
            placed(PLACED)
                .places()
                .get("snapshot")
                .and_then(Place::as_whole),
            Some([prefix("mysql_shop"), prefix("shop")].as_slice())
        );
    }

    #[test]
    fn an_artifact_cut_at_several_instants_is_placed_by_each_instant() {
        let placed = placed(PLACED);
        let instant = placed.places().get("instant").unwrap();

        assert_eq!(
            instant.cut("09:00"),
            Some([prefix("mysql_shop_0900")].as_slice())
        );
        assert_eq!(instant.cut("18:00"), None);
        assert_eq!(instant.as_whole(), None);
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
    fn a_place_neither_listed_nor_by_instant_is_rejected() {
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
            places
                .get("instant")
                .and_then(|instant| instant.cut("09:00")),
            Some(["mysql_shop_0900".to_owned()].as_slice())
        );
    }
}
