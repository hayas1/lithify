use std::collections::BTreeMap;

use serde::Deserialize;
use serde::de::{self, DeserializeOwned, Deserializer};

pub trait Declaration {
    type Location;

    fn source(&self) -> Option<&'static str>;

    fn places(&self) -> impl Iterator<Item = &Self::Location>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declarations<A>(BTreeMap<String, A>);

impl<A> Declarations<A> {
    pub fn get(&self, artifact: &str) -> Option<&A> {
        self.0.get(artifact)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &A)> {
        self.0
            .iter()
            .map(|(artifact, declared)| (artifact.as_str(), declared))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn over(mut self, default: &Self) -> Self
    where
        A: Clone,
    {
        for (artifact, declared) in &default.0 {
            self.0
                .entry(artifact.clone())
                .or_insert_with(|| declared.clone());
        }

        self
    }
}

impl<'de, A: DeserializeOwned> Deserialize<'de> for Declarations<A> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        toml::Table::deserialize(deserializer)?
            .into_iter()
            .map(|(artifact, written)| {
                let declared = A::deserialize(toml::Value::Table(toml::Table::from_iter([(
                    artifact.clone(),
                    written,
                )])))
                .map_err(de::Error::custom)?;

                Ok((artifact, declared))
            })
            .collect::<Result<_, _>>()
            .map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    #[serde(deny_unknown_fields, rename_all = "lowercase")]
    enum Made {
        Snapshot { shard: String },
        Copy {},
    }

    fn declared(written: &str) -> Declarations<Made> {
        toml::from_str(written).unwrap()
    }

    #[test]
    fn each_artifact_is_declared_under_the_name_of_its_kind() {
        let declarations = declared("snapshot.shard = \"%Y%m%d\"\ncopy = {}");

        assert_eq!(
            declarations.get("snapshot"),
            Some(&Made::Snapshot {
                shard: "%Y%m%d".to_owned()
            })
        );
        assert_eq!(declarations.get("copy"), Some(&Made::Copy {}));
    }

    #[test]
    fn a_kind_the_application_does_not_build_is_rejected() {
        assert!(toml::from_str::<Declarations<Made>>("history = {}").is_err());
    }

    #[test]
    fn an_artifact_declared_again_replaces_the_one_declared_by_default_whole() {
        let declarations = declared("snapshot.shard = \"%Y%m\"")
            .over(&declared("snapshot.shard = \"%Y%m%d\"\ncopy = {}"));

        assert_eq!(
            declarations.get("snapshot"),
            Some(&Made::Snapshot {
                shard: "%Y%m".to_owned()
            })
        );
        assert_eq!(declarations.get("copy"), Some(&Made::Copy {}));
    }
}
