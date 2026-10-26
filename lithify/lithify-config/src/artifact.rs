use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use chrono_tz::Tz;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use lithify_core::artifact::ArtifactName;
use lithify_core::schedule::Schedule;

use crate::place::Places;

pub trait Declaration {
    fn kind(&self) -> &'static str;

    fn built_from(&self) -> &'static str;

    fn base(&self) -> Option<&ArtifactName> {
        None
    }

    fn schedule(&self, time_zone: Tz) -> Option<Schedule> {
        let _ = time_zone;

        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Declarations<A>(Vec<(ArtifactName, A)>);

impl<A> Declarations<A> {
    pub fn get(&self, artifact: &ArtifactName) -> Option<&A> {
        self.iter()
            .find_map(|(name, declared)| (name == artifact).then_some(declared))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ArtifactName, &A)> {
        self.0
            .iter()
            .map(|(artifact, declared)| (artifact, declared))
    }

    pub(crate) fn placed<L>(&self, places: &Places<L>) -> Self
    where
        A: Clone,
    {
        Self(
            self.0
                .iter()
                .filter(|(artifact, _)| places.get(artifact).is_some())
                .cloned()
                .collect(),
        )
    }
}

impl<A> Default for Declarations<A> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Declaring {
    #[serde(default, rename = "artifact")]
    artifacts: Vec<toml::Table>,
}

impl Declaring {
    const NAME: &str = "name";

    fn declared<A: DeserializeOwned>(
        at: usize,
        mut written: toml::Table,
    ) -> Result<(ArtifactName, A), Misdeclaring> {
        let name = match written.remove(Self::NAME) {
            Some(toml::Value::String(name)) => ArtifactName::from(name),
            _ => return Err(Misdeclaring::Unnamed(at)),
        };
        let declared = A::deserialize(toml::Value::Table(written)).map_err(|malformed| {
            Misdeclaring::Malformed {
                artifact: name.clone(),
                cause: malformed.to_string(),
            }
        })?;

        Ok((name, declared))
    }
}

impl<A: Declaration> Declarations<A> {
    fn based(
        artifact: &ArtifactName,
        declared: &A,
        base: &ArtifactName,
        found: Option<&A>,
    ) -> Result<(), Misdeclaring> {
        let found = found.ok_or_else(|| Misdeclaring::Unbased {
            artifact: artifact.clone(),
            base: base.clone(),
        })?;

        match found.kind() == declared.built_from() {
            true => Ok(()),
            false => Err(Misdeclaring::Misbased {
                artifact: artifact.clone(),
                base: base.clone(),
                kind: declared.built_from(),
            }),
        }
    }
}

impl<A: DeserializeOwned + Declaration> TryFrom<Declaring> for Declarations<A> {
    type Error = Misdeclaring;

    fn try_from(declaring: Declaring) -> Result<Self, Misdeclaring> {
        let declarations = Self(
            declaring
                .artifacts
                .into_iter()
                .enumerate()
                .map(|(at, written)| Declaring::declared(at, written))
                .collect::<Result<_, _>>()?,
        );
        let mut named = BTreeSet::new();

        for (artifact, declared) in declarations.iter() {
            if !named.insert(artifact) {
                return Err(Misdeclaring::NamedTwice(artifact.clone()));
            }
            if let Some(base) = declared.base() {
                Self::based(artifact, declared, base, declarations.get(base))?;
            }
        }

        Ok(declarations)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Misdeclaring {
    Unnamed(usize),
    Malformed {
        artifact: ArtifactName,
        cause: String,
    },
    NamedTwice(ArtifactName),
    Unbased {
        artifact: ArtifactName,
        base: ArtifactName,
    },
    Misbased {
        artifact: ArtifactName,
        base: ArtifactName,
        kind: &'static str,
    },
}

impl Display for Misdeclaring {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unnamed(at) => write!(formatter, "artifact {} is not named", at + 1),
            Self::Malformed { artifact, cause } => write!(formatter, "{artifact}: {cause}"),
            Self::NamedTwice(artifact) => write!(formatter, "{artifact} is declared twice"),
            Self::Unbased { artifact, base } => write!(
                formatter,
                "{artifact} is built from {base}, which is declared nowhere"
            ),
            Self::Misbased {
                artifact,
                base,
                kind,
            } => write!(
                formatter,
                "{artifact} is built from {base}, which is no {kind}"
            ),
        }
    }
}

impl std::error::Error for Misdeclaring {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    #[serde(deny_unknown_fields, tag = "kind", rename_all = "lowercase")]
    enum Made {
        Snapshot { shard: String },
        Copy { base: ArtifactName },
    }

    impl Declaration for Made {
        fn kind(&self) -> &'static str {
            match self {
                Self::Snapshot { .. } => "snapshot",
                Self::Copy { .. } => "copy",
            }
        }

        fn built_from(&self) -> &'static str {
            "snapshot"
        }

        fn base(&self) -> Option<&ArtifactName> {
            match self {
                Self::Snapshot { .. } => None,
                Self::Copy { base } => Some(base),
            }
        }
    }

    const DECLARED: &str = r#"
        [[artifact]]
        name = "daily"
        kind = "snapshot"
        shard = "%Y%m%d"

        [[artifact]]
        name = "copy"
        kind = "copy"
        base = "daily"
    "#;

    fn declared(written: &str) -> Result<Declarations<Made>, Misdeclaring> {
        Declarations::try_from(toml::from_str::<Declaring>(written).unwrap())
    }

    #[test]
    fn each_artifact_is_declared_under_its_name_as_the_kind_it_names() {
        let declarations = declared(DECLARED).unwrap();

        assert_eq!(
            declarations.get(&"daily".into()),
            Some(&Made::Snapshot {
                shard: "%Y%m%d".to_owned()
            })
        );
        assert_eq!(
            declarations.get(&"copy".into()),
            Some(&Made::Copy {
                base: "daily".into()
            })
        );
    }

    #[test]
    fn the_artifacts_are_kept_in_the_order_they_are_declared() {
        assert_eq!(
            declared(DECLARED)
                .unwrap()
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["daily", "copy"]
        );
    }

    #[test]
    fn an_artifact_without_a_name_is_rejected() {
        let nameless = DECLARED.replace("name = \"copy\"\n", "");

        assert_eq!(declared(&nameless), Err(Misdeclaring::Unnamed(1)));
    }

    #[test]
    fn a_kind_the_application_does_not_build_is_rejected() {
        let unknown = DECLARED.replace("kind = \"copy\"", "kind = \"history\"");

        assert!(matches!(
            declared(&unknown),
            Err(Misdeclaring::Malformed { artifact, .. }) if artifact.as_str() == "copy"
        ));
    }

    #[test]
    fn an_artifact_declared_twice_is_rejected() {
        let twice = DECLARED.replace("name = \"copy\"", "name = \"daily\"");

        assert_eq!(
            declared(&twice),
            Err(Misdeclaring::NamedTwice("daily".into()))
        );
    }

    #[test]
    fn an_artifact_built_from_one_declared_nowhere_is_rejected() {
        let unbased = DECLARED.replace("base = \"daily\"", "base = \"weekly\"");

        assert_eq!(
            declared(&unbased),
            Err(Misdeclaring::Unbased {
                artifact: "copy".into(),
                base: "weekly".into()
            })
        );
    }

    #[test]
    fn an_artifact_built_from_another_names_the_one_it_is_built_from() {
        let base = DECLARED.replace("base = \"daily\"\n", "");

        assert!(matches!(
            declared(&base),
            Err(Misdeclaring::Malformed { artifact, .. }) if artifact.as_str() == "copy"
        ));
    }

    #[test]
    fn an_artifact_built_from_one_of_another_kind_than_it_is_built_from_is_rejected() {
        let misbased = format!(
            "{DECLARED}\n[[artifact]]\nname = \"twice\"\nkind = \"copy\"\nbase = \"copy\"\n"
        );

        assert_eq!(
            declared(&misbased),
            Err(Misdeclaring::Misbased {
                artifact: "twice".into(),
                base: "copy".into(),
                kind: "snapshot",
            })
        );
    }
}
