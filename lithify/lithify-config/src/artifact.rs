use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ArtifactKind {
    Snapshot,
    History,
}

impl Display for ArtifactKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot => formatter.write_str("snapshot"),
            Self::History => formatter.write_str("history"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_artifact_is_named_as_the_pipeline_writes_it() {
        assert_eq!(ArtifactKind::Snapshot.to_string(), "snapshot");
        assert_eq!(ArtifactKind::History.to_string(), "history");
    }
}
