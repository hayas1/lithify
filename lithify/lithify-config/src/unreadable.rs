use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};

use crate::artifact::Misdeclaring;
use crate::infra::Miswiring;
use crate::table::Mislisting;

#[derive(Debug)]
pub struct Unreadable {
    path: PathBuf,
    cause: Cause,
}

impl Unreadable {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn cause(&self) -> &Cause {
        &self.cause
    }

    pub(crate) fn at(path: &Path, cause: impl Into<Cause>) -> Self {
        Self {
            path: path.to_owned(),
            cause: cause.into(),
        }
    }
}

impl Display for Unreadable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.path.display(), self.cause)
    }
}

impl std::error::Error for Unreadable {}

#[derive(Debug)]
pub enum Cause {
    Missing(std::io::Error),
    Malformed(toml::de::Error),
    Misdeclaring(Misdeclaring),
    Mislisting(Mislisting),
    Miswiring(Miswiring),
}

impl Display for Cause {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing(cause) => write!(formatter, "{cause}"),
            Self::Malformed(cause) => write!(formatter, "{cause}"),
            Self::Misdeclaring(cause) => write!(formatter, "{cause}"),
            Self::Mislisting(cause) => write!(formatter, "{cause}"),
            Self::Miswiring(cause) => write!(formatter, "{cause}"),
        }
    }
}

impl From<std::io::Error> for Cause {
    fn from(cause: std::io::Error) -> Self {
        Self::Missing(cause)
    }
}

impl From<toml::de::Error> for Cause {
    fn from(cause: toml::de::Error) -> Self {
        Self::Malformed(cause)
    }
}

impl From<Misdeclaring> for Cause {
    fn from(cause: Misdeclaring) -> Self {
        Self::Misdeclaring(cause)
    }
}

impl From<Mislisting> for Cause {
    fn from(cause: Mislisting) -> Self {
        Self::Mislisting(cause)
    }
}

impl From<Miswiring> for Cause {
    fn from(cause: Miswiring) -> Self {
        Self::Miswiring(cause)
    }
}
