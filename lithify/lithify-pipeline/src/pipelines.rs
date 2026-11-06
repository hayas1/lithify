use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

use crate::artifact::Declaration;
use crate::pipeline::Pipeline;
use crate::place::Placing;
use crate::unreadable::Unreadable;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineLocation {
    name: String,
    directory: String,
}

impl PipelineLocation {
    pub fn new(name: &str, directory: &str) -> Self {
        Self {
            name: name.to_owned(),
            directory: directory.to_owned(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn directory(&self) -> &str {
        &self.directory
    }
}

pub struct Pipelines<C, W, A> {
    root: PathBuf,
    configured: PhantomData<fn(C, W, A)>,
}

impl<C, W, A> Pipelines<C, W, A>
where
    C: Clone + DeserializeOwned,
    W: Clone + DeserializeOwned + Placing,
    A: Clone + DeserializeOwned + Declaration,
{
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            configured: PhantomData,
        }
    }

    pub fn locations(&self) -> Result<Vec<PipelineLocation>, Unlisted> {
        Ok(self
            .directories()?
            .iter()
            .map(|directory| Self::location(directory))
            .collect())
    }

    pub fn located(&self, name: &str) -> Result<Option<PipelineLocation>, Unlisted> {
        Ok(self
            .locations()?
            .into_iter()
            .find(|location| location.name() == name))
    }

    pub fn open(&self, location: &PipelineLocation) -> Result<Pipeline<C, W, A>, Unreadable> {
        Pipeline::open(location.directory())
    }

    fn directories(&self) -> Result<Vec<PathBuf>, Unlisted> {
        let directories =
            Pipeline::<C, W, A>::directories(&self.root).map_err(Unlisted::Unreadable)?;

        match directories.is_empty() {
            true => Err(Unlisted::Empty(self.root.clone())),
            false => Ok(directories),
        }
    }

    fn location(directory: &Path) -> PipelineLocation {
        let name = directory
            .file_name()
            .unwrap_or(directory.as_os_str())
            .to_string_lossy();

        PipelineLocation::new(&name, &directory.display().to_string())
    }
}

#[derive(Debug)]
pub enum Unlisted {
    Unreadable(Unreadable),
    Empty(PathBuf),
}

impl Display for Unlisted {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreadable(cause) => cause.fmt(formatter),
            Self::Empty(root) => write!(formatter, "{} configures no pipeline", root.display()),
        }
    }
}

impl std::error::Error for Unlisted {}
