use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

use crate::warehouse::SinkWarehouse;

use crate::artifact::{Declaration, Declarations, Declaring};
use crate::infra::{self, Sink, Source};
use crate::place::Placing;
use crate::table::{self, Mislisting, Tables};
use crate::unreadable::Unreadable;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pipeline<C, W: SinkWarehouse, A> {
    name: String,
    sources: Vec<Source<C>>,
    sinks: Vec<Sink<W, A>>,
    tables: Tables,
}

impl<C, W, A> Pipeline<C, W, A>
where
    C: Clone + DeserializeOwned,
    W: Clone + DeserializeOwned + Placing,
    A: Clone + DeserializeOwned + Declaration,
{
    const WIRING: &str = "infra.toml";
    const DECLARING: &str = "artifact.toml";
    const LISTING: &str = "table.toml";

    pub fn open(directory: impl AsRef<Path>) -> Result<Self, Unreadable> {
        let directory = directory.as_ref();
        let wiring = directory.join(Self::WIRING);
        let declaring = directory.join(Self::DECLARING);
        let listing = directory.join(Self::LISTING);
        let written: infra::Wiring<C, W> = Self::read(&wiring)?;
        let miswired = |miswiring| Unreadable::at(&wiring, miswiring);
        let mislisted = |mislisting| Unreadable::at(&listing, mislisting);
        let declarations = Declarations::try_from(Self::read::<Declaring>(&declaring)?)
            .map_err(|misdeclaring| Unreadable::at(&declaring, misdeclaring))?;
        let sources = written.sources().map_err(miswired)?;
        let sinks = written.sinks(&declarations).map_err(miswired)?;
        let tables =
            Tables::try_from(Self::read::<table::Listing>(&listing)?).map_err(mislisted)?;
        Self::declared(&sinks, &tables).map_err(mislisted)?;

        Ok(Self {
            name: Self::named(directory),
            sources,
            sinks,
            tables,
        })
    }

    fn declared(sinks: &[Sink<W, A>], tables: &Tables) -> Result<(), Mislisting> {
        for (table, listed) in tables.listed() {
            for artifact in listed.artifacts() {
                let declared = sinks
                    .iter()
                    .find_map(|sink| sink.artifact(artifact))
                    .ok_or_else(|| Mislisting::Undeclared {
                        table: table.cloned(),
                        artifact: artifact.to_owned(),
                    })?;
                if let Some(base) = declared.base()
                    && !listed.builds(base)
                {
                    return Err(Mislisting::WithoutBase {
                        table: table.cloned(),
                        artifact: artifact.to_owned(),
                        base: base.to_owned(),
                    });
                }
            }
        }

        Ok(())
    }

    pub fn directories(root: impl AsRef<Path>) -> Result<Vec<PathBuf>, Unreadable> {
        let root = root.as_ref();

        if Self::configures(root) {
            return Ok(vec![root.to_owned()]);
        }

        let mut directories = std::fs::read_dir(root)
            .map_err(|unlisted| Unreadable::at(root, unlisted))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|unlisted| Unreadable::at(root, unlisted))?;
        directories.retain(|directory| Self::configures(directory));
        directories.sort();

        Ok(directories)
    }

    fn configures(directory: &Path) -> bool {
        directory.join(Self::WIRING).is_file()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sources(&self) -> impl Iterator<Item = &Source<C>> {
        self.sources.iter()
    }

    pub fn sinks(&self) -> impl Iterator<Item = &Sink<W, A>> {
        self.sinks.iter()
    }

    pub fn tables(&self) -> &Tables {
        &self.tables
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
