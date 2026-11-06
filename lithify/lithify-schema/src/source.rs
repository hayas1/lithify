use std::fmt;

use crate::event::SchemaChange;
use crate::listener::{CatalogModel, OutOfOrder, SchemaChangeListener};
use crate::position::{Coordinate, Position};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPath(Vec<String>);

impl ColumnPath {
    pub fn parts(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }
}

impl From<&str> for ColumnPath {
    fn from(path: &str) -> Self {
        Self(path.split('.').map(str::to_owned).collect())
    }
}

impl fmt::Display for ColumnPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.join("."))
    }
}

pub trait ChangeLog {
    type Position: Position;

    fn envelope(&self) -> Option<ColumnPath>;

    fn event_time(&self) -> ColumnPath;

    fn processing_time(&self) -> ColumnPath;

    fn ordering(&self) -> Vec<ColumnPath>;

    fn coordinates(&self, position: &Self::Position) -> Vec<Coordinate>;

    fn sequence(&self) -> Vec<ColumnPath> {
        std::iter::once(self.processing_time())
            .chain(self.ordering())
            .collect()
    }
}

pub struct SchemaHistory<P> {
    opened: Vec<SchemaChange<P>>,
    within: Vec<SchemaChange<P>>,
}

impl<P: Position> SchemaHistory<P> {
    pub fn new(opened: Vec<SchemaChange<P>>, within: Vec<SchemaChange<P>>) -> Self {
        Self { opened, within }
    }

    pub fn opened(&self) -> &[SchemaChange<P>] {
        &self.opened
    }

    pub fn within(&self) -> &[SchemaChange<P>] {
        &self.within
    }

    pub fn replay<E, V>(&self, context: &V::Context, listener: &mut V) -> Result<CatalogModel<P>, E>
    where
        V: SchemaChangeListener<P>,
        E: From<OutOfOrder> + From<V::Error>,
    {
        let mut catalog = CatalogModel::default();

        for change in &self.opened {
            catalog.accept::<OutOfOrder, ()>(&(), change, &mut ())?;
        }
        for change in &self.within {
            catalog.accept::<E, V>(context, change, listener)?;
        }

        Ok(catalog)
    }
}
