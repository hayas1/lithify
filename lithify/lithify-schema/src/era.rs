use crate::event::SchemaChange;
use crate::listener::{Alteration, TableAltered, TableRenamed};
use crate::model::TableModel;
use crate::position::{Coordinate, Position};
use crate::source::{ChangeLog, ColumnPath};
use crate::ts::{Event, Processing, Ts};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bound<P> {
    EventTime(Ts<Event>),
    Position { at: Ts<Processing>, position: P },
    Open,
}

impl<P> Bound<P> {
    pub fn placed<L: ChangeLog<Position = P>>(&self, log: &L) -> Vec<(ColumnPath, Coordinate)> {
        match self {
            Self::EventTime(at) => vec![(log.event_time(), Coordinate::from(*at))],
            Self::Position { at, position } => log
                .sequence()
                .into_iter()
                .zip(std::iter::once(Coordinate::from(*at)).chain(log.coordinates(position)))
                .collect(),
            Self::Open => Vec::new(),
        }
    }
}

impl<P: Clone> From<&SchemaChange<P>> for Bound<P> {
    fn from(change: &SchemaChange<P>) -> Self {
        Self::Position {
            at: *change.processing_time(),
            position: change.position().clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Era<P> {
    table: TableModel,
    from: Bound<P>,
    to: Bound<P>,
}

impl<P> Era<P> {
    pub fn new(table: TableModel, from: Bound<P>, to: Bound<P>) -> Self {
        Self { table, from, to }
    }

    pub fn table(&self) -> &TableModel {
        &self.table
    }

    pub fn from(&self) -> &Bound<P> {
        &self.from
    }

    pub fn to(&self) -> &Bound<P> {
        &self.to
    }
}

// A listener that folds a table era by era keeps one of these for where the era running now
// began, the one thing the schema changes it is told of do not tell it again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EraTracker<P> {
    since: Bound<P>,
}

impl<P: Position> EraTracker<P> {
    pub fn since(since: Bound<P>) -> Self {
        Self { since }
    }

    pub fn altered(&mut self, event: &TableAltered<'_, P>) -> Option<Era<P>> {
        match event.alterations().iter().all(Alteration::is_metadata_only) {
            true => None,
            false => self.close(event.before(), Bound::from(event.change())),
        }
    }

    pub fn renamed(&mut self, event: &TableRenamed<'_, P>) -> Option<Era<P>> {
        self.close(event.before(), Bound::from(event.change()))
    }

    pub fn last(&self, table: &TableModel, to: Bound<P>) -> Era<P> {
        Era::new(table.clone(), self.since.clone(), to)
    }

    fn close(&mut self, table: &TableModel, at: Bound<P>) -> Option<Era<P>> {
        let from = std::mem::replace(&mut self.since, at.clone());

        (from != at).then(|| Era::new(table.clone(), from, at))
    }
}
