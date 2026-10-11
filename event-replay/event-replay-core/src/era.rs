use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::marker::PhantomData;

use crate::event::SchemaChange;
use crate::listener::{
    CatalogModel, ColumnAdded, ColumnDropped, ColumnRetyped, PrimaryKeyChanged,
    SchemaChangeListener, TableCreated, TableDropped,
};
use crate::model::{TableId, TableModel};
use crate::position::{Position, RecordedAt};
use crate::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Era {
    table: TableModel,
    window: Window,
}

impl Era {
    pub fn new(table: TableModel, window: Window) -> Self {
        Self { table, window }
    }

    pub fn table(&self) -> &TableModel {
        &self.table
    }

    pub fn window(&self) -> &Window {
        &self.window
    }
}

pub struct EraTracker<F, L, E> {
    window: Window,
    at: RecordedAt,
    open: F,
    tables: BTreeMap<TableId, Track<L>>,
    error: PhantomData<fn() -> E>,
}

struct Track<L> {
    listener: L,
    since: RecordedAt,
}

impl<L> Track<L> {
    fn open<E>(
        open: &mut impl FnMut(&TableId) -> Result<L, E>,
        table: &TableId,
        since: RecordedAt,
    ) -> Result<Self, E> {
        Ok(Self {
            listener: open(table)?,
            since,
        })
    }

    fn advance<P>(
        &mut self,
        context: &L::Context,
        at: RecordedAt,
        before: &TableModel,
    ) -> Result<(), L::Error>
    where
        L: SchemaChangeListener<P>,
    {
        if self.since < at {
            let era = Era::new(before.clone(), Window::new(self.since, at));

            self.listener.visit_era(context, &era)?;
            self.since = at;
        }

        Ok(())
    }

    fn finish<P>(
        mut self,
        context: &L::Context,
        table: &TableModel,
        to: RecordedAt,
    ) -> Result<L, L::Error>
    where
        L: SchemaChangeListener<P>,
    {
        self.advance(context, to, table)?;

        Ok(self.listener)
    }
}

impl<F, L, E> EraTracker<F, L, E> {
    pub fn new(window: Window, open: F) -> Self {
        Self {
            at: *window.from(),
            window,
            open,
            tables: BTreeMap::new(),
            error: PhantomData,
        }
    }

    pub fn finish<P: Position>(
        mut self,
        context: &L::Context,
        catalog: &CatalogModel<P>,
    ) -> Result<Vec<L>, E>
    where
        L: SchemaChangeListener<P>,
        F: FnMut(&TableId) -> Result<L, E>,
        E: From<L::Error>,
    {
        catalog
            .tables()
            .map(|table| {
                let track = match self.tables.remove(table.id()) {
                    Some(track) => track,
                    None => self.open(table.id(), *self.window.from())?,
                };

                Ok(track.finish(context, table, *self.window.to())?)
            })
            .collect()
    }

    fn open<P>(&mut self, table: &TableId, since: RecordedAt) -> Result<Track<L>, E>
    where
        L: SchemaChangeListener<P>,
        F: FnMut(&TableId) -> Result<L, E>,
        E: From<L::Error>,
    {
        Track::open(&mut self.open, table, since)
    }

    fn track<P>(&mut self, table: &TableId) -> Result<&mut Track<L>, E>
    where
        L: SchemaChangeListener<P>,
        F: FnMut(&TableId) -> Result<L, E>,
        E: From<L::Error>,
    {
        let from = *self.window.from();
        let Self { open, tables, .. } = self;

        match tables.entry(table.clone()) {
            Entry::Occupied(track) => Ok(track.into_mut()),
            Entry::Vacant(vacant) => Ok(vacant.insert(Track::open(open, table, from)?)),
        }
    }

    fn alter<P>(
        &mut self,
        context: &L::Context,
        before: &TableModel,
        after: &TableModel,
        visit: impl FnOnce(&mut L) -> Result<(), L::Error>,
    ) -> Result<(), E>
    where
        L: SchemaChangeListener<P>,
        F: FnMut(&TableId) -> Result<L, E>,
        E: From<L::Error>,
    {
        let at = self.at;
        let track = self.track(after.id())?;

        track.advance(context, at, before)?;

        Ok(visit(&mut track.listener)?)
    }
}

impl<P, F, L, E> SchemaChangeListener<P> for EraTracker<F, L, E>
where
    F: FnMut(&TableId) -> Result<L, E>,
    L: SchemaChangeListener<P>,
    E: From<L::Error>,
{
    type Context = L::Context;
    type Error = E;

    fn visit_statement(
        &mut self,
        _: &Self::Context,
        change: &SchemaChange<P>,
    ) -> Result<(), Self::Error> {
        self.at = *change.recorded_at();

        Ok(())
    }

    fn visit_created(
        &mut self,
        context: &Self::Context,
        event: &TableCreated<'_>,
    ) -> Result<(), Self::Error> {
        let mut track = self.open(event.table().id(), self.at)?;

        track.listener.visit_created(context, event)?;
        self.tables.insert(event.table().id().clone(), track);

        Ok(())
    }

    fn visit_dropped(
        &mut self,
        context: &Self::Context,
        event: &TableDropped<'_>,
    ) -> Result<(), Self::Error> {
        match self.tables.remove(event.table().id()) {
            Some(mut track) => Ok(track.listener.visit_dropped(context, event)?),
            None => Ok(()),
        }
    }

    fn visit_column_added(
        &mut self,
        context: &Self::Context,
        event: &ColumnAdded<'_>,
    ) -> Result<(), Self::Error> {
        self.alter(context, event.before(), event.after(), |listener| {
            listener.visit_column_added(context, event)
        })
    }

    fn visit_column_dropped(
        &mut self,
        context: &Self::Context,
        event: &ColumnDropped<'_>,
    ) -> Result<(), Self::Error> {
        self.alter(context, event.before(), event.after(), |listener| {
            listener.visit_column_dropped(context, event)
        })
    }

    fn visit_column_retyped(
        &mut self,
        context: &Self::Context,
        event: &ColumnRetyped<'_>,
    ) -> Result<(), Self::Error> {
        self.alter(context, event.before(), event.after(), |listener| {
            listener.visit_column_retyped(context, event)
        })
    }

    fn visit_primary_key_changed(
        &mut self,
        context: &Self::Context,
        event: &PrimaryKeyChanged<'_>,
    ) -> Result<(), Self::Error> {
        self.alter(context, event.before(), event.after(), |listener| {
            listener.visit_primary_key_changed(context, event)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{SchemaChange, TableChange};
    use crate::listener::{ColumnAdded, ColumnDropped, OutOfOrder, TableCreated, TableDropped};
    use crate::model::{ColumnModel, ColumnType, TableModel};
    use crate::position::Position;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Offset(u64);

    impl Position for Offset {}

    #[derive(Debug, PartialEq, Eq)]
    enum Untold {
        OutOfOrder,
        Unopened(TableId),
    }

    impl From<OutOfOrder> for Untold {
        fn from(_: OutOfOrder) -> Self {
            Self::OutOfOrder
        }
    }

    #[derive(Debug)]
    struct Story {
        table: TableId,
        told: Vec<String>,
    }

    impl SchemaChangeListener<Offset> for Story {
        type Context = ();
        type Error = Untold;

        fn visit_era(&mut self, _: &(), era: &Era) -> Result<(), Untold> {
            self.told.push(format!(
                "era {:?} {}..{}",
                era.table()
                    .columns()
                    .map(|column| column.name())
                    .collect::<Vec<_>>(),
                era.window().from().milliseconds_since_epoch(),
                era.window().to().milliseconds_since_epoch()
            ));

            Ok(())
        }

        fn visit_created(&mut self, _: &(), event: &TableCreated<'_>) -> Result<(), Untold> {
            self.told.push(format!("created {}", event.table().id()));

            Ok(())
        }

        fn visit_dropped(&mut self, _: &(), event: &TableDropped<'_>) -> Result<(), Untold> {
            self.told.push(format!("dropped {}", event.table().id()));

            Ok(())
        }

        fn visit_column_added(&mut self, _: &(), event: &ColumnAdded<'_>) -> Result<(), Untold> {
            self.told.push(format!("added {}", event.column().name()));

            Ok(())
        }

        fn visit_column_dropped(
            &mut self,
            _: &(),
            event: &ColumnDropped<'_>,
        ) -> Result<(), Untold> {
            self.told.push(format!("dropped {}", event.column().name()));

            Ok(())
        }
    }

    fn people() -> TableId {
        TableId::new("shop", "people")
    }

    fn pets() -> TableId {
        TableId::new("shop", "pets")
    }

    fn table(id: TableId, columns: &[&str]) -> TableModel {
        TableModel::new(
            id,
            columns
                .iter()
                .map(|name| ColumnModel::new(name, "BIGINT", ColumnType::Int64, false, None))
                .collect(),
            vec!["id".to_owned()],
        )
    }

    fn change(at: i64, tables: Vec<TableChange>) -> SchemaChange<Offset> {
        SchemaChange::new(Offset(at as u64), RecordedAt::from(at), "", tables)
    }

    fn stories(
        before: Vec<SchemaChange<Offset>>,
        within: Vec<SchemaChange<Offset>>,
    ) -> Vec<(TableId, Vec<String>)> {
        let mut catalog = CatalogModel::default();
        for change in &before {
            catalog
                .accept::<OutOfOrder, _>(&(), change, &mut ())
                .unwrap();
        }
        let mut tracker = EraTracker::new(
            Window::new(RecordedAt::from(10), RecordedAt::from(20)),
            |table: &TableId| {
                Ok::<_, Untold>(Story {
                    table: table.clone(),
                    told: Vec::new(),
                })
            },
        );
        for change in &within {
            catalog
                .accept::<Untold, _>(&(), change, &mut tracker)
                .unwrap();
        }

        tracker
            .finish(&(), &catalog)
            .unwrap()
            .into_iter()
            .map(|story| (story.table, story.told))
            .collect()
    }

    #[test]
    fn a_table_no_schema_change_touches_lives_one_era_across_the_window() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::Created(table(people(), &["id"]))]
                )],
                vec![]
            ),
            [(people(), vec!["era [\"id\"] 10..20".to_owned()])]
        );
    }

    #[test]
    fn a_schema_change_tells_the_era_before_it_then_itself_then_the_era_after_it() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::Created(table(people(), &["id"]))]
                )],
                vec![change(
                    15,
                    vec![TableChange::Altered(table(people(), &["id", "name"]))]
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\"] 10..15".to_owned(),
                    "added name".to_owned(),
                    "era [\"id\", \"name\"] 15..20".to_owned()
                ]
            )]
        );
    }

    #[test]
    fn a_statement_that_changes_a_table_twice_ends_one_era() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::Created(table(people(), &["id"]))]
                )],
                vec![change(
                    15,
                    vec![TableChange::Altered(table(
                        people(),
                        &["id", "name", "born"]
                    ))]
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\"] 10..15".to_owned(),
                    "added name".to_owned(),
                    "added born".to_owned(),
                    "era [\"id\", \"name\", \"born\"] 15..20".to_owned()
                ]
            )]
        );
    }

    #[test]
    fn a_statement_that_steps_through_several_changes_ends_the_era_of_the_schema_before_it() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::Created(table(people(), &["id", "name"]))]
                )],
                vec![change(
                    15,
                    vec![TableChange::Altered(table(people(), &["id", "born"]))]
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\", \"name\"] 10..15".to_owned(),
                    "dropped name".to_owned(),
                    "added born".to_owned(),
                    "era [\"id\", \"born\"] 15..20".to_owned()
                ]
            )]
        );
    }

    #[test]
    fn a_table_created_within_the_window_lives_from_its_creation() {
        assert_eq!(
            stories(
                vec![],
                vec![change(
                    12,
                    vec![TableChange::Created(table(pets(), &["id"]))]
                )],
            ),
            [(
                pets(),
                vec![
                    "created shop.pets".to_owned(),
                    "era [\"id\"] 12..20".to_owned()
                ]
            )]
        );
    }

    #[test]
    fn a_table_dropped_within_the_window_is_told_nothing_more() {
        assert!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::Created(table(people(), &["id"]))]
                )],
                vec![change(15, vec![TableChange::Dropped(people())])],
            )
            .is_empty()
        );
    }

    #[test]
    fn a_table_dropped_and_created_again_lives_only_from_its_new_creation() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::Created(table(people(), &["id"]))]
                )],
                vec![
                    change(13, vec![TableChange::Dropped(people())]),
                    change(
                        16,
                        vec![TableChange::Created(table(people(), &["id", "name"]))]
                    ),
                ],
            ),
            [(
                people(),
                vec![
                    "created shop.people".to_owned(),
                    "era [\"id\", \"name\"] 16..20".to_owned()
                ]
            )]
        );
    }

    #[test]
    fn a_table_whose_listener_cannot_be_opened_fails_the_tracking() {
        let mut catalog = CatalogModel::default();
        catalog
            .accept::<OutOfOrder, _>(
                &(),
                &change(1, vec![TableChange::Created(table(people(), &["id"]))]),
                &mut (),
            )
            .unwrap();
        let tracker = EraTracker::new(
            Window::new(RecordedAt::from(10), RecordedAt::from(20)),
            |table: &TableId| Err::<Story, _>(Untold::Unopened(table.clone())),
        );

        assert_eq!(
            tracker.finish(&(), &catalog).unwrap_err(),
            Untold::Unopened(people())
        );
    }
}
