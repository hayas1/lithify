use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::marker::PhantomData;

use crate::event::SchemaChange;
use crate::listener::{
    Appearance, CatalogModel, SchemaChangeListener, TableAltered, TableCreated, TableDropped,
    TableRenamed,
};
use crate::model::{TableId, TableModel};
use crate::position::{Position, RecordedAt};
use crate::window::Window;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bound<P> {
    Time(RecordedAt),
    Position { at: RecordedAt, position: P },
}

impl<P> Bound<P> {
    pub fn instant(&self) -> RecordedAt {
        match self {
            Self::Time(at) | Self::Position { at, .. } => *at,
        }
    }

    pub fn as_position(&self) -> Option<&P> {
        match self {
            Self::Time(_) => None,
            Self::Position { position, .. } => Some(position),
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

pub struct EraTracker<F, L, E, P> {
    window: Window,
    at: Bound<P>,
    open: F,
    tables: BTreeMap<TableId, Track<L, P>>,
    dropped: Vec<L>,
    error: PhantomData<fn() -> E>,
}

struct Track<L, P> {
    listener: L,
    since: Bound<P>,
    appearance: Option<Appearance>,
}

impl<L, P: Position> Track<L, P> {
    fn open<E>(
        open: &mut impl FnMut(&TableId) -> Result<L, E>,
        table: &TableId,
        since: Bound<P>,
        appearance: Option<Appearance>,
    ) -> Result<Self, E> {
        Ok(Self {
            listener: open(table)?,
            since,
            appearance,
        })
    }

    fn is_created_within(&self) -> bool {
        self.appearance
            .is_some_and(|appearance| appearance.is_creation())
    }

    fn advance(
        &mut self,
        context: &L::Context,
        at: &Bound<P>,
        before: &TableModel,
    ) -> Result<(), L::Error>
    where
        L: SchemaChangeListener<P>,
    {
        if &self.since != at {
            let closed_era = Era::new(before.clone(), self.since.clone(), at.clone());

            self.listener.era_boundary(context, &closed_era)?;
            self.since = at.clone();
        }

        Ok(())
    }

    fn finish(
        mut self,
        context: &L::Context,
        table: &TableModel,
        to: &Bound<P>,
    ) -> Result<L, L::Error>
    where
        L: SchemaChangeListener<P>,
    {
        self.advance(context, to, table)?;

        Ok(self.listener)
    }
}

impl<F, L, E, P: Position> EraTracker<F, L, E, P> {
    pub fn new(window: Window, open: F) -> Self {
        Self {
            at: Bound::Time(*window.from()),
            window,
            open,
            tables: BTreeMap::new(),
            dropped: Vec::new(),
            error: PhantomData,
        }
    }

    pub fn finish(mut self, context: &L::Context, catalog: &CatalogModel<P>) -> Result<Vec<L>, E>
    where
        L: SchemaChangeListener<P>,
        F: FnMut(&TableId) -> Result<L, E>,
        E: From<L::Error>,
    {
        let standing = catalog
            .tables()
            .map(|table| {
                let track = match self.tables.remove(table.id()) {
                    Some(track) => track,
                    None => self.open(table.id(), Bound::Time(*self.window.from()), None)?,
                };

                Ok(track.finish(context, table, &Bound::Time(*self.window.to()))?)
            })
            .collect::<Result<Vec<_>, E>>()?;

        Ok(self.dropped.into_iter().chain(standing).collect())
    }

    fn open(
        &mut self,
        table: &TableId,
        since: Bound<P>,
        appearance: Option<Appearance>,
    ) -> Result<Track<L, P>, E>
    where
        L: SchemaChangeListener<P>,
        F: FnMut(&TableId) -> Result<L, E>,
        E: From<L::Error>,
    {
        Track::open(&mut self.open, table, since, appearance)
    }

    fn track(&mut self, table: &TableId) -> Result<&mut Track<L, P>, E>
    where
        L: SchemaChangeListener<P>,
        F: FnMut(&TableId) -> Result<L, E>,
        E: From<L::Error>,
    {
        let from = Bound::Time(*self.window.from());
        let Self { open, tables, .. } = self;

        match tables.entry(table.clone()) {
            Entry::Occupied(track) => Ok(track.into_mut()),
            Entry::Vacant(vacant) => Ok(vacant.insert(Track::open(open, table, from, None)?)),
        }
    }

    fn follow(&mut self, from: &TableId, to: &TableId) {
        if let Some(track) = self.tables.remove(from) {
            self.tables.insert(to.clone(), track);
        }
    }

    fn alter(
        &mut self,
        context: &L::Context,
        before: &TableModel,
        visit: impl FnOnce(&mut L) -> Result<(), L::Error>,
    ) -> Result<(), E>
    where
        L: SchemaChangeListener<P>,
        F: FnMut(&TableId) -> Result<L, E>,
        E: From<L::Error>,
    {
        let at = self.at.clone();
        let track = self.track(before.id())?;

        track.advance(context, &at, before)?;

        Ok(visit(&mut track.listener)?)
    }
}

impl<P, F, L, E> SchemaChangeListener<P> for EraTracker<F, L, E, P>
where
    P: Position,
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
        self.at = Bound::Position {
            at: *change.recorded_at(),
            position: change.position().clone(),
        };

        Ok(())
    }

    fn table_created(
        &mut self,
        context: &Self::Context,
        event: &TableCreated<'_>,
    ) -> Result<(), Self::Error> {
        let mut track = self.open(
            event.table().id(),
            self.at.clone(),
            Some(event.appearance()),
        )?;

        track.listener.table_created(context, event)?;
        self.tables.insert(event.table().id().clone(), track);

        Ok(())
    }

    fn table_dropped(
        &mut self,
        context: &Self::Context,
        event: &TableDropped<'_>,
    ) -> Result<(), Self::Error> {
        let mut track = match self.tables.remove(event.table().id()) {
            Some(track) => track,
            None => self.open(event.table().id(), Bound::Time(*self.window.from()), None)?,
        };

        track.listener.table_dropped(context, event)?;
        // A table created within the window and gone by its end left nothing to build or resume.
        if !track.is_created_within() {
            self.dropped.push(track.listener);
        }

        Ok(())
    }

    fn table_renamed(
        &mut self,
        context: &Self::Context,
        event: &TableRenamed<'_>,
    ) -> Result<(), Self::Error> {
        self.alter(context, event.before(), |listener| {
            listener.table_renamed(context, event)
        })?;
        self.follow(event.before().id(), event.after().id());

        Ok(())
    }

    fn table_altered(
        &mut self,
        context: &Self::Context,
        event: &TableAltered<'_>,
    ) -> Result<(), Self::Error> {
        self.alter(context, event.before(), |listener| {
            listener.table_altered(context, event)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Hint, SchemaChange, TableChange};
    use crate::listener::{
        Alteration, OutOfOrder, TableAltered, TableCreated, TableDropped, TableRenamed,
    };
    use crate::model::{ColumnModel, ColumnType, TableModel};
    use crate::position::Position;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Offset(u64);

    impl Position for Offset {}

    fn bound(bound: &Bound<Offset>) -> String {
        match bound {
            Bound::Time(at) => at.milliseconds_since_epoch().to_string(),
            Bound::Position {
                at,
                position: Offset(offset),
            } => format!("{}@{offset}", at.milliseconds_since_epoch()),
        }
    }

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

        fn era_boundary(&mut self, _: &(), closed_era: &Era<Offset>) -> Result<(), Untold> {
            self.told.push(format!(
                "era {:?} {}..{}",
                closed_era
                    .table()
                    .columns()
                    .map(|column| column.name())
                    .collect::<Vec<_>>(),
                bound(closed_era.from()),
                bound(closed_era.to())
            ));

            Ok(())
        }

        fn table_created(&mut self, _: &(), event: &TableCreated<'_>) -> Result<(), Untold> {
            self.told.push(format!("created {}", event.table().id()));

            Ok(())
        }

        fn table_dropped(&mut self, _: &(), event: &TableDropped<'_>) -> Result<(), Untold> {
            self.told.push(format!("dropped {}", event.table().id()));

            Ok(())
        }

        fn table_renamed(&mut self, _: &(), event: &TableRenamed<'_>) -> Result<(), Untold> {
            self.told.push(format!(
                "renamed {} to {}",
                event.before().id(),
                event.after().id()
            ));

            Ok(())
        }

        fn table_altered(&mut self, _: &(), event: &TableAltered<'_>) -> Result<(), Untold> {
            let alterations: Vec<String> = event
                .alterations()
                .iter()
                .map(Alteration::announcement)
                .collect();
            self.told
                .push(format!("altered: {}", alterations.join(", ")));

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
        hinted(at, Vec::new(), tables)
    }

    fn hinted(at: i64, hints: Vec<Hint>, tables: Vec<TableChange>) -> SchemaChange<Offset> {
        SchemaChange::new(Offset(at as u64), RecordedAt::from(at), "", tables, hints)
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
                    vec![TableChange::initialized(table(people(), &["id"]))]
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
                    vec![TableChange::initialized(table(people(), &["id"]))]
                )],
                vec![change(
                    15,
                    vec![TableChange::altered(table(people(), &["id", "name"]))]
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\"] 10..15@15".to_owned(),
                    "altered: added name".to_owned(),
                    "era [\"id\", \"name\"] 15@15..20".to_owned()
                ]
            )]
        );
    }

    #[test]
    fn a_schema_change_ends_an_era_after_its_instant_at_its_position() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::initialized(table(people(), &["id"]))]
                )],
                vec![SchemaChange::new(
                    Offset(99),
                    RecordedAt::from(15),
                    "",
                    vec![TableChange::altered(table(people(), &["id", "name"]))],
                    Vec::new(),
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\"] 10..15@99".to_owned(),
                    "altered: added name".to_owned(),
                    "era [\"id\", \"name\"] 15@99..20".to_owned()
                ]
            )]
        );
    }

    #[test]
    fn a_statement_that_changes_a_table_twice_ends_one_era_and_tells_both_changes_at_once() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::initialized(table(people(), &["id"]))]
                )],
                vec![change(
                    15,
                    vec![TableChange::altered(table(
                        people(),
                        &["id", "name", "born"]
                    ))]
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\"] 10..15@15".to_owned(),
                    "altered: added name, added born".to_owned(),
                    "era [\"id\", \"name\", \"born\"] 15@15..20".to_owned()
                ]
            )]
        );
    }

    #[test]
    fn a_statement_that_drops_and_adds_columns_ends_the_era_of_the_schema_before_it() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::initialized(table(people(), &["id", "name"]))]
                )],
                vec![change(
                    15,
                    vec![TableChange::altered(table(people(), &["id", "born"]))]
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\", \"name\"] 10..15@15".to_owned(),
                    "altered: dropped name, added born".to_owned(),
                    "era [\"id\", \"born\"] 15@15..20".to_owned()
                ]
            )]
        );
    }

    fn persons() -> TableId {
        TableId::new("shop", "persons")
    }

    #[test]
    fn a_renamed_column_ends_the_era_of_its_old_name() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::initialized(table(people(), &["id", "name"]))]
                )],
                vec![hinted(
                    15,
                    vec![Hint::ColumnRename {
                        table: people(),
                        from: "name".to_owned(),
                        to: "full_name".to_owned(),
                    }],
                    vec![TableChange::altered(table(people(), &["id", "full_name"]))]
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\", \"name\"] 10..15@15".to_owned(),
                    "altered: renamed name to full_name".to_owned(),
                    "era [\"id\", \"full_name\"] 15@15..20".to_owned()
                ]
            )]
        );
    }

    #[test]
    fn a_renamed_table_goes_on_with_the_listener_it_had_under_its_old_name() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::initialized(table(people(), &["id"]))]
                )],
                vec![hinted(
                    15,
                    vec![Hint::TableRename {
                        from: people(),
                        to: persons(),
                    }],
                    vec![TableChange::altered(table(persons(), &["id"]))]
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\"] 10..15@15".to_owned(),
                    "renamed shop.people to shop.persons".to_owned(),
                    "era [\"id\"] 15@15..20".to_owned()
                ]
            )]
        );
    }

    #[test]
    fn a_table_renamed_twice_within_the_window_goes_on_with_one_listener() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::initialized(table(people(), &["id"]))]
                )],
                vec![
                    hinted(
                        12,
                        vec![Hint::TableRename {
                            from: people(),
                            to: persons(),
                        }],
                        vec![]
                    ),
                    hinted(
                        15,
                        vec![Hint::TableRename {
                            from: persons(),
                            to: pets(),
                        }],
                        vec![]
                    ),
                ],
            ),
            [(
                people(),
                vec![
                    "era [\"id\"] 10..12@12".to_owned(),
                    "renamed shop.people to shop.persons".to_owned(),
                    "era [\"id\"] 12@12..15@15".to_owned(),
                    "renamed shop.persons to shop.pets".to_owned(),
                    "era [\"id\"] 15@15..20".to_owned()
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
                    vec![TableChange::initialized(table(pets(), &["id"]))]
                )],
            ),
            [(
                pets(),
                vec![
                    "created shop.pets".to_owned(),
                    "era [\"id\"] 12@12..20".to_owned()
                ]
            )]
        );
    }

    #[test]
    fn a_table_dropped_within_the_window_is_told_its_drop() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::initialized(table(people(), &["id"]))]
                )],
                vec![change(15, vec![TableChange::dropped(people())])],
            ),
            [(people(), vec!["dropped shop.people".to_owned()])]
        );
    }

    #[test]
    fn a_table_dropped_and_created_again_is_told_its_drop_apart_from_its_new_creation() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::initialized(table(people(), &["id"]))]
                )],
                vec![
                    change(13, vec![TableChange::dropped(people())]),
                    change(
                        16,
                        vec![TableChange::initialized(table(people(), &["id", "name"]))]
                    ),
                ],
            ),
            [
                (people(), vec!["dropped shop.people".to_owned()]),
                (
                    people(),
                    vec![
                        "created shop.people".to_owned(),
                        "era [\"id\", \"name\"] 16@16..20".to_owned()
                    ]
                )
            ]
        );
    }

    #[test]
    fn a_table_created_and_dropped_within_the_window_leaves_nothing_to_tell() {
        assert!(
            stories(
                vec![],
                vec![
                    change(12, vec![TableChange::initialized(table(pets(), &["id"]))]),
                    change(15, vec![TableChange::dropped(pets())]),
                ],
            )
            .is_empty()
        );
    }

    #[test]
    fn a_table_whose_listener_cannot_be_opened_fails_the_tracking() {
        let mut catalog = CatalogModel::default();
        catalog
            .accept::<OutOfOrder, _>(
                &(),
                &change(1, vec![TableChange::initialized(table(people(), &["id"]))]),
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

    #[test]
    fn a_bound_in_time_has_an_instant_and_no_position() {
        let bound = Bound::<Offset>::Time(RecordedAt::from(15));

        assert_eq!(bound.instant(), RecordedAt::from(15));
        assert_eq!(bound.as_position(), None);
    }

    #[test]
    fn a_bound_at_a_position_has_the_instant_it_was_recorded_at_and_its_position() {
        let bound = Bound::Position {
            at: RecordedAt::from(15),
            position: Offset(99),
        };

        assert_eq!(bound.instant(), RecordedAt::from(15));
        assert_eq!(bound.as_position(), Some(&Offset(99)));
    }

    #[test]
    fn a_schema_change_at_the_opening_of_the_window_ends_an_era_holding_nothing_before_it() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::initialized(table(people(), &["id"]))]
                )],
                vec![change(
                    10,
                    vec![TableChange::altered(table(people(), &["id", "name"]))]
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\"] 10..10@10".to_owned(),
                    "altered: added name".to_owned(),
                    "era [\"id\", \"name\"] 10@10..20".to_owned()
                ]
            )]
        );
    }
}
