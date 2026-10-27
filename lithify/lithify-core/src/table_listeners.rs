use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::convert::Infallible;

use crate::listener::{
    Appearance, CatalogModel, Finish, ReplayEnd, SchemaChangeListener, TableAltered, TableCreated,
    TableDropped, TableRenamed,
};
use crate::model::{TableId, TableModel};
use crate::position::Position;

pub struct TableListeners<F, G, L, E> {
    make_listener: F,
    finishes: G,
    standing: BTreeMap<TableId, Track<L, E>>,
    dropped: Vec<(TableId, Track<L, E>)>,
}

// One table as the listeners follow it: its listener and, once that listener has refused a
// schema change, why, so the table stops alone while the others go on.
struct Track<L, E> {
    listener: L,
    appearance: Option<Appearance>,
    refusal: Option<E>,
}

impl<L, E> Track<L, E> {
    fn new(listener: L, appearance: Option<Appearance>) -> Self {
        Self {
            listener,
            appearance,
            refusal: None,
        }
    }

    fn tell(&mut self, visit: impl FnOnce(&mut L) -> Result<(), E>) {
        if self.refusal.is_none() {
            self.refusal = visit(&mut self.listener).err();
        }
    }

    fn is_created_within(&self) -> bool {
        self.appearance
            .is_some_and(|appearance| appearance.is_creation())
    }

    fn finish<P>(
        self,
        context: &L::Context,
        table: TableId,
        standing: Option<TableModel>,
    ) -> Result<L::Output, L::FinishError>
    where
        L: Finish<P, Error = E, Input = ReplayEnd<E>>,
    {
        let end = match standing {
            Some(model) => ReplayEnd::standing(model, self.refusal),
            None => ReplayEnd::dropped(table, self.refusal),
        };

        self.listener.finish(context, end)
    }
}

impl<F, G, L, E> TableListeners<F, G, L, E> {
    pub fn new(make_listener: F, finishes: G) -> Self {
        Self {
            make_listener,
            finishes,
            standing: BTreeMap::new(),
            dropped: Vec::new(),
        }
    }

    fn follow(&mut self, from: &TableId, to: &TableId) {
        if let Some(track) = self.standing.remove(from) {
            self.standing.insert(to.clone(), track);
        }
    }
}

impl<F, G, L, E> TableListeners<F, G, L, E>
where
    F: FnMut(&TableId) -> L,
{
    fn track(&mut self, table: &TableId) -> &mut Track<L, E> {
        let Self {
            make_listener,
            standing,
            ..
        } = self;

        match standing.entry(table.clone()) {
            Entry::Occupied(track) => track.into_mut(),
            Entry::Vacant(vacant) => vacant.insert(Track::new(make_listener(table), None)),
        }
    }
}

impl<P, F, G, L, E> SchemaChangeListener<P> for TableListeners<F, G, L, E>
where
    P: Position,
    F: FnMut(&TableId) -> L,
    L: SchemaChangeListener<P, Error = E>,
{
    type Context = L::Context;
    type Error = Infallible;

    fn table_created(
        &mut self,
        context: &Self::Context,
        event: &TableCreated<'_, P>,
    ) -> Result<(), Self::Error> {
        let mut track = Track::new(
            (self.make_listener)(event.table().id()),
            Some(event.appearance()),
        );

        track.tell(|listener| listener.table_created(context, event));
        self.standing.insert(event.table().id().clone(), track);

        Ok(())
    }

    fn table_dropped(
        &mut self,
        context: &Self::Context,
        event: &TableDropped<'_, P>,
    ) -> Result<(), Self::Error> {
        let table = event.table().id();
        let mut track = self
            .standing
            .remove(table)
            .unwrap_or_else(|| Track::new((self.make_listener)(table), None));

        track.tell(|listener| listener.table_dropped(context, event));
        // A table created within the window and gone by its end left nothing to build or resume.
        if !track.is_created_within() {
            self.dropped.push((table.clone(), track));
        }

        Ok(())
    }

    fn table_renamed(
        &mut self,
        context: &Self::Context,
        event: &TableRenamed<'_, P>,
    ) -> Result<(), Self::Error> {
        self.track(event.before().id())
            .tell(|listener| listener.table_renamed(context, event));
        self.follow(event.before().id(), event.after().id());

        Ok(())
    }

    fn table_altered(
        &mut self,
        context: &Self::Context,
        event: &TableAltered<'_, P>,
    ) -> Result<(), Self::Error> {
        self.track(event.before().id())
            .tell(|listener| listener.table_altered(context, event));

        Ok(())
    }
}

impl<P, F, G, L, E> Finish<P> for TableListeners<F, G, L, E>
where
    P: Position,
    F: FnMut(&TableId) -> L,
    G: Fn(&TableId) -> bool,
    L: Finish<P, Error = E, Input = ReplayEnd<E>>,
{
    type Input = CatalogModel<P>;
    type Output = Vec<L::Output>;
    type FinishError = L::FinishError;

    fn finish(
        mut self,
        context: &Self::Context,
        mut catalog: CatalogModel<P>,
    ) -> Result<Self::Output, Self::FinishError> {
        let standing: Vec<_> = catalog.tables().map(|table| table.id().clone()).collect();
        let dropped = std::mem::take(&mut self.dropped)
            .into_iter()
            .filter(|(table, _)| (self.finishes)(table))
            .map(|(table, track)| track.finish(context, table, None))
            .collect::<Result<Vec<_>, _>>()?;
        let finished = standing
            .into_iter()
            .filter(|table| (self.finishes)(table))
            .map(|table| {
                let track = self
                    .standing
                    .remove(&table)
                    .unwrap_or_else(|| Track::new((self.make_listener)(&table), None));

                let standing = catalog.remove(&table);

                track.finish(context, table, standing)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(dropped.into_iter().chain(finished).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::era::{Bound, Era, EraTracker};
    use crate::event::{Hint, SchemaChange, TableChange};
    use crate::listener::{Alteration, OutOfOrder};
    use crate::model::{ColumnModel, ColumnType, TableModel};
    use crate::ts::Ts;
    use crate::window::Window;

    use crate::testing::Offset;

    fn bound(bound: &Bound<Offset>) -> String {
        match bound {
            Bound::EventTime(at) => at.milliseconds_since_epoch().to_string(),
            Bound::Position {
                at,
                position: Offset(offset),
            } => format!("{}@{offset}", at.milliseconds_since_epoch()),
            Bound::Open => "open".to_owned(),
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Untold {
        Refused(TableId),
    }

    #[derive(Debug)]
    struct Story {
        table: TableId,
        eras: EraTracker<Offset>,
        refusing: bool,
        told: Vec<String>,
    }

    impl Story {
        fn tell(&mut self, era: &Era<Offset>) {
            self.told.push(format!(
                "era {:?} {}..{}",
                era.table()
                    .columns()
                    .map(|column| column.name())
                    .collect::<Vec<_>>(),
                bound(era.from()),
                bound(era.to())
            ));
        }
    }

    impl SchemaChangeListener<Offset> for Story {
        type Context = ();
        type Error = Untold;

        fn table_created(
            &mut self,
            _: &(),
            event: &TableCreated<'_, Offset>,
        ) -> Result<(), Untold> {
            self.eras = EraTracker::since(Bound::from(event.change()));
            self.told.push(format!("created {}", event.table().id()));

            Ok(())
        }

        fn table_dropped(
            &mut self,
            _: &(),
            event: &TableDropped<'_, Offset>,
        ) -> Result<(), Untold> {
            self.told.push(format!("dropped {}", event.table().id()));

            Ok(())
        }

        fn table_renamed(
            &mut self,
            _: &(),
            event: &TableRenamed<'_, Offset>,
        ) -> Result<(), Untold> {
            if let Some(era) = self.eras.renamed(event) {
                self.tell(&era);
            }
            self.told.push(format!(
                "renamed {} to {}",
                event.before().id(),
                event.after().id()
            ));

            Ok(())
        }

        fn table_altered(
            &mut self,
            _: &(),
            event: &TableAltered<'_, Offset>,
        ) -> Result<(), Untold> {
            if self.refusing {
                return Err(Untold::Refused(self.table.clone()));
            }
            if let Some(era) = self.eras.altered(event) {
                self.tell(&era);
            }
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

    impl Finish<Offset> for Story {
        type Input = ReplayEnd<Untold>;
        type Output = (TableId, Vec<String>, Option<Untold>);
        type FinishError = Infallible;

        fn finish(mut self, _: &(), finish: ReplayEnd<Untold>) -> Result<Self::Output, Infallible> {
            if let (None, Some(table)) = (finish.refusal(), finish.model()) {
                let last_era = self.eras.last(table, Bound::Open);
                self.tell(&last_era);
            }

            Ok((self.table, self.told, finish.refusal().cloned()))
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
            ["id"].into(),
        )
    }

    fn ten_to_twenty() -> Window {
        Window::new(Ts::try_from(10).unwrap(), Ts::try_from(20).unwrap())
    }

    fn change(at: i64, tables: Vec<TableChange>) -> SchemaChange<Offset> {
        hinted(at, Vec::new(), tables)
    }

    fn hinted(at: i64, hints: Vec<Hint>, tables: Vec<TableChange>) -> SchemaChange<Offset> {
        SchemaChange::new(
            Offset(at as u64),
            Ts::try_from(at).unwrap(),
            "",
            tables,
            hints,
        )
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
        let mut tracker = TableListeners::new(
            |table: &TableId| Story {
                table: table.clone(),
                eras: EraTracker::since(Bound::EventTime(*ten_to_twenty().from())),
                refusing: false,
                told: Vec::new(),
            },
            |_: &TableId| true,
        );
        for change in &within {
            catalog
                .accept::<OutOfOrder, _>(&(), change, &mut tracker)
                .unwrap();
        }

        tracker
            .finish(&(), catalog)
            .unwrap()
            .into_iter()
            .map(|(table, told, _)| (table, told))
            .collect()
    }

    #[test]
    fn a_table_whose_listener_refuses_a_schema_change_stops_alone_while_the_others_go_on() {
        let mut catalog = CatalogModel::default();
        catalog
            .accept::<OutOfOrder, _>(
                &(),
                &change(
                    1,
                    vec![
                        TableChange::Created(table(people(), &["id"])),
                        TableChange::Created(table(pets(), &["id"])),
                    ],
                ),
                &mut (),
            )
            .unwrap();
        let mut listeners = TableListeners::new(
            |table: &TableId| Story {
                table: table.clone(),
                eras: EraTracker::since(Bound::EventTime(*ten_to_twenty().from())),
                refusing: *table == people(),
                told: Vec::new(),
            },
            |_: &TableId| true,
        );
        for change in [
            change(
                15,
                vec![TableChange::Altered(table(people(), &["id", "name"]))],
            ),
            change(
                16,
                vec![TableChange::Altered(table(pets(), &["id", "name"]))],
            ),
            change(
                17,
                vec![TableChange::Altered(table(
                    people(),
                    &["id", "name", "born"],
                ))],
            ),
        ] {
            catalog
                .accept::<OutOfOrder, _>(&(), &change, &mut listeners)
                .unwrap();
        }

        assert_eq!(
            listeners.finish(&(), catalog).unwrap(),
            [
                (people(), vec![], Some(Untold::Refused(people()))),
                (
                    pets(),
                    vec![
                        "era [\"id\"] 10..16@16".to_owned(),
                        "altered: added name".to_owned(),
                        "era [\"id\", \"name\"] 16@16..open".to_owned()
                    ],
                    None
                ),
            ]
        );
    }

    #[test]
    fn a_table_the_listeners_are_told_to_leave_out_is_not_finished() {
        let mut catalog = CatalogModel::default();
        catalog
            .accept::<OutOfOrder, _>(
                &(),
                &change(
                    1,
                    vec![
                        TableChange::Created(table(people(), &["id"])),
                        TableChange::Created(table(pets(), &["id"])),
                    ],
                ),
                &mut (),
            )
            .unwrap();
        let listeners = TableListeners::new(
            |table: &TableId| Story {
                table: table.clone(),
                eras: EraTracker::since(Bound::EventTime(*ten_to_twenty().from())),
                refusing: false,
                told: Vec::new(),
            },
            |table: &TableId| *table != pets(),
        );

        assert_eq!(
            listeners
                .finish(&(), catalog)
                .unwrap()
                .into_iter()
                .map(|(table, ..)| table)
                .collect::<Vec<_>>(),
            [people()]
        );
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
            [(people(), vec!["era [\"id\"] 10..open".to_owned()])]
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
                    "era [\"id\"] 10..15@15".to_owned(),
                    "altered: added name".to_owned(),
                    "era [\"id\", \"name\"] 15@15..open".to_owned()
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
                    vec![TableChange::Created(table(people(), &["id"]))]
                )],
                vec![SchemaChange::new(
                    Offset(99),
                    Ts::try_from(15).unwrap(),
                    "",
                    vec![TableChange::Altered(table(people(), &["id", "name"]))],
                    Vec::new(),
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\"] 10..15@99".to_owned(),
                    "altered: added name".to_owned(),
                    "era [\"id\", \"name\"] 15@99..open".to_owned()
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
                    "era [\"id\"] 10..15@15".to_owned(),
                    "altered: added name, added born".to_owned(),
                    "era [\"id\", \"name\", \"born\"] 15@15..open".to_owned()
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
                    "era [\"id\", \"name\"] 10..15@15".to_owned(),
                    "altered: dropped name, added born".to_owned(),
                    "era [\"id\", \"born\"] 15@15..open".to_owned()
                ]
            )]
        );
    }

    #[test]
    fn a_statement_that_only_describes_columns_ends_no_era() {
        let described = TableModel::new(
            people(),
            vec![
                ColumnModel::new("id", "BIGINT", ColumnType::Int64, false, None)
                    .with_comment(Some("who they are")),
            ],
            ["id"].into(),
        );

        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::Created(table(people(), &["id"]))]
                )],
                vec![change(15, vec![TableChange::Altered(described)])],
            ),
            [(
                people(),
                vec![
                    "altered: described id".to_owned(),
                    "era [\"id\"] 10..open".to_owned()
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
                    vec![TableChange::Created(table(people(), &["id", "name"]))]
                )],
                vec![hinted(
                    15,
                    vec![Hint::ColumnRename {
                        table: people(),
                        from: "name".into(),
                        to: "full_name".into(),
                    }],
                    vec![TableChange::Altered(table(people(), &["id", "full_name"]))]
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\", \"name\"] 10..15@15".to_owned(),
                    "altered: renamed name to full_name".to_owned(),
                    "era [\"id\", \"full_name\"] 15@15..open".to_owned()
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
                    vec![TableChange::Created(table(people(), &["id"]))]
                )],
                vec![hinted(
                    15,
                    vec![Hint::TableRename {
                        from: people(),
                        to: persons(),
                    }],
                    vec![TableChange::Altered(table(persons(), &["id"]))]
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\"] 10..15@15".to_owned(),
                    "renamed shop.people to shop.persons".to_owned(),
                    "era [\"id\"] 15@15..open".to_owned()
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
                    vec![TableChange::Created(table(people(), &["id"]))]
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
                    "era [\"id\"] 15@15..open".to_owned()
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
                    "era [\"id\"] 12@12..open".to_owned()
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
                    vec![TableChange::Created(table(people(), &["id"]))]
                )],
                vec![change(15, vec![TableChange::Dropped(people())])],
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
            [
                (people(), vec!["dropped shop.people".to_owned()]),
                (
                    people(),
                    vec![
                        "created shop.people".to_owned(),
                        "era [\"id\", \"name\"] 16@16..open".to_owned()
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
                    change(12, vec![TableChange::Created(table(pets(), &["id"]))]),
                    change(15, vec![TableChange::Dropped(pets())]),
                ],
            )
            .is_empty()
        );
    }

    #[test]
    fn a_schema_change_at_the_opening_of_the_window_ends_an_era_holding_nothing_before_it() {
        assert_eq!(
            stories(
                vec![change(
                    1,
                    vec![TableChange::Created(table(people(), &["id"]))]
                )],
                vec![change(
                    10,
                    vec![TableChange::Altered(table(people(), &["id", "name"]))]
                )],
            ),
            [(
                people(),
                vec![
                    "era [\"id\"] 10..10@10".to_owned(),
                    "altered: added name".to_owned(),
                    "era [\"id\", \"name\"] 10@10..open".to_owned()
                ]
            )]
        );
    }
}
