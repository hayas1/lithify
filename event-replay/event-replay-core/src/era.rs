use std::collections::BTreeMap;

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
    tables: BTreeMap<TableId, Track<L, E>>,
}

struct Track<L, E> {
    listener: Result<L, E>,
    since: RecordedAt,
}

impl<L, E> Track<L, E> {
    fn open(
        open: &mut impl FnMut(&TableId) -> Result<L, E>,
        table: &TableId,
        since: RecordedAt,
    ) -> Self {
        Self {
            listener: open(table),
            since,
        }
    }

    fn tell(&mut self, visit: impl FnOnce(&mut L)) {
        if let Ok(listener) = &mut self.listener {
            visit(listener);
        }
    }

    fn advance<P>(&mut self, at: RecordedAt, before: &TableModel)
    where
        L: SchemaChangeListener<P>,
    {
        if self.since < at {
            let era = Era::new(before.clone(), Window::new(self.since, at));

            self.tell(|listener| listener.visit_era(&era));
            self.since = at;
        }
    }

    fn finish<P>(mut self, table: &TableModel, to: RecordedAt) -> Result<L, E>
    where
        L: SchemaChangeListener<P>,
    {
        self.advance(to, table);

        self.listener
    }
}

impl<F, L, E> EraTracker<F, L, E>
where
    F: FnMut(&TableId) -> Result<L, E>,
{
    pub fn new(window: Window, open: F) -> Self {
        Self {
            at: *window.from(),
            window,
            open,
            tables: BTreeMap::new(),
        }
    }

    pub fn finish<P: Position>(mut self, catalog: &CatalogModel<P>) -> Result<Vec<L>, E>
    where
        L: SchemaChangeListener<P>,
    {
        catalog
            .tables()
            .map(|table| {
                let track = self.tables.remove(table.id()).unwrap_or_else(|| {
                    Track::open(&mut self.open, table.id(), *self.window.from())
                });

                track.finish(table, *self.window.to())
            })
            .collect()
    }

    fn track(&mut self, table: &TableId) -> &mut Track<L, E> {
        let Self {
            window,
            open,
            tables,
            ..
        } = self;

        tables
            .entry(table.clone())
            .or_insert_with(|| Track::open(open, table, *window.from()))
    }

    fn alter<P>(&mut self, before: &TableModel, after: &TableModel, visit: impl FnOnce(&mut L))
    where
        L: SchemaChangeListener<P>,
    {
        let at = self.at;
        let track = self.track(after.id());

        track.advance(at, before);
        track.tell(visit);
    }
}

impl<P, F, L, E> SchemaChangeListener<P> for EraTracker<F, L, E>
where
    F: FnMut(&TableId) -> Result<L, E>,
    L: SchemaChangeListener<P>,
{
    fn visit_statement(&mut self, change: &SchemaChange<P>) {
        self.at = *change.recorded_at();
    }

    fn visit_created(&mut self, event: &TableCreated<'_>) {
        let mut track = Track::open(&mut self.open, event.table().id(), self.at);

        track.tell(|listener| listener.visit_created(event));
        self.tables.insert(event.table().id().clone(), track);
    }

    fn visit_dropped(&mut self, event: &TableDropped<'_>) {
        if let Some(mut track) = self.tables.remove(event.table().id()) {
            track.tell(|listener| listener.visit_dropped(event));
        }
    }

    fn visit_column_added(&mut self, event: &ColumnAdded<'_>) {
        self.alter(event.before(), event.after(), |listener| {
            listener.visit_column_added(event)
        });
    }

    fn visit_column_dropped(&mut self, event: &ColumnDropped<'_>) {
        self.alter(event.before(), event.after(), |listener| {
            listener.visit_column_dropped(event)
        });
    }

    fn visit_column_retyped(&mut self, event: &ColumnRetyped<'_>) {
        self.alter(event.before(), event.after(), |listener| {
            listener.visit_column_retyped(event)
        });
    }

    fn visit_primary_key_changed(&mut self, event: &PrimaryKeyChanged<'_>) {
        self.alter(event.before(), event.after(), |listener| {
            listener.visit_primary_key_changed(event)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{SchemaChange, TableChange};
    use crate::listener::{ColumnAdded, TableCreated, TableDropped};
    use crate::model::{ColumnModel, ColumnType, TableModel};
    use crate::position::Position;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Offset(u64);

    impl Position for Offset {}

    #[derive(Debug)]
    struct Story {
        table: TableId,
        told: Vec<String>,
    }

    impl SchemaChangeListener<Offset> for Story {
        fn visit_era(&mut self, era: &Era) {
            self.told.push(format!(
                "era {:?} {}..{}",
                era.table()
                    .columns()
                    .map(|column| column.name())
                    .collect::<Vec<_>>(),
                era.window().from().milliseconds_since_epoch(),
                era.window().to().milliseconds_since_epoch()
            ));
        }

        fn visit_created(&mut self, event: &TableCreated<'_>) {
            self.told.push(format!("created {}", event.table().id()));
        }

        fn visit_dropped(&mut self, event: &TableDropped<'_>) {
            self.told.push(format!("dropped {}", event.table().id()));
        }

        fn visit_column_added(&mut self, event: &ColumnAdded<'_>) {
            self.told.push(format!("added {}", event.column().name()));
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
                .enumerate()
                .map(|(at, name)| {
                    ColumnModel::new(
                        name,
                        "BIGINT",
                        ColumnType::Int64,
                        at as u32 + 1,
                        false,
                        None,
                    )
                })
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
            catalog.accept(change, &mut ()).unwrap();
        }
        let mut tracker = EraTracker::new(
            Window::new(RecordedAt::from(10), RecordedAt::from(20)),
            |table: &TableId| {
                Ok::<_, ()>(Story {
                    table: table.clone(),
                    told: Vec::new(),
                })
            },
        );
        for change in &within {
            catalog.accept(change, &mut tracker).unwrap();
        }

        tracker
            .finish(&catalog)
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
            .accept(
                &change(1, vec![TableChange::Created(table(people(), &["id"]))]),
                &mut (),
            )
            .unwrap();
        let tracker = EraTracker::new(
            Window::new(RecordedAt::from(10), RecordedAt::from(20)),
            |table: &TableId| Err::<Story, _>(table.clone()),
        );

        assert_eq!(tracker.finish(&catalog).unwrap_err(), people());
    }
}
