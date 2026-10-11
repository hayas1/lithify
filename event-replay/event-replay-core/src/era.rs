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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lineage {
    table: TableModel,
    eras: Vec<Era>,
}

impl Lineage {
    pub fn new(table: TableModel, eras: Vec<Era>) -> Self {
        Self { table, eras }
    }

    pub fn table(&self) -> &TableModel {
        &self.table
    }

    pub fn eras(&self) -> impl Iterator<Item = &Era> {
        self.eras.iter()
    }

    pub fn first(&self) -> Option<&Era> {
        self.eras.first()
    }
}

pub struct EraTracker {
    window: Window,
    at: RecordedAt,
    tables: BTreeMap<TableId, Track>,
}

struct Track {
    closed: Vec<Era>,
    table: TableModel,
    since: RecordedAt,
}

impl Track {
    fn new(table: TableModel, since: RecordedAt) -> Self {
        Self {
            closed: Vec::new(),
            table,
            since,
        }
    }

    fn advance(&mut self, at: RecordedAt, table: &TableModel) {
        if self.since != at {
            self.closed
                .push(Era::new(self.table.clone(), Window::new(self.since, at)));
            self.since = at;
        }
        self.table = table.clone();
    }

    fn close(self, to: RecordedAt) -> Vec<Era> {
        let mut eras = self.closed;
        eras.push(Era::new(self.table, Window::new(self.since, to)));

        eras
    }
}

impl EraTracker {
    pub fn new(window: Window) -> Self {
        Self {
            at: *window.from(),
            window,
            tables: BTreeMap::new(),
        }
    }

    pub fn lineages<P: Position>(mut self, catalog: &CatalogModel<P>) -> Vec<Lineage> {
        catalog
            .tables()
            .map(|table| {
                let eras = match self.tables.remove(table.id()) {
                    Some(track) => track.close(*self.window.to()),
                    None => vec![Era::new(table.clone(), self.window.clone())],
                };

                Lineage::new(table.clone(), eras)
            })
            .collect()
    }

    fn alter(&mut self, before: &TableModel, after: &TableModel) {
        let from = *self.window.from();

        self.tables
            .entry(after.id().clone())
            .or_insert_with(|| Track::new(before.clone(), from))
            .advance(self.at, after);
    }
}

impl<P> SchemaChangeListener<P> for EraTracker {
    fn visit_statement(&mut self, change: &SchemaChange<P>) {
        self.at = *change.recorded_at();
    }

    fn visit_created(&mut self, event: &TableCreated<'_>) {
        self.tables.insert(
            event.table().id().clone(),
            Track::new(event.table().clone(), self.at),
        );
    }

    fn visit_dropped(&mut self, event: &TableDropped<'_>) {
        self.tables.remove(event.table().id());
    }

    fn visit_column_added(&mut self, event: &ColumnAdded<'_>) {
        self.alter(event.before(), event.after());
    }

    fn visit_column_dropped(&mut self, event: &ColumnDropped<'_>) {
        self.alter(event.before(), event.after());
    }

    fn visit_column_retyped(&mut self, event: &ColumnRetyped<'_>) {
        self.alter(event.before(), event.after());
    }

    fn visit_primary_key_changed(&mut self, event: &PrimaryKeyChanged<'_>) {
        self.alter(event.before(), event.after());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{SchemaChange, TableChange};
    use crate::listener::CatalogModel;
    use crate::model::{ColumnModel, ColumnType, TableId, TableModel};
    use crate::position::{Position, RecordedAt};
    use crate::window::Window;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Offset(u64);

    impl Position for Offset {}

    fn people() -> TableId {
        TableId::new("shop", "people")
    }

    fn pets() -> TableId {
        TableId::new("shop", "pets")
    }

    fn column(name: &str, position: u32) -> ColumnModel {
        ColumnModel::new(name, "BIGINT", ColumnType::Int64, position, false, None)
    }

    fn table(id: TableId, columns: &[&str]) -> TableModel {
        TableModel::new(
            id,
            columns
                .iter()
                .enumerate()
                .map(|(at, name)| column(name, at as u32 + 1))
                .collect(),
            vec!["id".to_owned()],
        )
    }

    fn change(at: i64, tables: Vec<TableChange>) -> SchemaChange<Offset> {
        SchemaChange::new(Offset(at as u64), RecordedAt::from(at), "", tables)
    }

    fn window(from: i64, to: i64) -> Window {
        Window::new(RecordedAt::from(from), RecordedAt::from(to))
    }

    fn lineages(
        before: Vec<SchemaChange<Offset>>,
        within: Vec<SchemaChange<Offset>>,
    ) -> Vec<Lineage> {
        let mut catalog = CatalogModel::default();
        for change in &before {
            catalog.accept(change, &mut ()).unwrap();
        }
        let mut tracker = EraTracker::new(window(10, 20));
        for change in &within {
            catalog.accept(change, &mut tracker).unwrap();
        }

        tracker.lineages(&catalog)
    }

    fn spans(lineage: &Lineage) -> Vec<(Vec<String>, i64, i64)> {
        lineage
            .eras()
            .map(|era| {
                (
                    era.table()
                        .columns()
                        .map(|column| column.name().to_owned())
                        .collect(),
                    era.window().from().milliseconds_since_epoch(),
                    era.window().to().milliseconds_since_epoch(),
                )
            })
            .collect()
    }

    #[test]
    fn a_table_no_schema_change_touches_lives_one_era_across_the_window() {
        let lineages = lineages(
            vec![change(
                1,
                vec![TableChange::Created(table(people(), &["id"]))],
            )],
            vec![],
        );

        assert_eq!(lineages.len(), 1);
        assert_eq!(lineages[0].table().id(), &people());
        assert_eq!(spans(&lineages[0]), [(vec!["id".to_owned()], 10, 20)]);
    }

    #[test]
    fn a_schema_change_within_the_window_closes_one_era_and_opens_the_next() {
        let lineages = lineages(
            vec![change(
                1,
                vec![TableChange::Created(table(people(), &["id"]))],
            )],
            vec![change(
                15,
                vec![TableChange::Altered(table(people(), &["id", "name"]))],
            )],
        );

        assert_eq!(
            spans(&lineages[0]),
            [
                (vec!["id".to_owned()], 10, 15),
                (vec!["id".to_owned(), "name".to_owned()], 15, 20)
            ]
        );
        assert_eq!(lineages[0].table().columns().count(), 2);
    }

    #[test]
    fn a_statement_that_changes_a_table_twice_opens_one_era() {
        let lineages = lineages(
            vec![change(
                1,
                vec![TableChange::Created(table(people(), &["id"]))],
            )],
            vec![change(
                15,
                vec![TableChange::Altered(table(
                    people(),
                    &["id", "name", "born"],
                ))],
            )],
        );

        assert_eq!(
            spans(&lineages[0]),
            [
                (vec!["id".to_owned()], 10, 15),
                (
                    vec!["id".to_owned(), "name".to_owned(), "born".to_owned()],
                    15,
                    20
                )
            ]
        );
    }

    #[test]
    fn a_table_created_within_the_window_lives_from_its_creation() {
        let lineages = lineages(
            vec![],
            vec![change(
                12,
                vec![TableChange::Created(table(pets(), &["id"]))],
            )],
        );

        assert_eq!(spans(&lineages[0]), [(vec!["id".to_owned()], 12, 20)]);
    }

    #[test]
    fn a_table_dropped_within_the_window_has_no_lineage() {
        let lineages = lineages(
            vec![change(
                1,
                vec![TableChange::Created(table(people(), &["id"]))],
            )],
            vec![change(15, vec![TableChange::Dropped(people())])],
        );

        assert!(lineages.is_empty());
    }

    #[test]
    fn a_table_dropped_and_created_again_lives_only_from_its_new_creation() {
        let lineages = lineages(
            vec![change(
                1,
                vec![TableChange::Created(table(people(), &["id"]))],
            )],
            vec![
                change(13, vec![TableChange::Dropped(people())]),
                change(
                    16,
                    vec![TableChange::Created(table(people(), &["id", "name"]))],
                ),
            ],
        );

        assert_eq!(
            spans(&lineages[0]),
            [(vec!["id".to_owned(), "name".to_owned()], 16, 20)]
        );
    }
}
