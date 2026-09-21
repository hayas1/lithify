use std::collections::BTreeMap;
use std::fmt;

use crate::event::{SchemaChange, TableChange};
use crate::model::{ColumnModel, TableId, TableModel};
use crate::position::Position;

pub trait SchemaChangeListener<P> {
    fn accept(&mut self, change: &SchemaChange<P>);
}

pub struct TableCreated<'a, P> {
    change: &'a SchemaChange<P>,
    table: &'a TableModel,
}

impl<'a, P> TableCreated<'a, P> {
    pub fn change(&self) -> &'a SchemaChange<P> {
        self.change
    }

    pub fn table(&self) -> &'a TableModel {
        self.table
    }
}

pub struct TableDropped<'a, P> {
    change: &'a SchemaChange<P>,
    table: &'a TableModel,
}

impl<'a, P> TableDropped<'a, P> {
    pub fn change(&self) -> &'a SchemaChange<P> {
        self.change
    }

    pub fn table(&self) -> &'a TableModel {
        self.table
    }
}

pub struct ColumnAdded<'a, P> {
    change: &'a SchemaChange<P>,
    before: &'a TableModel,
    after: &'a TableModel,
    column: &'a ColumnModel,
}

impl<'a, P> ColumnAdded<'a, P> {
    pub fn change(&self) -> &'a SchemaChange<P> {
        self.change
    }

    pub fn before(&self) -> &'a TableModel {
        self.before
    }

    pub fn after(&self) -> &'a TableModel {
        self.after
    }

    pub fn column(&self) -> &'a ColumnModel {
        self.column
    }
}

pub struct ColumnDropped<'a, P> {
    change: &'a SchemaChange<P>,
    before: &'a TableModel,
    after: &'a TableModel,
    column: &'a ColumnModel,
}

impl<'a, P> ColumnDropped<'a, P> {
    pub fn change(&self) -> &'a SchemaChange<P> {
        self.change
    }

    pub fn before(&self) -> &'a TableModel {
        self.before
    }

    pub fn after(&self) -> &'a TableModel {
        self.after
    }

    pub fn column(&self) -> &'a ColumnModel {
        self.column
    }
}

pub struct ColumnRetyped<'a, P> {
    change: &'a SchemaChange<P>,
    before: &'a TableModel,
    after: &'a TableModel,
    previous: &'a ColumnModel,
    column: &'a ColumnModel,
}

impl<'a, P> ColumnRetyped<'a, P> {
    pub fn change(&self) -> &'a SchemaChange<P> {
        self.change
    }

    pub fn before(&self) -> &'a TableModel {
        self.before
    }

    pub fn after(&self) -> &'a TableModel {
        self.after
    }

    pub fn previous(&self) -> &'a ColumnModel {
        self.previous
    }

    pub fn column(&self) -> &'a ColumnModel {
        self.column
    }
}

pub struct PrimaryKeyChanged<'a, P> {
    change: &'a SchemaChange<P>,
    before: &'a TableModel,
    after: &'a TableModel,
}

impl<'a, P> PrimaryKeyChanged<'a, P> {
    pub fn change(&self) -> &'a SchemaChange<P> {
        self.change
    }

    pub fn before(&self) -> &'a TableModel {
        self.before
    }

    pub fn after(&self) -> &'a TableModel {
        self.after
    }
}

pub enum TableEvent<'a, P> {
    Created(TableCreated<'a, P>),
    Dropped(TableDropped<'a, P>),
    ColumnAdded(ColumnAdded<'a, P>),
    ColumnDropped(ColumnDropped<'a, P>),
    ColumnRetyped(ColumnRetyped<'a, P>),
    PrimaryKeyChanged(PrimaryKeyChanged<'a, P>),
}

impl<'a, P> TableEvent<'a, P> {
    pub fn change(&self) -> &'a SchemaChange<P> {
        match self {
            Self::Created(event) => event.change(),
            Self::Dropped(event) => event.change(),
            Self::ColumnAdded(event) => event.change(),
            Self::ColumnDropped(event) => event.change(),
            Self::ColumnRetyped(event) => event.change(),
            Self::PrimaryKeyChanged(event) => event.change(),
        }
    }

    pub fn table(&self) -> &'a TableId {
        match self {
            Self::Created(event) => event.table().id(),
            Self::Dropped(event) => event.table().id(),
            Self::ColumnAdded(event) => event.after().id(),
            Self::ColumnDropped(event) => event.after().id(),
            Self::ColumnRetyped(event) => event.after().id(),
            Self::PrimaryKeyChanged(event) => event.after().id(),
        }
    }
}

pub trait TableListener<P> {
    fn accept(&mut self, event: &TableEvent<'_, P>);
}

#[derive(Debug, PartialEq, Eq)]
pub struct OutOfOrder {
    pub table: TableId,
}

impl fmt::Display for OutOfOrder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "a schema change older than the one already applied to {}",
            self.table
        )
    }
}

impl std::error::Error for OutOfOrder {}

#[derive(Debug)]
pub struct CatalogModel<P> {
    tables: BTreeMap<TableId, TableModel>,
    applied_at: BTreeMap<TableId, P>,
}

impl<P> Default for CatalogModel<P> {
    fn default() -> Self {
        Self {
            tables: BTreeMap::new(),
            applied_at: BTreeMap::new(),
        }
    }
}

impl<P: Position> CatalogModel<P> {
    pub fn table(&self, id: &TableId) -> Option<&TableModel> {
        self.tables.get(id)
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableModel> {
        self.tables.values()
    }

    pub fn accept(
        &mut self,
        change: &SchemaChange<P>,
        listener: &mut impl TableListener<P>,
    ) -> Result<(), OutOfOrder> {
        for table in change.tables() {
            self.guard_order(table.id(), change.position())?;
        }

        for table in change.tables() {
            match table {
                TableChange::Created(table) => self.create(change, table, listener),
                TableChange::Altered(table) => self.alter(change, table, listener),
                TableChange::Dropped(id) => self.forget(change, id, listener),
            }
        }

        Ok(())
    }

    fn guard_order(&self, table: &TableId, position: &P) -> Result<(), OutOfOrder> {
        match self.applied_at.get(table) {
            Some(applied) if applied > position => Err(OutOfOrder {
                table: table.clone(),
            }),
            _ => Ok(()),
        }
    }

    fn create(
        &mut self,
        change: &SchemaChange<P>,
        table: &TableModel,
        listener: &mut impl TableListener<P>,
    ) {
        listener.accept(&TableEvent::Created(TableCreated { change, table }));
        self.remember(change, table);
    }

    fn forget(
        &mut self,
        change: &SchemaChange<P>,
        id: &TableId,
        listener: &mut impl TableListener<P>,
    ) {
        if let Some(table) = self.tables.get(id) {
            listener.accept(&TableEvent::Dropped(TableDropped { change, table }));
        }
        self.tables.remove(id);
        self.applied_at
            .insert(id.clone(), change.position().clone());
    }

    fn alter(
        &mut self,
        change: &SchemaChange<P>,
        after: &TableModel,
        listener: &mut impl TableListener<P>,
    ) {
        let Some(before) = self.tables.get(after.id()).cloned() else {
            return self.create(change, after, listener);
        };

        for column in before.columns() {
            match after.column(column.name()) {
                None => listener.accept(&TableEvent::ColumnDropped(ColumnDropped {
                    change,
                    before: &before,
                    after,
                    column,
                })),
                Some(altered) if altered.type_name() != column.type_name() => {
                    listener.accept(&TableEvent::ColumnRetyped(ColumnRetyped {
                        change,
                        before: &before,
                        after,
                        previous: column,
                        column: altered,
                    }))
                }
                Some(_) => {}
            }
        }
        for column in after.columns() {
            if before.column(column.name()).is_none() {
                listener.accept(&TableEvent::ColumnAdded(ColumnAdded {
                    change,
                    before: &before,
                    after,
                    column,
                }));
            }
        }
        if before.primary_key().ne(after.primary_key()) {
            listener.accept(&TableEvent::PrimaryKeyChanged(PrimaryKeyChanged {
                change,
                before: &before,
                after,
            }));
        }

        self.remember(change, after);
    }

    fn remember(&mut self, change: &SchemaChange<P>, table: &TableModel) {
        self.tables.insert(table.id().clone(), table.clone());
        self.applied_at
            .insert(table.id().clone(), change.position().clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::TableChange;
    use crate::model::{ColumnType, TableId};
    use crate::position::RecordedAt;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Offset(u64);

    impl Position for Offset {}

    #[derive(Debug, Default)]
    struct Journal {
        entries: Vec<String>,
    }

    impl SchemaChangeListener<Offset> for Journal {
        fn accept(&mut self, change: &SchemaChange<Offset>) {
            self.entries
                .push(format!("statement {} tables", change.tables().count()));
        }
    }

    impl TableListener<Offset> for Journal {
        fn accept(&mut self, event: &TableEvent<'_, Offset>) {
            let entry = match event {
                TableEvent::Created(created) => format!("created {}", created.table().id()),
                TableEvent::Dropped(dropped) => format!("dropped {}", dropped.table().id()),
                TableEvent::ColumnAdded(added) => format!(
                    "added {}.{} at {} of {}",
                    added.after().id(),
                    added.column().name(),
                    added.column().position(),
                    added.after().columns().count()
                ),
                TableEvent::ColumnDropped(dropped) => format!(
                    "dropped {}.{}",
                    dropped.before().id(),
                    dropped.column().name()
                ),
                TableEvent::ColumnRetyped(retyped) => format!(
                    "retyped {}.{} {} -> {}",
                    retyped.after().id(),
                    retyped.column().name(),
                    retyped.previous().type_name(),
                    retyped.column().type_name()
                ),
                TableEvent::PrimaryKeyChanged(changed) => format!(
                    "primary key {} {:?} -> {:?}",
                    changed.after().id(),
                    changed.before().primary_key().collect::<Vec<_>>(),
                    changed.after().primary_key().collect::<Vec<_>>()
                ),
            };
            self.entries.push(entry);
        }
    }

    fn customers() -> TableId {
        TableId::new("shop", "customers")
    }

    fn column(name: &str, type_name: &str, column_type: ColumnType, position: u32) -> ColumnModel {
        ColumnModel::new(name, type_name, column_type, position, false, None)
    }

    fn identifier() -> ColumnModel {
        column("id", "BIGINT", ColumnType::Int64, 1)
    }

    fn name_at(position: u32) -> ColumnModel {
        column("name", "VARCHAR", ColumnType::Text, position)
    }

    fn table(columns: Vec<ColumnModel>, primary_key: &[&str]) -> TableModel {
        TableModel::new(
            customers(),
            columns,
            primary_key.iter().map(|name| (*name).to_owned()).collect(),
        )
    }

    fn change(offset: u64, tables: Vec<TableChange>) -> SchemaChange<Offset> {
        SchemaChange::new(Offset(offset), RecordedAt::from(offset as i64), "", tables)
    }

    fn created(offset: u64) -> SchemaChange<Offset> {
        change(
            offset,
            vec![TableChange::Created(table(
                vec![identifier(), name_at(2)],
                &["id"],
            ))],
        )
    }

    fn altered(
        offset: u64,
        columns: Vec<ColumnModel>,
        primary_key: &[&str],
    ) -> SchemaChange<Offset> {
        change(
            offset,
            vec![TableChange::Altered(table(columns, primary_key))],
        )
    }

    fn accept(catalog: &mut CatalogModel<Offset>, change: SchemaChange<Offset>) -> Vec<String> {
        let mut journal = Journal::default();
        catalog.accept(&change, &mut journal).unwrap();
        journal.entries
    }

    #[test]
    fn a_created_table_is_announced_and_remembered() {
        let mut catalog = CatalogModel::default();

        let entries = accept(&mut catalog, created(1));

        assert_eq!(entries, ["created shop.customers"]);
        assert_eq!(
            catalog
                .table(&customers())
                .unwrap()
                .columns()
                .map(ColumnModel::name)
                .collect::<Vec<_>>(),
            ["id", "name"]
        );
    }

    #[test]
    fn an_added_column_is_announced_with_where_it_landed() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(1));

        let entries = accept(
            &mut catalog,
            altered(
                2,
                vec![
                    identifier(),
                    column("nickname", "VARCHAR", ColumnType::Text, 2),
                    name_at(3),
                ],
                &["id"],
            ),
        );

        assert_eq!(entries, ["added shop.customers.nickname at 2 of 3"]);
    }

    #[test]
    fn a_dropped_column_is_announced_against_the_table_that_had_it() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(1));

        let entries = accept(&mut catalog, altered(2, vec![identifier()], &["id"]));

        assert_eq!(entries, ["dropped shop.customers.name"]);
    }

    #[test]
    fn a_retyped_column_is_announced_with_both_types() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(1));

        let entries = accept(
            &mut catalog,
            altered(
                2,
                vec![identifier(), column("name", "TEXT", ColumnType::Text, 2)],
                &["id"],
            ),
        );

        assert_eq!(entries, ["retyped shop.customers.name VARCHAR -> TEXT"]);
    }

    #[test]
    fn a_changed_primary_key_is_announced() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(1));

        let entries = accept(
            &mut catalog,
            altered(2, vec![identifier(), name_at(2)], &["id", "name"]),
        );

        assert_eq!(
            entries,
            ["primary key shop.customers [\"id\"] -> [\"id\", \"name\"]"]
        );
    }

    #[test]
    fn an_alter_of_a_table_the_catalog_never_saw_created_is_announced_as_a_creation() {
        let mut catalog = CatalogModel::default();

        let entries = accept(&mut catalog, altered(1, vec![identifier()], &["id"]));

        assert_eq!(entries, ["created shop.customers"]);
    }

    #[test]
    fn a_dropped_table_is_announced_and_forgotten() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(1));

        let entries = accept(
            &mut catalog,
            change(2, vec![TableChange::Dropped(customers())]),
        );

        assert_eq!(entries, ["dropped shop.customers"]);
        assert_eq!(catalog.table(&customers()), None);
    }

    #[test]
    fn a_statement_that_changes_nothing_announces_nothing() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(1));

        let entries = accept(
            &mut catalog,
            altered(2, vec![identifier(), name_at(2)], &["id"]),
        );

        assert_eq!(entries, [] as [String; 0]);
    }

    #[test]
    fn statements_of_one_position_all_apply_because_a_position_holds_several() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(1));

        let entries = accept(
            &mut catalog,
            change(1, vec![TableChange::Dropped(customers())]),
        );

        assert_eq!(entries, ["dropped shop.customers"]);
        assert_eq!(catalog.table(&customers()), None);
    }

    #[test]
    fn a_statement_older_than_the_table_it_changes_is_rejected() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(2));

        assert_eq!(
            catalog
                .accept(&created(1), &mut Journal::default())
                .unwrap_err(),
            OutOfOrder { table: customers() }
        );
    }

    #[test]
    fn a_statement_reaches_a_listener_of_whole_statements_once() {
        let mut journal = Journal::default();

        SchemaChangeListener::accept(&mut journal, &created(1));

        assert_eq!(journal.entries, ["statement 1 tables"]);
    }
}
