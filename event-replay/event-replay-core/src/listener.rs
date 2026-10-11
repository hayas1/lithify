use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt;

use crate::era::Era;
use crate::event::{SchemaChange, TableChange};
use crate::model::{ColumnModel, TableId, TableModel};
use crate::position::Position;

pub trait SchemaChangeListener<P> {
    type Context;
    type Error;

    fn visit_statement(
        &mut self,
        context: &Self::Context,
        change: &SchemaChange<P>,
    ) -> Result<(), Self::Error> {
        let _ = (context, change);

        Ok(())
    }

    fn leave_statement(
        &mut self,
        context: &Self::Context,
        change: &SchemaChange<P>,
    ) -> Result<(), Self::Error> {
        let _ = (context, change);

        Ok(())
    }

    fn visit_era(&mut self, context: &Self::Context, era: &Era) -> Result<(), Self::Error> {
        let _ = (context, era);

        Ok(())
    }

    fn visit_created(
        &mut self,
        context: &Self::Context,
        event: &TableCreated<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }

    fn visit_dropped(
        &mut self,
        context: &Self::Context,
        event: &TableDropped<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }

    fn visit_column_added(
        &mut self,
        context: &Self::Context,
        event: &ColumnAdded<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }

    fn visit_column_dropped(
        &mut self,
        context: &Self::Context,
        event: &ColumnDropped<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }

    fn visit_column_retyped(
        &mut self,
        context: &Self::Context,
        event: &ColumnRetyped<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }

    fn visit_primary_key_changed(
        &mut self,
        context: &Self::Context,
        event: &PrimaryKeyChanged<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }
}

impl<P> SchemaChangeListener<P> for () {
    type Context = ();
    type Error = Infallible;
}

pub struct TableCreated<'a> {
    table: &'a TableModel,
}

impl<'a> TableCreated<'a> {
    pub fn table(&self) -> &'a TableModel {
        self.table
    }
}

pub struct TableDropped<'a> {
    table: &'a TableModel,
}

impl<'a> TableDropped<'a> {
    pub fn table(&self) -> &'a TableModel {
        self.table
    }
}

pub struct ColumnAdded<'a> {
    before: &'a TableModel,
    after: &'a TableModel,
    column: &'a ColumnModel,
}

impl<'a> ColumnAdded<'a> {
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

pub struct ColumnDropped<'a> {
    before: &'a TableModel,
    after: &'a TableModel,
    column: &'a ColumnModel,
}

impl<'a> ColumnDropped<'a> {
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

pub struct ColumnRetyped<'a> {
    before: &'a TableModel,
    after: &'a TableModel,
    previous: &'a ColumnModel,
    column: &'a ColumnModel,
}

impl<'a> ColumnRetyped<'a> {
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

pub struct PrimaryKeyChanged<'a> {
    before: &'a TableModel,
    after: &'a TableModel,
}

impl<'a> PrimaryKeyChanged<'a> {
    pub fn before(&self) -> &'a TableModel {
        self.before
    }

    pub fn after(&self) -> &'a TableModel {
        self.after
    }
}

pub enum TableEvent<'a> {
    Created(TableCreated<'a>),
    Dropped(TableDropped<'a>),
    ColumnAdded(ColumnAdded<'a>),
    ColumnDropped(ColumnDropped<'a>),
    ColumnRetyped(ColumnRetyped<'a>),
    PrimaryKeyChanged(PrimaryKeyChanged<'a>),
}

impl TableEvent<'_> {
    pub fn accept<P, L: SchemaChangeListener<P>>(
        &self,
        listener: &mut L,
        context: &L::Context,
    ) -> Result<(), L::Error> {
        match self {
            Self::Created(event) => listener.visit_created(context, event),
            Self::Dropped(event) => listener.visit_dropped(context, event),
            Self::ColumnAdded(event) => listener.visit_column_added(context, event),
            Self::ColumnDropped(event) => listener.visit_column_dropped(context, event),
            Self::ColumnRetyped(event) => listener.visit_column_retyped(context, event),
            Self::PrimaryKeyChanged(event) => listener.visit_primary_key_changed(context, event),
        }
    }
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

impl From<Infallible> for OutOfOrder {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

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

    pub fn accept<E, L>(
        &mut self,
        change: &SchemaChange<P>,
        listener: &mut L,
        context: &L::Context,
    ) -> Result<(), E>
    where
        L: SchemaChangeListener<P>,
        E: From<OutOfOrder> + From<L::Error>,
    {
        for table in change.tables() {
            self.guard_order(table.id(), change.position())?;
        }

        listener.visit_statement(context, change)?;

        for table in change.tables() {
            match table {
                TableChange::Created(table) => self.create(change, table, listener, context)?,
                TableChange::Altered(table) => self.alter(change, table, listener, context)?,
                TableChange::Dropped(id) => self.forget(change, id, listener, context)?,
            }
        }

        listener.leave_statement(context, change)?;

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

    fn create<L: SchemaChangeListener<P>>(
        &mut self,
        change: &SchemaChange<P>,
        table: &TableModel,
        listener: &mut L,
        context: &L::Context,
    ) -> Result<(), L::Error> {
        TableEvent::Created(TableCreated { table }).accept(listener, context)?;
        self.remember(change, table);

        Ok(())
    }

    fn forget<L: SchemaChangeListener<P>>(
        &mut self,
        change: &SchemaChange<P>,
        id: &TableId,
        listener: &mut L,
        context: &L::Context,
    ) -> Result<(), L::Error> {
        if let Some(table) = self.tables.get(id) {
            TableEvent::Dropped(TableDropped { table }).accept(listener, context)?;
        }
        self.tables.remove(id);
        self.applied_at
            .insert(id.clone(), change.position().clone());

        Ok(())
    }

    fn alter<L: SchemaChangeListener<P>>(
        &mut self,
        change: &SchemaChange<P>,
        after: &TableModel,
        listener: &mut L,
        context: &L::Context,
    ) -> Result<(), L::Error> {
        let Some(before) = self.tables.get(after.id()).cloned() else {
            return self.create(change, after, listener, context);
        };

        for column in before.columns() {
            match after.column(column.name()) {
                None => TableEvent::ColumnDropped(ColumnDropped {
                    before: &before,
                    after,
                    column,
                })
                .accept(listener, context)?,
                Some(altered) if altered.type_name() != column.type_name() => {
                    TableEvent::ColumnRetyped(ColumnRetyped {
                        before: &before,
                        after,
                        previous: column,
                        column: altered,
                    })
                    .accept(listener, context)?
                }
                Some(_) => {}
            }
        }
        for column in after.columns() {
            if before.column(column.name()).is_none() {
                TableEvent::ColumnAdded(ColumnAdded {
                    before: &before,
                    after,
                    column,
                })
                .accept(listener, context)?;
            }
        }
        if before.primary_key().ne(after.primary_key()) {
            TableEvent::PrimaryKeyChanged(PrimaryKeyChanged {
                before: &before,
                after,
            })
            .accept(listener, context)?;
        }

        self.remember(change, after);

        Ok(())
    }

    fn remember(&mut self, change: &SchemaChange<P>, table: &TableModel) {
        self.tables.insert(table.id().clone(), table.clone());
        self.applied_at
            .insert(table.id().clone(), change.position().clone());
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

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

    #[derive(Debug, Default)]
    struct Statements {
        entries: Vec<String>,
    }

    impl SchemaChangeListener<Offset> for Statements {
        type Context = ();
        type Error = Infallible;

        fn visit_statement(
            &mut self,
            _: &(),
            change: &SchemaChange<Offset>,
        ) -> Result<(), Infallible> {
            self.entries
                .push(format!("statement {} tables", change.tables().count()));

            Ok(())
        }

        fn leave_statement(
            &mut self,
            _: &(),
            change: &SchemaChange<Offset>,
        ) -> Result<(), Infallible> {
            self.entries
                .push(format!("left statement {} tables", change.tables().count()));

            Ok(())
        }

        fn visit_created(&mut self, _: &(), event: &TableCreated<'_>) -> Result<(), Infallible> {
            self.entries.push(format!("table {}", event.table().id()));

            Ok(())
        }
    }

    impl SchemaChangeListener<Offset> for Journal {
        type Context = ();
        type Error = Infallible;

        fn visit_created(&mut self, _: &(), event: &TableCreated<'_>) -> Result<(), Infallible> {
            self.entries.push(format!("created {}", event.table().id()));

            Ok(())
        }

        fn visit_dropped(&mut self, _: &(), event: &TableDropped<'_>) -> Result<(), Infallible> {
            self.entries.push(format!("dropped {}", event.table().id()));

            Ok(())
        }

        fn visit_column_added(
            &mut self,
            _: &(),
            event: &ColumnAdded<'_>,
        ) -> Result<(), Infallible> {
            self.entries.push(format!(
                "added {}.{} at {} of {}",
                event.after().id(),
                event.column().name(),
                event.column().position(),
                event.after().columns().count()
            ));

            Ok(())
        }

        fn visit_column_dropped(
            &mut self,
            _: &(),
            event: &ColumnDropped<'_>,
        ) -> Result<(), Infallible> {
            self.entries.push(format!(
                "dropped {}.{}",
                event.before().id(),
                event.column().name()
            ));

            Ok(())
        }

        fn visit_column_retyped(
            &mut self,
            _: &(),
            event: &ColumnRetyped<'_>,
        ) -> Result<(), Infallible> {
            self.entries.push(format!(
                "retyped {}.{} {} -> {}",
                event.after().id(),
                event.column().name(),
                event.previous().type_name(),
                event.column().type_name()
            ));

            Ok(())
        }

        fn visit_primary_key_changed(
            &mut self,
            _: &(),
            event: &PrimaryKeyChanged<'_>,
        ) -> Result<(), Infallible> {
            self.entries.push(format!(
                "primary key {} {:?} -> {:?}",
                event.after().id(),
                event.before().primary_key().collect::<Vec<_>>(),
                event.after().primary_key().collect::<Vec<_>>()
            ));

            Ok(())
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
        catalog
            .accept::<OutOfOrder, _>(&change, &mut journal, &())
            .unwrap();
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
    fn a_statement_that_changes_no_table_announces_the_statement_alone() {
        let mut catalog = CatalogModel::default();
        let mut statements = Statements::default();
        catalog
            .accept::<OutOfOrder, _>(&created(1), &mut statements, &())
            .unwrap();
        statements.entries.clear();

        catalog
            .accept::<OutOfOrder, _>(
                &altered(2, vec![identifier(), name_at(2)], &["id"]),
                &mut statements,
                &(),
            )
            .unwrap();

        assert_eq!(
            statements.entries,
            ["statement 1 tables", "left statement 1 tables"]
        );
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
                .accept::<OutOfOrder, _>(&created(1), &mut Journal::default(), &())
                .unwrap_err(),
            OutOfOrder { table: customers() }
        );
    }

    #[test]
    fn a_statement_reaches_the_listener_before_the_tables_it_changes() {
        let mut catalog = CatalogModel::default();
        let mut statements = Statements::default();

        catalog
            .accept::<OutOfOrder, _>(&created(1), &mut statements, &())
            .unwrap();

        assert_eq!(
            statements.entries,
            [
                "statement 1 tables",
                "table shop.customers",
                "left statement 1 tables"
            ]
        );
    }

    #[test]
    fn a_statement_that_is_rejected_reaches_no_listener() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(2));
        let mut statements = Statements::default();

        assert!(
            catalog
                .accept::<OutOfOrder, _>(&created(1), &mut statements, &())
                .is_err()
        );
        assert!(statements.entries.is_empty());
    }
}
