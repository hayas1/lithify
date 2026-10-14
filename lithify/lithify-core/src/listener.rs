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

    fn era_boundary(
        &mut self,
        context: &Self::Context,
        closed_era: &Era<P>,
    ) -> Result<(), Self::Error> {
        let _ = (context, closed_era);

        Ok(())
    }

    fn table_created(
        &mut self,
        context: &Self::Context,
        event: &TableCreated<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }

    fn table_dropped(
        &mut self,
        context: &Self::Context,
        event: &TableDropped<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }

    fn table_renamed(
        &mut self,
        context: &Self::Context,
        event: &TableRenamed<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }

    fn column_renamed(
        &mut self,
        context: &Self::Context,
        event: &ColumnRenamed<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }

    fn column_added(
        &mut self,
        context: &Self::Context,
        event: &ColumnAdded<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }

    fn column_dropped(
        &mut self,
        context: &Self::Context,
        event: &ColumnDropped<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }

    fn column_retyped(
        &mut self,
        context: &Self::Context,
        event: &ColumnRetyped<'_>,
    ) -> Result<(), Self::Error> {
        let _ = (context, event);

        Ok(())
    }

    fn primary_key_changed(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Appearance {
    Created,
    Captured,
    Discovered,
}

impl fmt::Display for Appearance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Created => formatter.write_str("created"),
            Self::Captured => formatter.write_str("captured"),
            Self::Discovered => formatter.write_str("discovered"),
        }
    }
}

impl Appearance {
    pub fn is_creation(&self) -> bool {
        match self {
            Self::Created => true,
            Self::Captured | Self::Discovered => false,
        }
    }

    pub fn is_logged_from_start(&self) -> bool {
        match self {
            Self::Created | Self::Captured => true,
            Self::Discovered => false,
        }
    }

    fn of<P>(change: &SchemaChange<P>) -> Self {
        match change.is_captured() {
            true => Self::Captured,
            false => Self::Created,
        }
    }
}

pub struct TableCreated<'a> {
    table: &'a TableModel,
    appearance: Appearance,
}

impl<'a> TableCreated<'a> {
    pub fn table(&self) -> &'a TableModel {
        self.table
    }

    pub fn appearance(&self) -> Appearance {
        self.appearance
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

pub struct TableRenamed<'a> {
    before: &'a TableModel,
    after: &'a TableModel,
}

impl<'a> TableRenamed<'a> {
    pub fn before(&self) -> &'a TableModel {
        self.before
    }

    pub fn after(&self) -> &'a TableModel {
        self.after
    }
}

pub struct ColumnRenamed<'a> {
    before: &'a TableModel,
    after: &'a TableModel,
    previous: &'a ColumnModel,
    column: &'a ColumnModel,
}

impl<'a> ColumnRenamed<'a> {
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
    TableCreated(TableCreated<'a>),
    TableDropped(TableDropped<'a>),
    TableRenamed(TableRenamed<'a>),
    ColumnRenamed(ColumnRenamed<'a>),
    ColumnAdded(ColumnAdded<'a>),
    ColumnDropped(ColumnDropped<'a>),
    ColumnRetyped(ColumnRetyped<'a>),
    PrimaryKeyChanged(PrimaryKeyChanged<'a>),
}

impl TableEvent<'_> {
    pub fn accept<P, L: SchemaChangeListener<P>>(
        &self,
        context: &L::Context,
        listener: &mut L,
    ) -> Result<(), L::Error> {
        match self {
            Self::TableCreated(event) => listener.table_created(context, event),
            Self::TableDropped(event) => listener.table_dropped(context, event),
            Self::TableRenamed(event) => listener.table_renamed(context, event),
            Self::ColumnRenamed(event) => listener.column_renamed(context, event),
            Self::ColumnAdded(event) => listener.column_added(context, event),
            Self::ColumnDropped(event) => listener.column_dropped(context, event),
            Self::ColumnRetyped(event) => listener.column_retyped(context, event),
            Self::PrimaryKeyChanged(event) => listener.primary_key_changed(context, event),
        }
    }
}

enum Alteration<'a> {
    Dropped(&'a ColumnModel),
    Retyped {
        previous: &'a ColumnModel,
        column: &'a ColumnModel,
    },
    Added(&'a ColumnModel),
    Rekeyed(Vec<&'a str>),
}

impl<'a> Alteration<'a> {
    fn between(before: &'a TableModel, after: &'a TableModel) -> Vec<Self> {
        let dropped = before
            .columns()
            .filter(|column| after.column(column.name()).is_none())
            .map(Self::Dropped);
        let retyped = before.columns().filter_map(|previous| {
            after
                .column(previous.name())
                .filter(|column| column.type_name() != previous.type_name())
                .map(|column| Self::Retyped { previous, column })
        });
        let added = after
            .columns()
            .filter(|column| before.column(column.name()).is_none())
            .map(Self::Added);
        let rekeyed = before
            .primary_key()
            .ne(after.primary_key())
            .then(|| Self::Rekeyed(after.primary_key().collect()));

        dropped.chain(retyped).chain(added).chain(rekeyed).collect()
    }

    fn warn_of_unread_rename<P>(alterations: &[Self], table: &TableId, change: &SchemaChange<P>) {
        let dropped: Vec<&str> = alterations.iter().filter_map(Self::as_dropped).collect();
        let added: Vec<&str> = alterations.iter().filter_map(Self::as_added).collect();

        if !dropped.is_empty() && !added.is_empty() {
            tracing::warn!(
                table = %table,
                ?dropped,
                ?added,
                statement = change.statement(),
                "one statement dropped and added columns; if it renamed a column the dialect could \
                 not read, the values of the dropped column are lost to the added one, so carry \
                 them over by hand from the snapshot taken before the statement"
            );
        }
    }

    fn as_dropped(&self) -> Option<&str> {
        match self {
            Self::Dropped(column) => Some(column.name()),
            _ => None,
        }
    }

    fn as_added(&self) -> Option<&str> {
        match self {
            Self::Added(column) => Some(column.name()),
            _ => None,
        }
    }

    fn apply(&self, shape: &TableModel) -> TableModel {
        match self {
            Self::Dropped(column) => shape.without_column(column.name()),
            Self::Retyped { column, .. } | Self::Added(column) => shape.with_column(column),
            Self::Rekeyed(primary_key) => shape.keyed_by(primary_key.iter().copied()),
        }
    }

    fn event<'b>(&self, before: &'b TableModel, after: &'b TableModel) -> TableEvent<'b>
    where
        'a: 'b,
    {
        match *self {
            Self::Dropped(column) => TableEvent::ColumnDropped(ColumnDropped {
                before,
                after,
                column,
            }),
            Self::Retyped { previous, column } => TableEvent::ColumnRetyped(ColumnRetyped {
                before,
                after,
                previous,
                column,
            }),
            Self::Added(column) => TableEvent::ColumnAdded(ColumnAdded {
                before,
                after,
                column,
            }),
            Self::Rekeyed(_) => TableEvent::PrimaryKeyChanged(PrimaryKeyChanged { before, after }),
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
        context: &L::Context,
        change: &SchemaChange<P>,
        listener: &mut L,
    ) -> Result<(), E>
    where
        L: SchemaChangeListener<P>,
        E: From<OutOfOrder> + From<L::Error>,
    {
        for table in change.tables().flat_map(TableChange::ids) {
            self.guard_order(table, change.position())?;
        }

        listener.visit_statement(context, change)?;

        for table in change.tables() {
            match table {
                TableChange::Created(table) => {
                    self.create(context, change, table, Appearance::of(change), listener)?
                }
                TableChange::Renamed { from, to } => {
                    self.rename(context, change, from, to, listener)?
                }
                TableChange::ColumnRenamed { table, from, to } => {
                    self.rename_column(context, change, table, from, to, listener)?
                }
                TableChange::Altered(table) => self.alter(context, change, table, listener)?,
                TableChange::Dropped(id) => self.forget(context, change, id, listener)?,
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
        context: &L::Context,
        change: &SchemaChange<P>,
        table: &TableModel,
        appearance: Appearance,
        listener: &mut L,
    ) -> Result<(), L::Error> {
        TableEvent::TableCreated(TableCreated { table, appearance }).accept(context, listener)?;
        self.remember(change, table);

        Ok(())
    }

    fn forget<L: SchemaChangeListener<P>>(
        &mut self,
        context: &L::Context,
        change: &SchemaChange<P>,
        id: &TableId,
        listener: &mut L,
    ) -> Result<(), L::Error> {
        match self.tables.get(id) {
            Some(table) => {
                TableEvent::TableDropped(TableDropped { table }).accept(context, listener)?
            }
            None => tracing::warn!(
                table = %id,
                statement = change.statement(),
                "a statement dropped a table the catalog never saw, so the schema changes read \
                 may begin after the table was created"
            ),
        }
        self.release(change, id);

        Ok(())
    }

    fn rename<L: SchemaChangeListener<P>>(
        &mut self,
        context: &L::Context,
        change: &SchemaChange<P>,
        from: &TableId,
        to: &TableId,
        listener: &mut L,
    ) -> Result<(), L::Error> {
        let Some(before) = self.tables.get(from).cloned() else {
            tracing::warn!(
                table = %from,
                to = %to,
                statement = change.statement(),
                "a statement renamed a table the catalog never saw, so the table it was renamed \
                 to appears as discovered"
            );

            return Ok(());
        };
        let after = before.with_id(to);

        TableEvent::TableRenamed(TableRenamed {
            before: &before,
            after: &after,
        })
        .accept(context, listener)?;
        self.release(change, from);
        self.remember(change, &after);

        Ok(())
    }

    fn rename_column<L: SchemaChangeListener<P>>(
        &mut self,
        context: &L::Context,
        change: &SchemaChange<P>,
        table: &TableId,
        from: &str,
        to: &str,
        listener: &mut L,
    ) -> Result<(), L::Error> {
        let Some(before) = self.tables.get(table).cloned() else {
            tracing::warn!(
                table = %table,
                column = from,
                to,
                statement = change.statement(),
                "a statement renamed a column of a table the catalog never saw"
            );

            return Ok(());
        };
        let (Some(previous), None) = (before.column(from), before.column(to)) else {
            tracing::warn!(
                table = %table,
                column = from,
                to,
                statement = change.statement(),
                "a statement renamed a column the table never had, or onto a name it already \
                 holds, so the rename steps through as the alteration that follows it"
            );

            return Ok(());
        };
        let after = before.with_column_renamed(from, to);

        TableEvent::ColumnRenamed(ColumnRenamed {
            before: &before,
            after: &after,
            previous,
            column: &previous.named(to),
        })
        .accept(context, listener)?;
        self.remember(change, &after);

        Ok(())
    }

    fn alter<L: SchemaChangeListener<P>>(
        &mut self,
        context: &L::Context,
        change: &SchemaChange<P>,
        after: &TableModel,
        listener: &mut L,
    ) -> Result<(), L::Error> {
        let Some(before) = self.tables.get(after.id()).cloned() else {
            return self.create(context, change, after, Appearance::Discovered, listener);
        };
        let alterations = Alteration::between(&before, after);
        Alteration::warn_of_unread_rename(&alterations, after.id(), change);
        let last = alterations.len().saturating_sub(1);
        let mut shape = before.clone();

        for (step, alteration) in alterations.into_iter().enumerate() {
            // What no event announces (an order, a default, a nullability) lands with the last
            // step, so the steps end at the very table the catalog remembers.
            let next = match step == last {
                true => after.clone(),
                false => alteration.apply(&shape),
            };

            alteration.event(&shape, &next).accept(context, listener)?;
            shape = next;
        }

        self.remember(change, after);

        Ok(())
    }

    fn release(&mut self, change: &SchemaChange<P>, id: &TableId) {
        self.tables.remove(id);
        self.applied_at
            .insert(id.clone(), change.position().clone());
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

        fn table_created(&mut self, _: &(), event: &TableCreated<'_>) -> Result<(), Infallible> {
            self.entries.push(format!("table {}", event.table().id()));

            Ok(())
        }
    }

    impl SchemaChangeListener<Offset> for Journal {
        type Context = ();
        type Error = Infallible;

        fn table_created(&mut self, _: &(), event: &TableCreated<'_>) -> Result<(), Infallible> {
            self.entries.push(format!("created {}", event.table().id()));

            Ok(())
        }

        fn table_dropped(&mut self, _: &(), event: &TableDropped<'_>) -> Result<(), Infallible> {
            self.entries.push(format!("dropped {}", event.table().id()));

            Ok(())
        }

        fn column_added(&mut self, _: &(), event: &ColumnAdded<'_>) -> Result<(), Infallible> {
            self.entries.push(format!(
                "added {}.{}",
                event.after().id(),
                event.column().name()
            ));

            Ok(())
        }

        fn column_dropped(&mut self, _: &(), event: &ColumnDropped<'_>) -> Result<(), Infallible> {
            self.entries.push(format!(
                "dropped {}.{}",
                event.before().id(),
                event.column().name()
            ));

            Ok(())
        }

        fn column_retyped(&mut self, _: &(), event: &ColumnRetyped<'_>) -> Result<(), Infallible> {
            self.entries.push(format!(
                "retyped {}.{} {} -> {}",
                event.after().id(),
                event.column().name(),
                event.previous().type_name(),
                event.column().type_name()
            ));

            Ok(())
        }

        fn primary_key_changed(
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

    fn column(name: &str, type_name: &str, column_type: ColumnType) -> ColumnModel {
        ColumnModel::new(name, type_name, column_type, false, None)
    }

    fn identifier() -> ColumnModel {
        column("id", "BIGINT", ColumnType::Int64)
    }

    fn name() -> ColumnModel {
        column("name", "VARCHAR", ColumnType::Text)
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
                vec![identifier(), name()],
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
            .accept::<OutOfOrder, _>(&(), &change, &mut journal)
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
    fn an_added_column_is_announced_against_the_table_that_gained_it() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(1));

        let entries = accept(
            &mut catalog,
            altered(
                2,
                vec![
                    identifier(),
                    column("nickname", "VARCHAR", ColumnType::Text),
                    name(),
                ],
                &["id"],
            ),
        );

        assert_eq!(entries, ["added shop.customers.nickname"]);
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
                vec![identifier(), column("name", "TEXT", ColumnType::Text)],
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
            altered(2, vec![identifier(), name()], &["id", "name"]),
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
            .accept::<OutOfOrder, _>(&(), &created(1), &mut statements)
            .unwrap();
        statements.entries.clear();

        catalog
            .accept::<OutOfOrder, _>(
                &(),
                &altered(2, vec![identifier(), name()], &["id"]),
                &mut statements,
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
                .accept::<OutOfOrder, _>(&(), &created(1), &mut Journal::default())
                .unwrap_err(),
            OutOfOrder { table: customers() }
        );
    }

    #[test]
    fn a_statement_reaches_the_listener_before_the_tables_it_changes() {
        let mut catalog = CatalogModel::default();
        let mut statements = Statements::default();

        catalog
            .accept::<OutOfOrder, _>(&(), &created(1), &mut statements)
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
                .accept::<OutOfOrder, _>(&(), &created(1), &mut statements)
                .is_err()
        );
        assert!(statements.entries.is_empty());
    }

    #[derive(Debug, PartialEq)]
    struct Step {
        announced: String,
        before: TableModel,
        after: TableModel,
    }

    #[derive(Debug, Default)]
    struct Steps {
        taken: Vec<Step>,
    }

    impl Steps {
        fn take(&mut self, announced: String, before: &TableModel, after: &TableModel) {
            self.taken.push(Step {
                announced,
                before: before.clone(),
                after: after.clone(),
            });
        }

        fn announcements(&self) -> Vec<&str> {
            self.taken
                .iter()
                .map(|step| step.announced.as_str())
                .collect()
        }

        fn outlines(&self) -> Vec<(Vec<String>, Vec<String>)> {
            self.taken
                .iter()
                .map(|step| (Self::outline(&step.before), Self::outline(&step.after)))
                .collect()
        }

        fn outline(table: &TableModel) -> Vec<String> {
            table
                .columns()
                .map(|column| format!("{} {}", column.name(), column.type_name()))
                .chain([format!("key {:?}", table.primary_key().collect::<Vec<_>>())])
                .collect()
        }
    }

    impl SchemaChangeListener<Offset> for Steps {
        type Context = ();
        type Error = Infallible;

        fn table_renamed(&mut self, _: &(), event: &TableRenamed<'_>) -> Result<(), Infallible> {
            self.take(
                format!("renamed {} to {}", event.before().id(), event.after().id()),
                event.before(),
                event.after(),
            );

            Ok(())
        }

        fn column_renamed(&mut self, _: &(), event: &ColumnRenamed<'_>) -> Result<(), Infallible> {
            self.take(
                format!(
                    "renamed {} to {}",
                    event.previous().name(),
                    event.column().name()
                ),
                event.before(),
                event.after(),
            );

            Ok(())
        }

        fn column_added(&mut self, _: &(), event: &ColumnAdded<'_>) -> Result<(), Infallible> {
            self.take(
                format!("added {}", event.column().name()),
                event.before(),
                event.after(),
            );

            Ok(())
        }

        fn column_dropped(&mut self, _: &(), event: &ColumnDropped<'_>) -> Result<(), Infallible> {
            self.take(
                format!("dropped {}", event.column().name()),
                event.before(),
                event.after(),
            );

            Ok(())
        }

        fn column_retyped(&mut self, _: &(), event: &ColumnRetyped<'_>) -> Result<(), Infallible> {
            self.take(
                format!("retyped {}", event.column().name()),
                event.before(),
                event.after(),
            );

            Ok(())
        }

        fn primary_key_changed(
            &mut self,
            _: &(),
            event: &PrimaryKeyChanged<'_>,
        ) -> Result<(), Infallible> {
            self.take("rekeyed".to_owned(), event.before(), event.after());

            Ok(())
        }
    }

    fn email() -> ColumnModel {
        column("email", "VARCHAR", ColumnType::Text)
    }

    fn nickname() -> ColumnModel {
        column("nickname", "VARCHAR", ColumnType::Text)
    }

    fn phone() -> ColumnModel {
        column("phone", "VARCHAR", ColumnType::Text)
    }

    fn steps(before: TableModel, after: TableModel) -> Steps {
        let mut catalog = CatalogModel::default();
        catalog
            .accept::<OutOfOrder, _>(&(), &change(1, vec![TableChange::Created(before)]), &mut ())
            .unwrap();
        let mut steps = Steps::default();
        catalog
            .accept::<OutOfOrder, _>(
                &(),
                &change(2, vec![TableChange::Altered(after)]),
                &mut steps,
            )
            .unwrap();
        steps
    }

    fn listed(entries: &[&str]) -> Vec<String> {
        entries.iter().map(|entry| (*entry).to_owned()).collect()
    }

    fn reshaped() -> (TableModel, TableModel) {
        (
            table(vec![identifier(), name(), email()], &["id"]),
            table(
                vec![
                    identifier(),
                    nickname(),
                    column("name", "TEXT", ColumnType::Text),
                    phone(),
                ],
                &["id", "nickname"],
            ),
        )
    }

    #[test]
    fn an_alteration_of_one_change_steps_from_the_table_before_it_to_the_table_after_it() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier(), name(), email()], &["id"]);

        let steps = steps(before.clone(), after.clone());

        assert_eq!(
            steps.taken,
            [Step {
                announced: "added email".to_owned(),
                before,
                after
            }]
        );
    }

    #[test]
    fn an_alteration_steps_through_drops_then_retypes_then_additions_then_its_key() {
        let (before, after) = reshaped();

        assert_eq!(
            steps(before, after).announcements(),
            [
                "dropped email",
                "retyped name",
                "added nickname",
                "added phone",
                "rekeyed"
            ]
        );
    }

    #[test]
    fn each_step_of_an_alteration_starts_from_the_table_the_step_before_it_left() {
        let (before, after) = reshaped();
        let steps = steps(before, after);

        for pair in steps.taken.windows(2) {
            assert_eq!(pair[0].after, pair[1].before);
        }
    }

    #[test]
    fn the_first_step_of_an_alteration_starts_from_the_table_before_it() {
        let (before, after) = reshaped();

        assert_eq!(steps(before.clone(), after).taken[0].before, before);
    }

    #[test]
    fn the_last_step_of_an_alteration_ends_in_the_table_after_it() {
        let (before, after) = reshaped();

        assert_eq!(
            steps(before, after.clone()).taken.last().unwrap().after,
            after
        );
    }

    #[test]
    fn each_step_of_an_alteration_changes_only_what_it_announces() {
        let (before, after) = reshaped();

        assert_eq!(
            steps(before, after).outlines(),
            [
                (
                    listed(&["id BIGINT", "name VARCHAR", "email VARCHAR", "key [\"id\"]"]),
                    listed(&["id BIGINT", "name VARCHAR", "key [\"id\"]"]),
                ),
                (
                    listed(&["id BIGINT", "name VARCHAR", "key [\"id\"]"]),
                    listed(&["id BIGINT", "name TEXT", "key [\"id\"]"]),
                ),
                (
                    listed(&["id BIGINT", "name TEXT", "key [\"id\"]"]),
                    listed(&["id BIGINT", "name TEXT", "nickname VARCHAR", "key [\"id\"]"]),
                ),
                (
                    listed(&["id BIGINT", "name TEXT", "nickname VARCHAR", "key [\"id\"]"]),
                    listed(&[
                        "id BIGINT",
                        "name TEXT",
                        "nickname VARCHAR",
                        "phone VARCHAR",
                        "key [\"id\"]"
                    ]),
                ),
                (
                    listed(&[
                        "id BIGINT",
                        "name TEXT",
                        "nickname VARCHAR",
                        "phone VARCHAR",
                        "key [\"id\"]"
                    ]),
                    listed(&[
                        "id BIGINT",
                        "nickname VARCHAR",
                        "name TEXT",
                        "phone VARCHAR",
                        "key [\"id\", \"nickname\"]"
                    ]),
                ),
            ]
        );
    }

    #[test]
    fn columns_added_together_are_added_one_step_at_a_time() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier(), name(), nickname(), phone()], &["id"]);

        assert_eq!(
            steps(before, after).outlines(),
            [
                (
                    listed(&["id BIGINT", "name VARCHAR", "key [\"id\"]"]),
                    listed(&[
                        "id BIGINT",
                        "name VARCHAR",
                        "nickname VARCHAR",
                        "key [\"id\"]"
                    ]),
                ),
                (
                    listed(&[
                        "id BIGINT",
                        "name VARCHAR",
                        "nickname VARCHAR",
                        "key [\"id\"]"
                    ]),
                    listed(&[
                        "id BIGINT",
                        "name VARCHAR",
                        "nickname VARCHAR",
                        "phone VARCHAR",
                        "key [\"id\"]"
                    ]),
                ),
            ]
        );
    }

    #[test]
    fn columns_dropped_together_are_dropped_one_step_at_a_time() {
        let before = table(vec![identifier(), name(), email()], &["id"]);
        let after = table(vec![identifier()], &["id"]);

        assert_eq!(
            steps(before, after).outlines(),
            [
                (
                    listed(&["id BIGINT", "name VARCHAR", "email VARCHAR", "key [\"id\"]"]),
                    listed(&["id BIGINT", "email VARCHAR", "key [\"id\"]"]),
                ),
                (
                    listed(&["id BIGINT", "email VARCHAR", "key [\"id\"]"]),
                    listed(&["id BIGINT", "key [\"id\"]"]),
                ),
            ]
        );
    }

    #[test]
    fn a_column_replaced_by_one_of_its_name_is_retyped_in_one_step() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(
            vec![identifier(), column("name", "TEXT", ColumnType::Text)],
            &["id"],
        );

        assert_eq!(steps(before, after).announcements(), ["retyped name"]);
    }

    #[test]
    fn what_no_step_announces_changes_in_the_last_step() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(
            vec![
                identifier(),
                ColumnModel::new("name", "VARCHAR", ColumnType::Text, true, Some("anonymous")),
                email(),
            ],
            &["id"],
        );

        let steps = steps(before, after.clone());

        assert_eq!(steps.announcements(), ["added email"]);
        assert_eq!(steps.taken[0].after, after);
    }

    #[test]
    fn an_alteration_announcing_nothing_takes_no_step() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(
            vec![
                identifier(),
                ColumnModel::new("name", "VARCHAR", ColumnType::Text, true, Some("anonymous")),
            ],
            &["id"],
        );

        assert!(steps(before, after).taken.is_empty());
    }

    #[test]
    fn what_no_step_announces_waits_for_the_last_of_several_steps() {
        let before = table(vec![identifier(), name()], &["id"]);
        let anonymous =
            ColumnModel::new("name", "VARCHAR", ColumnType::Text, true, Some("anonymous"));
        let after = table(vec![identifier(), anonymous, email(), phone()], &["id"]);

        let steps = steps(before, after.clone());

        assert_eq!(steps.announcements(), ["added email", "added phone"]);
        assert_eq!(steps.taken[0].after.column("name"), Some(&name()));
        assert_eq!(steps.taken[1].after, after);
    }

    #[test]
    fn a_key_changed_alone_steps_from_the_table_before_it_to_the_table_after_it() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier(), name()], &["id", "name"]);

        assert_eq!(
            steps(before.clone(), after.clone()).taken,
            [Step {
                announced: "rekeyed".to_owned(),
                before,
                after
            }]
        );
    }

    #[derive(Debug, PartialEq)]
    enum Halted {
        OutOfOrder,
        Refused(String),
    }

    impl From<OutOfOrder> for Halted {
        fn from(_: OutOfOrder) -> Self {
            Self::OutOfOrder
        }
    }

    #[derive(Debug, Default)]
    struct Refusing {
        announced: Vec<String>,
    }

    impl SchemaChangeListener<Offset> for Refusing {
        type Context = ();
        type Error = Halted;

        fn column_added(&mut self, _: &(), event: &ColumnAdded<'_>) -> Result<(), Halted> {
            Err(Halted::Refused(event.column().name().to_owned()))
        }

        fn column_dropped(&mut self, _: &(), event: &ColumnDropped<'_>) -> Result<(), Halted> {
            self.announced
                .push(format!("dropped {}", event.column().name()));

            Ok(())
        }

        fn column_retyped(&mut self, _: &(), event: &ColumnRetyped<'_>) -> Result<(), Halted> {
            self.announced
                .push(format!("retyped {}", event.column().name()));

            Ok(())
        }
    }

    #[test]
    fn an_alteration_halts_at_the_step_its_listener_refuses_and_is_not_remembered() {
        let before = table(vec![identifier(), name(), email()], &["id"]);
        let after = table(
            vec![
                identifier(),
                column("name", "TEXT", ColumnType::Text),
                phone(),
                nickname(),
            ],
            &["id"],
        );
        let mut catalog = CatalogModel::default();
        catalog
            .accept::<OutOfOrder, _>(
                &(),
                &change(1, vec![TableChange::Created(before.clone())]),
                &mut (),
            )
            .unwrap();
        let mut refusing = Refusing::default();

        let halted = catalog.accept::<Halted, _>(
            &(),
            &change(2, vec![TableChange::Altered(after)]),
            &mut refusing,
        );

        assert_eq!(halted, Err(Halted::Refused("phone".to_owned())));
        assert_eq!(refusing.announced, ["dropped email", "retyped name"]);
        assert_eq!(catalog.table(&customers()), Some(&before));
    }

    fn clients() -> TableId {
        TableId::new("shop", "clients")
    }

    fn renaming(from: &str, to: &str) -> TableChange {
        TableChange::ColumnRenamed {
            table: customers(),
            from: from.to_owned(),
            to: to.to_owned(),
        }
    }

    fn statement_steps(
        before: TableModel,
        tables: Vec<TableChange>,
    ) -> (Steps, CatalogModel<Offset>) {
        let mut catalog = CatalogModel::default();
        catalog
            .accept::<OutOfOrder, _>(&(), &change(1, vec![TableChange::Created(before)]), &mut ())
            .unwrap();
        let mut steps = Steps::default();
        catalog
            .accept::<OutOfOrder, _>(&(), &change(2, tables), &mut steps)
            .unwrap();
        (steps, catalog)
    }

    fn full_name() -> ColumnModel {
        column("full_name", "VARCHAR", ColumnType::Text)
    }

    #[test]
    fn a_renamed_column_steps_once_instead_of_being_dropped_and_added() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier(), full_name()], &["id"]);

        let (steps, catalog) = statement_steps(
            before.clone(),
            vec![
                renaming("name", "full_name"),
                TableChange::Altered(after.clone()),
            ],
        );

        assert_eq!(
            steps.taken,
            [Step {
                announced: "renamed name to full_name".to_owned(),
                before,
                after: after.clone()
            }]
        );
        assert_eq!(catalog.table(&customers()), Some(&after));
    }

    #[test]
    fn a_column_renamed_and_another_added_by_one_statement_step_one_after_the_other() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier(), full_name(), email()], &["id"]);

        let (steps, _) = statement_steps(
            before,
            vec![renaming("name", "full_name"), TableChange::Altered(after)],
        );

        assert_eq!(
            steps.outlines(),
            [
                (
                    listed(&["id BIGINT", "name VARCHAR", "key [\"id\"]"]),
                    listed(&["id BIGINT", "full_name VARCHAR", "key [\"id\"]"]),
                ),
                (
                    listed(&["id BIGINT", "full_name VARCHAR", "key [\"id\"]"]),
                    listed(&[
                        "id BIGINT",
                        "full_name VARCHAR",
                        "email VARCHAR",
                        "key [\"id\"]"
                    ]),
                ),
            ]
        );
    }

    #[test]
    fn a_column_renamed_and_retyped_by_one_statement_is_retyped_under_its_new_name() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(
            vec![identifier(), column("full_name", "TEXT", ColumnType::Text)],
            &["id"],
        );

        let (steps, _) = statement_steps(
            before,
            vec![renaming("name", "full_name"), TableChange::Altered(after)],
        );

        assert_eq!(
            steps.announcements(),
            ["renamed name to full_name", "retyped full_name"]
        );
    }

    #[test]
    fn a_renamed_key_column_keeps_the_key_on_it_without_a_key_change() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(
            vec![column("customer_id", "BIGINT", ColumnType::Int64), name()],
            &["customer_id"],
        );

        let (steps, _) = statement_steps(
            before,
            vec![renaming("id", "customer_id"), TableChange::Altered(after)],
        );

        assert_eq!(steps.announcements(), ["renamed id to customer_id"]);
    }

    #[test]
    fn a_column_renamed_twice_in_one_statement_ends_under_its_last_name() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier(), nickname()], &["id"]);

        let (steps, _) = statement_steps(
            before,
            vec![
                renaming("name", "full_name"),
                renaming("full_name", "nickname"),
                TableChange::Altered(after),
            ],
        );

        assert_eq!(
            steps.announcements(),
            ["renamed name to full_name", "renamed full_name to nickname"]
        );
    }

    #[test]
    fn a_rename_of_a_column_the_table_never_had_announces_nothing() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier(), name()], &["id"]);

        let (steps, _) = statement_steps(
            before,
            vec![renaming("nickname", "alias"), TableChange::Altered(after)],
        );

        assert!(steps.taken.is_empty());
    }

    #[test]
    fn a_renamed_table_is_announced_and_remembered_under_its_new_name_alone() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = before.with_id(&clients());

        let (steps, catalog) = statement_steps(
            before.clone(),
            vec![
                TableChange::Renamed {
                    from: customers(),
                    to: clients(),
                },
                TableChange::Altered(after.clone()),
            ],
        );

        assert_eq!(
            steps.taken,
            [Step {
                announced: "renamed shop.customers to shop.clients".to_owned(),
                before,
                after: after.clone()
            }]
        );
        assert_eq!(catalog.table(&customers()), None);
        assert_eq!(catalog.table(&clients()), Some(&after));
    }

    #[test]
    fn a_table_renamed_and_altered_by_one_statement_is_altered_under_its_new_name() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier(), name(), email()], &["id"]).with_id(&clients());

        let (steps, _) = statement_steps(
            before,
            vec![
                TableChange::Renamed {
                    from: customers(),
                    to: clients(),
                },
                TableChange::Altered(after.clone()),
            ],
        );

        assert_eq!(
            steps.announcements(),
            ["renamed shop.customers to shop.clients", "added email"]
        );
        assert_eq!(steps.taken[1].after, after);
    }

    #[test]
    fn a_rename_of_a_table_the_catalog_never_saw_leaves_the_alteration_to_create_it() {
        let mut catalog = CatalogModel::default();
        let after = table(vec![identifier()], &["id"]).with_id(&clients());

        let entries = accept(
            &mut catalog,
            change(
                1,
                vec![
                    TableChange::Renamed {
                        from: customers(),
                        to: clients(),
                    },
                    TableChange::Altered(after),
                ],
            ),
        );

        assert_eq!(entries, ["created shop.clients"]);
    }

    #[test]
    fn a_rename_older_than_the_table_it_renames_is_rejected() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(2));

        assert_eq!(
            catalog
                .accept::<OutOfOrder, _>(
                    &(),
                    &change(
                        1,
                        vec![TableChange::Renamed {
                            from: customers(),
                            to: clients(),
                        }]
                    ),
                    &mut Journal::default()
                )
                .unwrap_err(),
            OutOfOrder { table: customers() }
        );
    }

    #[test]
    fn a_rename_onto_a_table_changed_later_is_rejected() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(1));
        accept(
            &mut catalog,
            change(
                3,
                vec![TableChange::Created(
                    table(vec![identifier()], &["id"]).with_id(&clients()),
                )],
            ),
        );

        assert_eq!(
            catalog
                .accept::<OutOfOrder, _>(
                    &(),
                    &change(
                        2,
                        vec![TableChange::Renamed {
                            from: customers(),
                            to: clients(),
                        }]
                    ),
                    &mut Journal::default()
                )
                .unwrap_err(),
            OutOfOrder { table: clients() }
        );
    }

    #[test]
    fn a_rename_of_a_column_onto_a_name_the_table_holds_announces_nothing() {
        let before = table(vec![identifier(), name(), email()], &["id"]);

        let (steps, catalog) = statement_steps(before.clone(), vec![renaming("name", "email")]);

        assert!(steps.taken.is_empty());
        assert_eq!(catalog.table(&customers()), Some(&before));
    }

    #[test]
    fn a_rename_of_a_column_of_a_table_the_catalog_never_saw_announces_nothing() {
        let mut catalog = CatalogModel::default();

        let entries = accept(&mut catalog, change(1, vec![renaming("name", "full_name")]));

        assert!(entries.is_empty());
        assert_eq!(catalog.table(&customers()), None);
    }

    #[derive(Clone, Default)]
    struct Traces(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Traces {
        fn write(&mut self, written: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(written);

            Ok(written.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Traces {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn warned(tables: Vec<TableChange>) -> String {
        let traces = Traces::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(traces.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let mut catalog = CatalogModel::default();
        accept(
            &mut catalog,
            change(
                1,
                vec![TableChange::Created(table(
                    vec![identifier(), name()],
                    &["id"],
                ))],
            ),
        );

        tracing::subscriber::with_default(subscriber, || {
            catalog
                .accept::<OutOfOrder, _>(
                    &(),
                    &SchemaChange::new(
                        Offset(2),
                        RecordedAt::from(2),
                        "ALTER TABLE customers DROP name, ADD full_name VARCHAR(64)",
                        tables,
                    ),
                    &mut (),
                )
                .unwrap();
        });

        String::from_utf8(traces.0.lock().unwrap().clone()).unwrap()
    }

    #[test]
    fn an_alteration_that_drops_and_adds_columns_warns_of_a_rename_it_may_hide() {
        let warning = warned(vec![TableChange::Altered(table(
            vec![identifier(), full_name()],
            &["id"],
        ))]);

        assert!(warning.contains("WARN"));
        assert!(warning.contains("table=shop.customers"));
        assert!(warning.contains("dropped=[\"name\"]"));
        assert!(warning.contains("added=[\"full_name\"]"));
        assert!(warning.contains("ALTER TABLE customers DROP name, ADD full_name VARCHAR(64)"));
        assert!(warning.contains("by hand"));
    }

    #[test]
    fn an_alteration_that_only_adds_columns_warns_of_nothing() {
        assert_eq!(
            warned(vec![TableChange::Altered(table(
                vec![identifier(), name(), email()],
                &["id"],
            ))]),
            ""
        );
    }

    #[test]
    fn an_alteration_whose_rename_was_read_warns_of_nothing() {
        assert_eq!(
            warned(vec![
                renaming("name", "full_name"),
                TableChange::Altered(table(vec![identifier(), full_name()], &["id"])),
            ]),
            ""
        );
    }

    #[derive(Debug, Default)]
    struct Appearances {
        told: Vec<(TableId, Appearance)>,
    }

    impl SchemaChangeListener<Offset> for Appearances {
        type Context = ();
        type Error = Infallible;

        fn table_created(&mut self, _: &(), event: &TableCreated<'_>) -> Result<(), Infallible> {
            self.told
                .push((event.table().id().clone(), event.appearance()));

            Ok(())
        }
    }

    fn appearances(changes: Vec<SchemaChange<Offset>>) -> Vec<(TableId, Appearance)> {
        let mut catalog = CatalogModel::default();
        let mut appearances = Appearances::default();

        for change in &changes {
            catalog
                .accept::<OutOfOrder, _>(&(), change, &mut appearances)
                .unwrap();
        }

        appearances.told
    }

    #[test]
    fn a_table_created_at_the_source_is_announced_as_created() {
        assert_eq!(
            appearances(vec![created(1)]),
            [(customers(), Appearance::Created)]
        );
    }

    #[test]
    fn a_table_the_snapshot_of_a_connector_took_is_announced_as_captured() {
        assert_eq!(
            appearances(vec![created(1).captured()]),
            [(customers(), Appearance::Captured)]
        );
    }

    #[test]
    fn a_table_first_seen_altered_is_announced_as_discovered() {
        assert_eq!(
            appearances(vec![altered(1, vec![identifier()], &["id"])]),
            [(customers(), Appearance::Discovered)]
        );
    }

    #[test]
    fn a_table_first_seen_renamed_onto_is_announced_as_discovered() {
        assert_eq!(
            appearances(vec![change(
                1,
                vec![
                    TableChange::Renamed {
                        from: customers(),
                        to: clients(),
                    },
                    TableChange::Altered(table(vec![identifier()], &["id"]).with_id(&clients())),
                ]
            )]),
            [(clients(), Appearance::Discovered)]
        );
    }

    #[test]
    fn a_table_created_or_captured_is_logged_from_its_start_but_one_discovered_is_not() {
        assert!(Appearance::Created.is_logged_from_start());
        assert!(Appearance::Captured.is_logged_from_start());
        assert!(!Appearance::Discovered.is_logged_from_start());
    }

    #[test]
    fn a_drop_of_a_table_the_catalog_never_saw_announces_nothing_yet_rejects_changes_older_than_it()
    {
        let mut catalog = CatalogModel::default();

        let entries = accept(
            &mut catalog,
            change(2, vec![TableChange::Dropped(customers())]),
        );

        assert!(entries.is_empty());
        assert_eq!(
            catalog
                .accept::<OutOfOrder, _>(&(), &created(1), &mut Journal::default())
                .unwrap_err(),
            OutOfOrder { table: customers() }
        );
    }

    fn orders() -> TableId {
        TableId::new("shop", "orders")
    }

    #[test]
    fn a_drop_of_a_table_the_catalog_knows_warns_of_nothing() {
        assert_eq!(warned(vec![TableChange::Dropped(customers())]), "");
    }

    #[test]
    fn a_drop_of_a_table_the_catalog_never_saw_warns_of_it() {
        let warning = warned(vec![TableChange::Dropped(orders())]);

        assert!(warning.contains("WARN"));
        assert!(warning.contains("table=shop.orders"));
    }

    #[test]
    fn a_rename_of_a_table_the_catalog_never_saw_warns_of_it() {
        let warning = warned(vec![TableChange::Renamed {
            from: orders(),
            to: clients(),
        }]);

        assert!(warning.contains("WARN"));
        assert!(warning.contains("table=shop.orders"));
        assert!(warning.contains("to=shop.clients"));
    }

    #[test]
    fn a_rename_of_a_column_the_table_never_had_warns_of_it() {
        let warning = warned(vec![renaming("nickname", "alias")]);

        assert!(warning.contains("WARN"));
        assert!(warning.contains("table=shop.customers"));
        assert!(warning.contains("column=\"nickname\""));
    }

    #[test]
    fn a_rename_of_a_column_onto_a_name_the_table_holds_warns_of_it() {
        let warning = warned(vec![renaming("name", "id")]);

        assert!(warning.contains("WARN"));
        assert!(warning.contains("column=\"name\""));
        assert!(warning.contains("to=\"id\""));
    }

    #[test]
    fn a_rename_of_a_column_of_a_table_the_catalog_never_saw_warns_of_it() {
        let warning = warned(vec![TableChange::ColumnRenamed {
            table: orders(),
            from: "note".to_owned(),
            to: "remark".to_owned(),
        }]);

        assert!(warning.contains("WARN"));
        assert!(warning.contains("table=shop.orders"));
        assert!(warning.contains("column=\"note\""));
    }
}
