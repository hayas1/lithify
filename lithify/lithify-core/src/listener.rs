use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt;

use crate::era::Era;
use crate::event::{Hint, SchemaChange, TableChange};
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

    fn table_altered(
        &mut self,
        context: &Self::Context,
        event: &TableAltered<'_>,
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

pub enum Alteration<'a> {
    ColumnAdded(&'a ColumnModel),
    ColumnDropped(&'a ColumnModel),
    ColumnRenamed {
        previous: &'a ColumnModel,
        column: &'a ColumnModel,
    },
    ColumnRetyped {
        previous: &'a ColumnModel,
        column: &'a ColumnModel,
    },
    ColumnDescribed {
        previous: &'a ColumnModel,
        column: &'a ColumnModel,
    },
    PrimaryKeyChanged,
}

impl<'a> Alteration<'a> {
    fn between<P>(
        before: &'a TableModel,
        after: &'a TableModel,
        change: &'a SchemaChange<P>,
    ) -> Vec<Self> {
        let lineage = ColumnLineage::traced(before, after.id(), change);
        let dropped = lineage
            .carried(after)
            .filter(|(_, column)| column.is_none())
            .map(|(previous, _)| Self::ColumnDropped(previous));
        let renamed = lineage.carried(after).filter_map(|(previous, column)| {
            column
                .filter(|column| column.name() != previous.name())
                .map(|column| Self::ColumnRenamed { previous, column })
        });
        let retyped = lineage.carried(after).filter_map(|(previous, column)| {
            column
                .filter(|column| column.type_name() != previous.type_name())
                .map(|column| Self::ColumnRetyped { previous, column })
        });
        let described = lineage.carried(after).filter_map(|(previous, column)| {
            column
                .filter(|column| column.comment() != previous.comment())
                .map(|column| Self::ColumnDescribed { previous, column })
        });
        let added = after
            .columns()
            .filter(|column| !lineage.holds(column.name()))
            .map(Self::ColumnAdded);
        let rekeyed = before
            .primary_key()
            .map(|key| lineage.name_of(key))
            .ne(after.primary_key())
            .then_some(Self::PrimaryKeyChanged);

        dropped
            .chain(renamed)
            .chain(retyped)
            .chain(described)
            .chain(added)
            .chain(rekeyed)
            .collect()
    }

    fn warn_of_unread_rename<P>(alterations: &[Self], table: &TableId, change: &SchemaChange<P>) {
        let dropped: Vec<&str> = alterations
            .iter()
            .filter_map(Self::as_column_dropped)
            .map(ColumnModel::name)
            .collect();
        let added: Vec<&str> = alterations
            .iter()
            .filter_map(Self::as_column_added)
            .map(ColumnModel::name)
            .collect();

        if !dropped.is_empty() && !added.is_empty() {
            tracing::warn!(
                table = %table,
                ?dropped,
                ?added,
                statement = change.statement(),
                "one statement dropped and added columns; if it renamed a column the dialect could \
                 not read, the column it became is filled as an added one rather than with the \
                 values it had, so carry them over by hand from the shard the window folds onto \
                 and the change log before the statement"
            );
        }
    }

    pub fn as_column_added(&self) -> Option<&'a ColumnModel> {
        match self {
            Self::ColumnAdded(column) => Some(column),
            _ => None,
        }
    }

    pub fn as_column_dropped(&self) -> Option<&'a ColumnModel> {
        match self {
            Self::ColumnDropped(column) => Some(column),
            _ => None,
        }
    }

    pub fn as_column_renamed(&self) -> Option<(&'a ColumnModel, &'a ColumnModel)> {
        match self {
            Self::ColumnRenamed { previous, column } => Some((previous, column)),
            _ => None,
        }
    }

    pub fn as_column_retyped(&self) -> Option<(&'a ColumnModel, &'a ColumnModel)> {
        match self {
            Self::ColumnRetyped { previous, column } => Some((previous, column)),
            _ => None,
        }
    }

    pub fn as_column_described(&self) -> Option<(&'a ColumnModel, &'a ColumnModel)> {
        match self {
            Self::ColumnDescribed { previous, column } => Some((previous, column)),
            _ => None,
        }
    }

    pub fn is_metadata_only(&self) -> bool {
        match self {
            Self::ColumnDescribed { .. } => true,
            Self::ColumnAdded(_)
            | Self::ColumnDropped(_)
            | Self::ColumnRenamed { .. }
            | Self::ColumnRetyped { .. }
            | Self::PrimaryKeyChanged => false,
        }
    }

    pub fn is_primary_key_changed(&self) -> bool {
        matches!(self, Self::PrimaryKeyChanged)
    }
}

struct ColumnLineage<'a> {
    names: Vec<(&'a ColumnModel, &'a str)>,
}

impl<'a> ColumnLineage<'a> {
    fn traced<P>(before: &'a TableModel, table: &'a TableId, change: &'a SchemaChange<P>) -> Self {
        let mut names: Vec<_> = before
            .columns()
            .map(|column| (column, column.name()))
            .collect();

        for (from, to) in change.column_renames(table) {
            let renamed = names.iter().position(|&(_, name)| name == from);
            let held = names.iter().any(|&(_, name)| name == to);

            match (renamed, held) {
                (Some(at), false) => names[at].1 = to,
                _ => tracing::warn!(
                    table = %table,
                    column = from,
                    to,
                    statement = change.statement(),
                    "a statement renamed a column the table never had, or onto a name it already \
                     holds, so this rename is ignored and the column is matched by its name"
                ),
            }
        }

        Self { names }
    }

    fn carried(
        &self,
        after: &'a TableModel,
    ) -> impl Iterator<Item = (&'a ColumnModel, Option<&'a ColumnModel>)> + '_ {
        self.names
            .iter()
            .map(move |&(previous, name)| (previous, after.column(name)))
    }

    fn name_of(&self, previous: &'a str) -> &'a str {
        self.names
            .iter()
            .find(|(column, _)| column.name() == previous)
            .map_or(previous, |&(_, name)| name)
    }

    fn holds(&self, name: &str) -> bool {
        self.names.iter().any(|&(_, held)| held == name)
    }
}

pub struct TableAltered<'a> {
    before: &'a TableModel,
    after: &'a TableModel,
    alterations: Vec<Alteration<'a>>,
}

impl<'a> TableAltered<'a> {
    pub fn before(&self) -> &'a TableModel {
        self.before
    }

    pub fn after(&self) -> &'a TableModel {
        self.after
    }

    pub fn alterations(&self) -> &[Alteration<'a>] {
        &self.alterations
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
        let touched = change
            .tables()
            .map(TableChange::id)
            .chain(change.hints().flat_map(Hint::ids));
        for table in touched {
            self.guard_order(table, change.position())?;
        }

        listener.visit_statement(context, change)?;

        self.warn_of_unseen_column_renames(change);
        for (from, to) in change.hints().filter_map(Hint::as_table_rename) {
            self.rename(context, change, from, to, listener)?;
        }
        for table in change.tables() {
            match table {
                TableChange::Created(table) => {
                    self.create(context, change, table, Appearance::of(change), listener)?
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
        listener.table_created(context, &TableCreated { table, appearance })?;
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
            Some(table) => listener.table_dropped(context, &TableDropped { table })?,
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

        listener.table_renamed(
            context,
            &TableRenamed {
                before: &before,
                after: &after,
            },
        )?;
        self.release(change, from);
        self.remember(change, &after);

        Ok(())
    }

    fn warn_of_unseen_column_renames(&self, change: &SchemaChange<P>) {
        let unseen = change
            .hints()
            .filter_map(Hint::as_column_rename)
            .filter(|(table, ..)| !self.tables.contains_key(table));

        for (table, from, to) in unseen {
            tracing::warn!(
                table = %table,
                column = from,
                to,
                statement = change.statement(),
                "a statement renamed a column of a table the catalog never saw"
            );
        }
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
        let alterations = Alteration::between(&before, after, change);
        Alteration::warn_of_unread_rename(&alterations, after.id(), change);

        if !alterations.is_empty() {
            listener.table_altered(
                context,
                &TableAltered {
                    before: &before,
                    after,
                    alterations,
                },
            )?;
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
    use crate::event::{Hint, TableChange};
    use crate::model::{ColumnType, TableId};
    use crate::ts::Ts;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Offset(u64);

    impl Position for Offset {}

    impl Alteration<'_> {
        pub(crate) fn announcement(&self) -> String {
            match self {
                Self::ColumnAdded(column) => format!("added {}", column.name()),
                Self::ColumnDropped(column) => format!("dropped {}", column.name()),
                Self::ColumnRenamed { previous, column } => {
                    format!("renamed {} to {}", previous.name(), column.name())
                }
                Self::ColumnRetyped { column, .. } => format!("retyped {}", column.name()),
                Self::ColumnDescribed { column, .. } => format!("described {}", column.name()),
                Self::PrimaryKeyChanged => "rekeyed".to_owned(),
            }
        }
    }

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

        fn table_altered(&mut self, _: &(), event: &TableAltered<'_>) -> Result<(), Infallible> {
            let id = event.after().id();
            let entries = event
                .alterations()
                .iter()
                .map(|alteration| match alteration {
                    Alteration::ColumnAdded(column) => format!("added {id}.{}", column.name()),
                    Alteration::ColumnDropped(column) => format!("dropped {id}.{}", column.name()),
                    Alteration::ColumnRenamed { previous, column } => {
                        format!("renamed {id}.{} to {}", previous.name(), column.name())
                    }
                    Alteration::ColumnRetyped { previous, column } => format!(
                        "retyped {id}.{} {} -> {}",
                        column.name(),
                        previous.type_name(),
                        column.type_name()
                    ),
                    Alteration::ColumnDescribed { previous, column } => format!(
                        "described {id}.{} {:?} -> {:?}",
                        column.name(),
                        previous.comment(),
                        column.comment()
                    ),
                    Alteration::PrimaryKeyChanged => format!(
                        "primary key {id} {:?} -> {:?}",
                        event.before().primary_key().collect::<Vec<_>>(),
                        event.after().primary_key().collect::<Vec<_>>()
                    ),
                });
            self.entries.extend(entries);

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
        hinted(offset, Vec::new(), tables)
    }

    fn hinted(offset: u64, hints: Vec<Hint>, tables: Vec<TableChange>) -> SchemaChange<Offset> {
        SchemaChange::new(
            Offset(offset),
            Ts::try_from(offset as i64).unwrap(),
            "",
            tables,
            hints,
        )
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
    fn a_column_whose_comment_changed_is_announced_with_both_comments() {
        let mut catalog = CatalogModel::default();
        accept(&mut catalog, created(1));

        let entries = accept(
            &mut catalog,
            altered(
                2,
                vec![identifier(), name().with_comment(Some("how to call them"))],
                &["id"],
            ),
        );

        assert_eq!(
            entries,
            ["described shop.customers.name None -> Some(\"how to call them\")"]
        );
    }

    #[test]
    fn a_column_renamed_and_described_by_one_statement_is_announced_renamed_and_described() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(
            vec![identifier(), full_name().with_comment(Some("as printed"))],
            &["id"],
        );

        let (steps, _) = statement_steps(
            before,
            vec![renaming("name", "full_name")],
            vec![TableChange::Altered(after)],
        );

        assert_eq!(
            steps.announcements(),
            ["renamed name to full_name, described full_name"]
        );
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

        fn table_altered(&mut self, _: &(), event: &TableAltered<'_>) -> Result<(), Infallible> {
            let announced: Vec<String> = event
                .alterations()
                .iter()
                .map(Alteration::announcement)
                .collect();
            self.take(announced.join(", "), event.before(), event.after());

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
    fn an_alteration_of_several_changes_steps_once_from_the_table_before_it_to_the_table_after_it()
    {
        let (before, after) = reshaped();

        assert_eq!(
            steps(before.clone(), after.clone()).taken,
            [Step {
                announced: "dropped email, retyped name, added nickname, added phone, rekeyed"
                    .to_owned(),
                before,
                after
            }]
        );
    }

    #[test]
    fn a_column_replaced_by_one_of_its_name_is_announced_as_retyped() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(
            vec![identifier(), column("name", "TEXT", ColumnType::Text)],
            &["id"],
        );

        assert_eq!(steps(before, after).announcements(), ["retyped name"]);
    }

    #[test]
    fn what_no_alteration_lists_still_reaches_the_listener_in_the_table_after_it() {
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
    fn a_key_changed_alone_is_announced_as_rekeyed() {
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

    struct Refusing;

    impl SchemaChangeListener<Offset> for Refusing {
        type Context = ();
        type Error = Halted;

        fn table_altered(&mut self, _: &(), event: &TableAltered<'_>) -> Result<(), Halted> {
            Err(Halted::Refused(event.after().id().to_string()))
        }
    }

    #[test]
    fn an_alteration_its_listener_refuses_halts_and_is_not_remembered() {
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

        let halted = catalog.accept::<Halted, _>(
            &(),
            &change(2, vec![TableChange::Altered(after)]),
            &mut Refusing,
        );

        assert_eq!(halted, Err(Halted::Refused("shop.customers".to_owned())));
        assert_eq!(catalog.table(&customers()), Some(&before));
    }

    fn clients() -> TableId {
        TableId::new("shop", "clients")
    }

    fn renaming(from: &str, to: &str) -> Hint {
        Hint::ColumnRename {
            table: customers(),
            from: from.to_owned(),
            to: to.to_owned(),
        }
    }

    fn to_clients() -> Hint {
        Hint::TableRename {
            from: customers(),
            to: clients(),
        }
    }

    fn statement_steps(
        before: TableModel,
        hints: Vec<Hint>,
        tables: Vec<TableChange>,
    ) -> (Steps, CatalogModel<Offset>) {
        let mut catalog = CatalogModel::default();
        catalog
            .accept::<OutOfOrder, _>(&(), &change(1, vec![TableChange::Created(before)]), &mut ())
            .unwrap();
        let mut steps = Steps::default();
        catalog
            .accept::<OutOfOrder, _>(&(), &hinted(2, hints, tables), &mut steps)
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
            vec![renaming("name", "full_name")],
            vec![TableChange::Altered(after.clone())],
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
    fn a_column_renamed_and_another_added_by_one_statement_are_announced_in_one_step() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier(), full_name(), email()], &["id"]);

        let (steps, _) = statement_steps(
            before,
            vec![renaming("name", "full_name")],
            vec![TableChange::Altered(after)],
        );

        assert_eq!(
            steps.announcements(),
            ["renamed name to full_name, added email"]
        );
    }

    #[test]
    fn a_column_renamed_and_retyped_by_one_statement_is_announced_renamed_and_retyped() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(
            vec![identifier(), column("full_name", "TEXT", ColumnType::Text)],
            &["id"],
        );

        let (steps, _) = statement_steps(
            before,
            vec![renaming("name", "full_name")],
            vec![TableChange::Altered(after)],
        );

        assert_eq!(
            steps.announcements(),
            ["renamed name to full_name, retyped full_name"]
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
            vec![renaming("id", "customer_id")],
            vec![TableChange::Altered(after)],
        );

        assert_eq!(steps.announcements(), ["renamed id to customer_id"]);
    }

    #[test]
    fn a_column_renamed_twice_in_one_statement_is_announced_under_its_first_and_last_names() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier(), nickname()], &["id"]);

        let (steps, _) = statement_steps(
            before,
            vec![
                renaming("name", "full_name"),
                renaming("full_name", "nickname"),
            ],
            vec![TableChange::Altered(after)],
        );

        assert_eq!(steps.announcements(), ["renamed name to nickname"]);
    }

    #[test]
    fn a_rename_of_a_column_the_table_never_had_announces_nothing() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier(), name()], &["id"]);

        let (steps, _) = statement_steps(
            before,
            vec![renaming("nickname", "alias")],
            vec![TableChange::Altered(after)],
        );

        assert!(steps.taken.is_empty());
    }

    #[test]
    fn a_rename_whose_column_the_definition_after_it_lacks_is_announced_as_a_drop() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier()], &["id"]);

        let (steps, _) = statement_steps(
            before,
            vec![renaming("name", "full_name")],
            vec![TableChange::Altered(after)],
        );

        assert_eq!(steps.announcements(), ["dropped name"]);
    }

    #[test]
    fn a_renamed_table_is_announced_and_remembered_under_its_new_name_alone() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = before.with_id(&clients());

        let (steps, catalog) = statement_steps(
            before.clone(),
            vec![to_clients()],
            vec![TableChange::Altered(after.clone())],
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
            vec![to_clients()],
            vec![TableChange::Altered(after.clone())],
        );

        assert_eq!(
            steps.announcements(),
            ["renamed shop.customers to shop.clients", "added email"]
        );
        assert_eq!(steps.taken[1].after, after);
    }

    #[test]
    fn a_column_renamed_under_the_old_name_of_a_table_renamed_by_the_same_statement_is_announced() {
        let before = table(vec![identifier(), name()], &["id"]);
        let after = table(vec![identifier(), full_name()], &["id"]).with_id(&clients());

        let (steps, _) = statement_steps(
            before,
            vec![renaming("name", "full_name"), to_clients()],
            vec![TableChange::Altered(after)],
        );

        assert_eq!(
            steps.announcements(),
            [
                "renamed shop.customers to shop.clients",
                "renamed name to full_name"
            ]
        );
    }

    #[test]
    fn a_rename_of_a_table_the_catalog_never_saw_leaves_the_alteration_to_create_it() {
        let mut catalog = CatalogModel::default();
        let after = table(vec![identifier()], &["id"]).with_id(&clients());

        let entries = accept(
            &mut catalog,
            hinted(1, vec![to_clients()], vec![TableChange::Altered(after)]),
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
                    &hinted(1, vec![to_clients()], vec![]),
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
                    &hinted(2, vec![to_clients()], vec![]),
                    &mut Journal::default()
                )
                .unwrap_err(),
            OutOfOrder { table: clients() }
        );
    }

    #[test]
    fn a_rename_of_a_column_onto_a_name_the_table_holds_announces_nothing() {
        let before = table(vec![identifier(), name(), email()], &["id"]);

        let (steps, catalog) = statement_steps(
            before.clone(),
            vec![renaming("name", "email")],
            vec![TableChange::Altered(before.clone())],
        );

        assert!(steps.taken.is_empty());
        assert_eq!(catalog.table(&customers()), Some(&before));
    }

    #[test]
    fn a_rename_of_a_column_of_a_table_the_catalog_never_saw_announces_nothing() {
        let mut catalog = CatalogModel::default();

        let entries = accept(
            &mut catalog,
            hinted(1, vec![renaming("name", "full_name")], vec![]),
        );

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

    fn warned(hints: Vec<Hint>, tables: Vec<TableChange>) -> String {
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
                        Ts::try_from(2).unwrap(),
                        "ALTER TABLE customers DROP name, ADD full_name VARCHAR(64)",
                        tables,
                        hints,
                    ),
                    &mut (),
                )
                .unwrap();
        });

        String::from_utf8(traces.0.lock().unwrap().clone()).unwrap()
    }

    #[test]
    fn an_alteration_that_drops_and_adds_columns_warns_of_a_rename_it_may_hide() {
        let warning = warned(
            vec![],
            vec![TableChange::Altered(table(
                vec![identifier(), full_name()],
                &["id"],
            ))],
        );

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
            warned(
                vec![],
                vec![TableChange::Altered(table(
                    vec![identifier(), name(), email()],
                    &["id"],
                ))]
            ),
            ""
        );
    }

    #[test]
    fn an_alteration_whose_rename_was_read_warns_of_nothing() {
        assert_eq!(
            warned(
                vec![renaming("name", "full_name")],
                vec![TableChange::Altered(table(
                    vec![identifier(), full_name()],
                    &["id"]
                ))],
            ),
            ""
        );
    }

    #[test]
    fn an_alteration_whose_drop_and_add_no_hint_explains_still_warns_of_a_rename_it_may_hide() {
        let warning = warned(
            vec![renaming("name", "full_name")],
            vec![TableChange::Altered(table(
                vec![identifier(), nickname()],
                &["id"],
            ))],
        );

        assert!(warning.contains("dropped=[\"name\"]"));
        assert!(warning.contains("added=[\"nickname\"]"));
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
    fn a_table_the_catalog_knows_captured_again_is_announced_as_captured() {
        assert_eq!(
            appearances(vec![created(1), created(2).captured()]),
            [
                (customers(), Appearance::Created),
                (customers(), Appearance::Captured)
            ]
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
            appearances(vec![hinted(
                1,
                vec![to_clients()],
                vec![TableChange::Altered(
                    table(vec![identifier()], &["id"]).with_id(&clients())
                )]
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
        assert_eq!(warned(vec![], vec![TableChange::Dropped(customers())]), "");
    }

    #[test]
    fn a_drop_of_a_table_the_catalog_never_saw_warns_of_it() {
        let warning = warned(vec![], vec![TableChange::Dropped(orders())]);

        assert!(warning.contains("WARN"));
        assert!(warning.contains("table=shop.orders"));
    }

    #[test]
    fn a_rename_of_a_table_the_catalog_never_saw_warns_of_it() {
        let warning = warned(
            vec![Hint::TableRename {
                from: orders(),
                to: clients(),
            }],
            vec![],
        );

        assert!(warning.contains("WARN"));
        assert!(warning.contains("table=shop.orders"));
        assert!(warning.contains("to=shop.clients"));
    }

    #[test]
    fn a_rename_of_a_column_the_table_never_had_warns_of_it() {
        let warning = warned(
            vec![renaming("nickname", "alias")],
            vec![TableChange::Altered(table(
                vec![identifier(), name()],
                &["id"],
            ))],
        );

        assert!(warning.contains("WARN"));
        assert!(warning.contains("table=shop.customers"));
        assert!(warning.contains("column=\"nickname\""));
    }

    #[test]
    fn a_rename_of_a_column_onto_a_name_the_table_holds_warns_of_it() {
        let warning = warned(
            vec![renaming("name", "id")],
            vec![TableChange::Altered(table(
                vec![identifier(), name()],
                &["id"],
            ))],
        );

        assert!(warning.contains("WARN"));
        assert!(warning.contains("column=\"name\""));
        assert!(warning.contains("to=\"id\""));
    }

    #[test]
    fn a_rename_of_a_column_of_a_table_the_catalog_never_saw_warns_of_it() {
        let warning = warned(
            vec![Hint::ColumnRename {
                table: orders(),
                from: "note".to_owned(),
                to: "remark".to_owned(),
            }],
            vec![],
        );

        assert!(warning.contains("WARN"));
        assert!(warning.contains("table=shop.orders"));
        assert!(warning.contains("column=\"note\""));
    }
}
