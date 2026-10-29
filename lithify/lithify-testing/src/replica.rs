use std::collections::{BTreeMap, BTreeSet};

use lithify_core::model::{
    ColumnModel, ColumnName, ColumnType, PrimaryKeyColumn, TableId, TableModel,
};
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

pub type Row = BTreeMap<ColumnName, CellValue>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CellValue {
    Null,
    Int(i64),
    Text(String),
}

impl CellValue {
    pub fn cast(&self, column_type: &ColumnType) -> Self {
        match (self, column_type) {
            (Self::Null, _) => Self::Null,
            (Self::Text(text), ColumnType::Int16 | ColumnType::Int32 | ColumnType::Int64) => {
                text.trim().parse().map_or(Self::Null, Self::Int)
            }
            (Self::Int(number), ColumnType::Text) => Self::Text(number.to_string()),
            (held, _) => held.clone(),
        }
    }
}

impl From<i64> for CellValue {
    fn from(number: i64) -> Self {
        Self::Int(number)
    }
}

impl From<&str> for CellValue {
    fn from(text: &str) -> Self {
        Self::Text(text.to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    Dml(Dml),
    Ddl(Ddl),
}

impl From<Dml> for Mutation {
    fn from(dml: Dml) -> Self {
        Self::Dml(dml)
    }
}

impl From<Ddl> for Mutation {
    fn from(ddl: Ddl) -> Self {
        Self::Ddl(ddl)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dml {
    Insert { table: TableId, row: Row },
    Update { table: TableId, row: Row },
    Delete { table: TableId, row: Row },
}

impl Dml {
    pub fn table(&self) -> &TableId {
        match self {
            Self::Insert { table, .. }
            | Self::Update { table, .. }
            | Self::Delete { table, .. } => table,
        }
    }

    pub fn row(&self) -> &Row {
        match self {
            Self::Insert { row, .. } | Self::Update { row, .. } | Self::Delete { row, .. } => row,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ddl {
    Create(TableModel),
    Drop(TableId),
    Rename {
        from: TableId,
        to: TableId,
    },
    Alter {
        table: TableId,
        renamed: Option<TableId>,
        changes: Vec<ColumnDdl>,
    },
}

impl Ddl {
    fn pass(&self, passages: &mut Vec<(Option<TableId>, Option<TableId>)>) {
        let (table, passed) = match self {
            Self::Create(model) => return passages.push((None, Some(model.id().clone()))),
            Self::Drop(table) => (table, None),
            Self::Rename { from, to }
            | Self::Alter {
                table: from,
                renamed: Some(to),
                ..
            } => (from, Some(to.clone())),
            Self::Alter { renamed: None, .. } => return,
        };

        if let Some((_, standing)) = passages
            .iter_mut()
            .find(|(_, standing)| standing.as_ref() == Some(table))
        {
            *standing = passed;
        }
    }

    pub fn table(&self) -> &TableId {
        match self {
            Self::Create(table) => table.id(),
            Self::Drop(table) | Self::Rename { from: table, .. } | Self::Alter { table, .. } => {
                table
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnDdl {
    Add {
        column: ColumnModel,
        placed: Placed,
    },
    Drop(ColumnName),
    Rename {
        from: ColumnName,
        to: ColumnName,
    },
    Change {
        from: ColumnName,
        column: ColumnModel,
        placed: Placed,
    },
    Rekey(PrimaryKeyColumn),
}

impl ColumnDdl {
    pub fn add(column: ColumnModel) -> Self {
        Self::Add {
            column,
            placed: Placed::Last,
        }
    }

    pub fn drop(name: &str) -> Self {
        Self::Drop(name.into())
    }

    pub fn rename(from: &str, to: &str) -> Self {
        Self::Rename {
            from: from.into(),
            to: to.into(),
        }
    }

    pub fn modify(column: ColumnModel) -> Self {
        Self::Change {
            from: column.name().clone(),
            column,
            placed: Placed::Kept,
        }
    }

    pub fn change(from: &str, column: ColumnModel) -> Self {
        Self::Change {
            from: from.into(),
            column,
            placed: Placed::Kept,
        }
    }

    pub fn rekey(key: &[&str]) -> Self {
        Self::Rekey(key.iter().map(|&column| column.into()).collect())
    }

    pub fn placed(self, at: Placed) -> Self {
        match self {
            Self::Add { column, .. } => Self::Add { column, placed: at },
            Self::Change { from, column, .. } => Self::Change {
                from,
                column,
                placed: at,
            },
            unplaced @ (Self::Drop(_) | Self::Rename { .. } | Self::Rekey(_)) => unplaced,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placed {
    Kept,
    Last,
    First,
    After(ColumnName),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Replica {
    mutations: Vec<(Ts<Event>, Mutation)>,
}

impl Replica {
    pub fn create(self, at: Ts<Event>, table: TableModel) -> Self {
        self.mutated(at, Ddl::Create(table))
    }

    pub fn drop(self, at: Ts<Event>, table: &TableId) -> Self {
        self.mutated(at, Ddl::Drop(table.clone()))
    }

    pub fn rename(self, at: Ts<Event>, from: &TableId, to: &TableId) -> Self {
        self.mutated(
            at,
            Ddl::Rename {
                from: from.clone(),
                to: to.clone(),
            },
        )
    }

    pub fn alter(self, at: Ts<Event>, table: &TableId, changes: Vec<ColumnDdl>) -> Self {
        self.mutated(
            at,
            Ddl::Alter {
                table: table.clone(),
                renamed: None,
                changes,
            },
        )
    }

    pub fn alter_into(
        self,
        at: Ts<Event>,
        table: &TableId,
        renamed: &TableId,
        changes: Vec<ColumnDdl>,
    ) -> Self {
        self.mutated(
            at,
            Ddl::Alter {
                table: table.clone(),
                renamed: Some(renamed.clone()),
                changes,
            },
        )
    }

    pub fn insert(self, at: Ts<Event>, table: &TableId, row: &[(&str, CellValue)]) -> Self {
        self.mutated(
            at,
            Dml::Insert {
                table: table.clone(),
                row: Self::row(row),
            },
        )
    }

    pub fn update(self, at: Ts<Event>, table: &TableId, row: &[(&str, CellValue)]) -> Self {
        self.mutated(
            at,
            Dml::Update {
                table: table.clone(),
                row: Self::row(row),
            },
        )
    }

    pub fn delete(self, at: Ts<Event>, table: &TableId, key: &[(&str, CellValue)]) -> Self {
        self.mutated(
            at,
            Dml::Delete {
                table: table.clone(),
                row: Self::row(key),
            },
        )
    }

    pub fn passages(&self, window: &Window) -> BTreeSet<(Option<TableId>, Option<TableId>)> {
        let mut passages: Vec<_> = self
            .dump(*window.from())
            .tables
            .keys()
            .map(|table| (Some(table.clone()), Some(table.clone())))
            .collect();

        for (_, mutation) in self
            .mutations
            .iter()
            .filter(|(at, _)| window.range().contains(at))
        {
            if let Mutation::Ddl(ddl) = mutation {
                ddl.pass(&mut passages);
            }
        }

        passages.into_iter().collect()
    }

    pub fn mutations(&self) -> &[(Ts<Event>, Mutation)] {
        &self.mutations
    }

    pub fn dump(&self, at: Ts<Event>) -> ReplicaDump {
        self.mutations
            .iter()
            .take_while(|(mutated, _)| *mutated < at)
            .fold(ReplicaDump::default(), |mut dump, (_, mutation)| {
                dump.apply(mutation);
                dump
            })
    }

    pub fn mutated(mut self, at: Ts<Event>, mutation: impl Into<Mutation>) -> Self {
        assert!(
            self.mutations.last().is_none_or(|(last, _)| *last <= at),
            "a replica is mutated in the order of time"
        );
        self.mutations.push((at, mutation.into()));

        self
    }

    fn row(cells: &[(&str, CellValue)]) -> Row {
        cells
            .iter()
            .map(|(column, value)| ((*column).into(), value.clone()))
            .collect()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicaDump {
    tables: BTreeMap<TableId, TableRows>,
}

impl ReplicaDump {
    pub fn table(&self, id: &TableId) -> Option<&TableModel> {
        self.tables.get(id).map(|table| &table.model)
    }

    pub fn rows(&self, id: &TableId) -> Option<Vec<Row>> {
        self.tables
            .get(id)
            .map(|table| table.rows.values().cloned().collect())
    }

    pub fn row(&self, id: &TableId, key: &Row) -> Option<&Row> {
        let table = self.tables.get(id)?;

        table
            .rows
            .get(&PrimaryKeyValue::of(table.model.primary_key(), key))
    }

    pub fn apply(&mut self, mutation: &Mutation) {
        match mutation {
            Mutation::Dml(dml) => self.apply_dml(dml),
            Mutation::Ddl(ddl) => self.apply_ddl(ddl),
        }
    }

    fn apply_dml(&mut self, dml: &Dml) {
        let table = self
            .tables
            .get_mut(dml.table())
            .unwrap_or_else(|| panic!("{} is changed before it is created", dml.table()));
        let key = PrimaryKeyValue::of(table.model.primary_key(), dml.row());

        match dml {
            Dml::Insert { row, .. } | Dml::Update { row, .. } => {
                table.rows.insert(key, table.typed(row));
            }
            Dml::Delete { .. } => {
                table.rows.remove(&key);
            }
        }
    }

    fn apply_ddl(&mut self, ddl: &Ddl) {
        match ddl {
            Ddl::Create(model) => {
                self.tables
                    .insert(model.id().clone(), TableRows::new(model.clone()));
            }
            Ddl::Drop(id) => {
                self.tables.remove(id);
            }
            Ddl::Rename { from, to } => self.moved(from, to),
            Ddl::Alter {
                table,
                renamed,
                changes,
            } => {
                let altered = self
                    .tables
                    .get_mut(table)
                    .unwrap_or_else(|| panic!("{table} is altered before it is created"));
                for change in changes {
                    altered.alter(change);
                }
                if let Some(renamed) = renamed {
                    self.moved(table, renamed);
                }
            }
        }
    }

    fn moved(&mut self, from: &TableId, to: &TableId) {
        let mut table = self
            .tables
            .remove(from)
            .unwrap_or_else(|| panic!("{from} is renamed before it is created"));
        table.model = table.model.with_id(to);
        self.tables.insert(to.clone(), table);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PrimaryKeyValue(Vec<CellValue>);

impl PrimaryKeyValue {
    fn of(key: &PrimaryKeyColumn, row: &Row) -> Self {
        Self(
            key.columns()
                .map(|column| row.get(column).cloned().unwrap_or(CellValue::Null))
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableRows {
    model: TableModel,
    rows: BTreeMap<PrimaryKeyValue, Row>,
}

impl TableRows {
    fn new(model: TableModel) -> Self {
        Self {
            model,
            rows: BTreeMap::new(),
        }
    }

    fn alter(&mut self, change: &ColumnDdl) {
        let mut columns: Vec<ColumnModel> = self.model.columns().cloned().collect();
        let mut key = self.model.primary_key().clone();

        match change {
            ColumnDdl::Add { column, placed } => {
                let default = column
                    .default_value()
                    .map_or(CellValue::Null, CellValue::from)
                    .cast(column.column_type());
                for row in self.rows.values_mut() {
                    row.insert(column.name().clone(), default.clone());
                }
                let last = columns.len();
                placed.place(&mut columns, column.clone(), last);
            }
            ColumnDdl::Drop(name) => {
                columns.retain(|column| column.name() != name);
                for row in self.rows.values_mut() {
                    row.remove(name);
                }
            }
            ColumnDdl::Rename { from, to } => {
                for column in columns.iter_mut().filter(|column| column.name() == from) {
                    *column = Self::renamed(column, to);
                }
                self.carry(from, to);
                key = Self::renamed_key(&key, from, to);
            }
            ColumnDdl::Change {
                from,
                column,
                placed,
            } => {
                let at = columns
                    .iter()
                    .position(|held| held.name() == from)
                    .unwrap_or_else(|| panic!("{from} is changed before it is added"));
                columns.remove(at);
                placed.place(&mut columns, column.clone(), at);
                self.carry(from, column.name());
                for row in self.rows.values_mut() {
                    let value = row.remove(column.name()).unwrap_or(CellValue::Null);
                    row.insert(column.name().clone(), value.cast(column.column_type()));
                }
                key = Self::renamed_key(&key, from, column.name());
            }
            ColumnDdl::Rekey(rekeyed) => key = rekeyed.clone(),
        }

        self.model = TableModel::new(self.model.id().clone(), columns, key);
        self.rows = std::mem::take(&mut self.rows)
            .into_values()
            .map(|row| (PrimaryKeyValue::of(self.model.primary_key(), &row), row))
            .collect();
    }

    fn typed(&self, row: &Row) -> Row {
        self.model
            .columns()
            .map(|column| {
                let value = row.get(column.name()).unwrap_or(&CellValue::Null);

                (column.name().clone(), value.cast(column.column_type()))
            })
            .collect()
    }

    fn renamed(column: &ColumnModel, to: &ColumnName) -> ColumnModel {
        ColumnModel::new(
            to.as_str(),
            column.type_name(),
            column.column_type().clone(),
            column.is_optional(),
            column.default_value(),
        )
        .with_comment(column.comment())
    }

    fn renamed_key(key: &PrimaryKeyColumn, from: &ColumnName, to: &ColumnName) -> PrimaryKeyColumn {
        key.columns()
            .map(|column| if column == from { to } else { column })
            .cloned()
            .collect()
    }

    fn carry(&mut self, from: &ColumnName, to: &ColumnName) {
        for row in self.rows.values_mut() {
            if let Some(value) = row.remove(from) {
                row.insert(to.clone(), value);
            }
        }
    }
}

impl Placed {
    fn place(&self, columns: &mut Vec<ColumnModel>, column: ColumnModel, kept: usize) {
        let at = match self {
            Self::Kept => kept,
            Self::Last => columns.len(),
            Self::First => 0,
            Self::After(name) => {
                columns
                    .iter()
                    .position(|held| held.name() == name)
                    .unwrap_or_else(|| panic!("{name} is placed after before it is added"))
                    + 1
            }
        };
        columns.insert(at, column);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(millis: i64) -> Ts<Event> {
        Ts::try_from(millis).unwrap()
    }

    fn people() -> TableId {
        TableId::new("shop", "people")
    }

    fn persons() -> TableId {
        TableId::new("shop", "persons")
    }

    fn id() -> ColumnModel {
        ColumnModel::new("id", "BIGINT", ColumnType::Int64, false, None)
    }

    fn text(name: &str) -> ColumnModel {
        ColumnModel::new(name, "VARCHAR", ColumnType::Text, true, None)
    }

    fn created() -> Replica {
        Replica::default().create(
            at(0),
            TableModel::new(people(), vec![id(), text("name")], ["id"].into()),
        )
    }

    fn replica() -> Replica {
        created()
            .insert(
                at(1),
                &people(),
                &[("id", 1.into()), ("name", "Alice".into())],
            )
            .insert(
                at(2),
                &people(),
                &[("id", 2.into()), ("name", "Bob".into())],
            )
            .update(
                at(3),
                &people(),
                &[("id", 1.into()), ("name", "Alicia".into())],
            )
            .delete(at(4), &people(), &[("id", 2.into())])
    }

    fn row(cells: &[(&str, CellValue)]) -> Row {
        Replica::row(cells)
    }

    fn names(replica: &Replica, instant: i64, table: &TableId) -> Vec<String> {
        replica
            .dump(at(instant))
            .table(table)
            .unwrap()
            .columns()
            .map(|column| column.name().to_string())
            .collect()
    }

    fn pets() -> TableId {
        TableId::new("shop", "pets")
    }

    fn passage(from: Option<TableId>, to: Option<TableId>) -> (Option<TableId>, Option<TableId>) {
        (from, to)
    }

    #[test]
    fn a_table_passes_through_a_window_it_was_not_touched_in_under_its_own_name() {
        assert_eq!(
            replica().passages(&Window::new(at(1), at(5))),
            [passage(Some(people()), Some(people()))].into()
        );
    }

    #[test]
    fn a_table_renamed_within_a_window_passes_from_its_old_name_to_its_new_one() {
        let replica = created().rename(at(2), &people(), &persons()).alter_into(
            at(3),
            &persons(),
            &pets(),
            vec![],
        );

        assert_eq!(
            replica.passages(&Window::new(at(1), at(5))),
            [passage(Some(people()), Some(pets()))].into()
        );
    }

    #[test]
    fn a_table_created_or_dropped_within_a_window_passes_from_or_into_nothing() {
        let replica = created()
            .drop(at(2), &people())
            .create(at(3), TableModel::new(pets(), vec![id()], ["id"].into()));

        assert_eq!(
            replica.passages(&Window::new(at(1), at(5))),
            [passage(None, Some(pets())), passage(Some(people()), None)].into()
        );
    }

    #[test]
    fn a_table_created_and_dropped_within_a_window_passes_from_nothing_into_nothing() {
        let replica = Replica::default()
            .create(at(2), TableModel::new(pets(), vec![id()], ["id"].into()))
            .drop(at(3), &pets());

        assert_eq!(
            replica.passages(&Window::new(at(1), at(5))),
            [passage(None, None)].into()
        );
    }

    #[test]
    fn a_replica_holds_each_key_as_the_last_change_before_an_instant_left_it() {
        assert_eq!(
            replica().dump(at(4)).rows(&people()),
            Some(vec![
                row(&[("id", 1.into()), ("name", "Alicia".into())]),
                row(&[("id", 2.into()), ("name", "Bob".into())]),
            ])
        );
    }

    #[test]
    fn a_replica_holds_nothing_of_a_change_at_the_instant_it_is_read_at() {
        assert_eq!(
            replica().dump(at(3)).rows(&people()),
            Some(vec![
                row(&[("id", 1.into()), ("name", "Alice".into())]),
                row(&[("id", 2.into()), ("name", "Bob".into())]),
            ])
        );
    }

    #[test]
    fn a_deleted_key_leaves_a_replica() {
        assert_eq!(
            replica().dump(at(5)).rows(&people()),
            Some(vec![row(&[("id", 1.into()), ("name", "Alicia".into())])])
        );
    }

    #[test]
    fn a_row_holds_every_column_of_its_table_even_one_left_unsaid() {
        let replica = created().insert(at(1), &people(), &[("id", 1.into())]);

        assert_eq!(
            replica.dump(at(2)).rows(&people()),
            Some(vec![row(&[("id", 1.into()), ("name", CellValue::Null)])])
        );
    }

    #[test]
    fn a_table_is_held_from_its_creation_until_its_drop() {
        let replica = replica().drop(at(5), &people());

        assert!(replica.dump(at(0)).table(&people()).is_none());
        assert!(replica.dump(at(5)).table(&people()).is_some());
        assert!(replica.dump(at(6)).table(&people()).is_none());
    }

    #[test]
    fn a_table_renamed_carries_its_rows_under_its_new_name() {
        let replica = replica().rename(at(5), &people(), &persons());

        assert_eq!(replica.dump(at(6)).rows(&people()), None);
        assert_eq!(
            replica.dump(at(6)).rows(&persons()),
            replica.dump(at(5)).rows(&people())
        );
    }

    #[test]
    fn a_table_renamed_and_altered_by_one_statement_is_altered_under_its_new_name() {
        let replica = replica().alter_into(
            at(5),
            &people(),
            &persons(),
            vec![ColumnDdl::add(text("tier"))],
        );

        assert_eq!(names(&replica, 6, &persons()), ["id", "name", "tier"]);
    }

    #[test]
    fn a_column_added_fills_the_rows_already_there_with_its_default() {
        let tier = ColumnModel::new("tier", "VARCHAR", ColumnType::Text, true, Some("gold"));
        let replica = replica().alter(at(5), &people(), vec![ColumnDdl::add(tier)]);

        assert_eq!(
            replica.dump(at(6)).rows(&people()),
            Some(vec![row(&[
                ("id", 1.into()),
                ("name", "Alicia".into()),
                ("tier", "gold".into())
            ])])
        );
    }

    #[test]
    fn a_column_added_lands_where_the_statement_placed_it() {
        let replica = created()
            .alter(
                at(1),
                &people(),
                vec![ColumnDdl::add(text("tier")).placed(Placed::First)],
            )
            .alter(
                at(2),
                &people(),
                vec![ColumnDdl::add(text("email")).placed(Placed::After("id".into()))],
            );

        assert_eq!(
            names(&replica, 3, &people()),
            ["tier", "id", "email", "name"]
        );
    }

    #[test]
    fn a_column_dropped_leaves_every_row() {
        let replica = replica().alter(at(5), &people(), vec![ColumnDdl::drop("name")]);

        assert_eq!(
            replica.dump(at(6)).rows(&people()),
            Some(vec![row(&[("id", 1.into())])])
        );
    }

    #[test]
    fn a_column_renamed_carries_its_values_under_its_new_name() {
        let replica = replica().alter(at(5), &people(), vec![ColumnDdl::rename("name", "label")]);

        assert_eq!(
            replica.dump(at(6)).rows(&people()),
            Some(vec![row(&[("id", 1.into()), ("label", "Alicia".into())])])
        );
    }

    #[test]
    fn a_column_retyped_casts_its_values_into_its_new_type() {
        let replica = created()
            .insert(
                at(1),
                &people(),
                &[("id", 1.into()), ("name", " 42".into())],
            )
            .insert(
                at(2),
                &people(),
                &[("id", 2.into()), ("name", "Bob".into())],
            )
            .alter(
                at(3),
                &people(),
                vec![ColumnDdl::modify(ColumnModel::new(
                    "name",
                    "BIGINT",
                    ColumnType::Int64,
                    true,
                    None,
                ))],
            );

        assert_eq!(
            replica.dump(at(4)).rows(&people()),
            Some(vec![
                row(&[("id", 1.into()), ("name", 42.into())]),
                row(&[("id", 2.into()), ("name", CellValue::Null)]),
            ])
        );
    }

    #[test]
    fn a_column_retyped_into_text_holds_its_numbers_as_text() {
        let replica = created()
            .insert(at(1), &people(), &[("id", 1.into())])
            .alter(at(2), &people(), vec![ColumnDdl::modify(text("id"))]);

        assert_eq!(
            replica.dump(at(3)).rows(&people()),
            Some(vec![row(&[("id", "1".into()), ("name", CellValue::Null)])])
        );
    }

    #[test]
    fn a_column_changed_is_renamed_retyped_and_moved_at_once() {
        let replica = replica().alter(
            at(5),
            &people(),
            vec![ColumnDdl::change("name", text("label")).placed(Placed::First)],
        );

        assert_eq!(names(&replica, 6, &people()), ["label", "id"]);
        assert_eq!(
            replica.dump(at(6)).rows(&people()),
            Some(vec![row(&[("id", 1.into()), ("label", "Alicia".into())])])
        );
    }

    #[test]
    fn a_renamed_key_column_keeps_the_key_on_it() {
        let replica = replica().alter(at(5), &people(), vec![ColumnDdl::rename("id", "person_id")]);

        assert_eq!(
            replica
                .dump(at(6))
                .table(&people())
                .unwrap()
                .primary_key()
                .columns()
                .collect::<Vec<_>>(),
            ["person_id"]
        );
    }

    #[test]
    fn a_table_rekeyed_holds_each_row_under_its_new_key() {
        let replica = replica()
            .alter(at(5), &people(), vec![ColumnDdl::rekey(&["name"])])
            .update(
                at(6),
                &people(),
                &[("id", 7.into()), ("name", "Alicia".into())],
            );

        assert_eq!(
            replica.dump(at(7)).rows(&people()),
            Some(vec![row(&[("id", 7.into()), ("name", "Alicia".into())])])
        );
    }

    #[test]
    fn a_row_is_found_by_its_key() {
        assert_eq!(
            replica()
                .dump(at(4))
                .row(&people(), &row(&[("id", 2.into())])),
            Some(&row(&[("id", 2.into()), ("name", "Bob".into())]))
        );
    }

    #[test]
    fn changes_of_one_instant_apply_in_the_order_they_were_made() {
        let replica = created()
            .insert(
                at(1),
                &people(),
                &[("id", 1.into()), ("name", "Alice".into())],
            )
            .update(
                at(1),
                &people(),
                &[("id", 1.into()), ("name", "Alicia".into())],
            );

        assert_eq!(
            replica.dump(at(2)).rows(&people()),
            Some(vec![row(&[("id", 1.into()), ("name", "Alicia".into())])])
        );
    }
}
