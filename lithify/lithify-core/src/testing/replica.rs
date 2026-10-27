use std::collections::BTreeMap;

use crate::model::{ColumnModel, ColumnName, ColumnType, TableId, TableModel};
use crate::ts::{Event, Ts};

pub type Row = BTreeMap<ColumnName, Option<String>>;

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
    Rekey(Vec<ColumnName>),
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
    pub fn create(self, at: i64, table: TableModel) -> Self {
        self.mutated(at, Ddl::Create(table))
    }

    pub fn drop(self, at: i64, table: &TableId) -> Self {
        self.mutated(at, Ddl::Drop(table.clone()))
    }

    pub fn rename(self, at: i64, from: &TableId, to: &TableId) -> Self {
        self.mutated(
            at,
            Ddl::Rename {
                from: from.clone(),
                to: to.clone(),
            },
        )
    }

    pub fn alter(self, at: i64, table: &TableId, changes: Vec<ColumnDdl>) -> Self {
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
        at: i64,
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

    pub fn insert(self, at: i64, table: &TableId, row: &[(&str, Option<&str>)]) -> Self {
        self.mutated(
            at,
            Dml::Insert {
                table: table.clone(),
                row: Self::row(row),
            },
        )
    }

    pub fn update(self, at: i64, table: &TableId, row: &[(&str, Option<&str>)]) -> Self {
        self.mutated(
            at,
            Dml::Update {
                table: table.clone(),
                row: Self::row(row),
            },
        )
    }

    pub fn delete(self, at: i64, table: &TableId, key: &[(&str, Option<&str>)]) -> Self {
        self.mutated(
            at,
            Dml::Delete {
                table: table.clone(),
                row: Self::row(key),
            },
        )
    }

    pub fn mutations(&self) -> &[(Ts<Event>, Mutation)] {
        &self.mutations
    }

    pub fn at(&self, instant: Ts<Event>) -> ReplicaDump {
        self.mutations
            .iter()
            .take_while(|(at, _)| *at < instant)
            .fold(ReplicaDump::default(), |mut dump, (_, mutation)| {
                dump.apply(mutation);
                dump
            })
    }

    pub fn mutated(mut self, at: i64, mutation: impl Into<Mutation>) -> Self {
        let at = Ts::try_from(at).expect("a replica is mutated at an instant since the epoch");
        assert!(
            self.mutations.last().is_none_or(|(last, _)| *last <= at),
            "a replica is mutated in the order of time"
        );
        self.mutations.push((at, mutation.into()));

        self
    }

    fn row(cells: &[(&str, Option<&str>)]) -> Row {
        cells
            .iter()
            .map(|&(column, value)| (column.into(), value.map(str::to_owned)))
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

        table.rows.get(&PrimaryKeyValue::of(&table.model, key))
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
        let key = PrimaryKeyValue::of(&table.model, dml.row());

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
struct PrimaryKeyValue(Vec<Option<String>>);

impl PrimaryKeyValue {
    fn of(table: &TableModel, row: &Row) -> Self {
        Self(
            table
                .primary_key()
                .map(|column| row.get(column).cloned().flatten())
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
        let mut key: Vec<ColumnName> = self.model.primary_key().cloned().collect();

        match change {
            ColumnDdl::Add { column, placed } => {
                let default = column.default_value().map(str::to_owned);
                for row in self.rows.values_mut() {
                    row.insert(column.name().clone(), Self::cast(default.clone(), column));
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
                Self::rename_in(&mut key, from, to);
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
                    let value = row.remove(column.name()).flatten();
                    row.insert(column.name().clone(), Self::cast(value, column));
                }
                Self::rename_in(&mut key, from, column.name());
            }
            ColumnDdl::Rekey(rekeyed) => key = rekeyed.clone(),
        }

        self.model = TableModel::new(self.model.id().clone(), columns, key);
        self.rows = std::mem::take(&mut self.rows)
            .into_values()
            .map(|row| (PrimaryKeyValue::of(&self.model, &row), row))
            .collect();
    }

    fn typed(&self, row: &Row) -> Row {
        self.model
            .columns()
            .map(|column| {
                let value = row.get(column.name()).cloned().flatten();

                (column.name().clone(), Self::cast(value, column))
            })
            .collect()
    }

    fn cast(value: Option<String>, column: &ColumnModel) -> Option<String> {
        match column.column_type() {
            ColumnType::Int16 | ColumnType::Int32 | ColumnType::Int64 => value?
                .trim()
                .parse::<i64>()
                .ok()
                .map(|number| number.to_string()),
            _ => value,
        }
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

    fn rename_in(key: &mut [ColumnName], from: &ColumnName, to: &ColumnName) {
        for column in key.iter_mut().filter(|column| *column == from) {
            *column = to.clone();
        }
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
            0,
            TableModel::new(people(), vec![id(), text("name")], vec!["id".into()]),
        )
    }

    fn replica() -> Replica {
        created()
            .insert(1, &people(), &[("id", Some("1")), ("name", Some("Alice"))])
            .insert(2, &people(), &[("id", Some("2")), ("name", Some("Bob"))])
            .update(3, &people(), &[("id", Some("1")), ("name", Some("Alicia"))])
            .delete(4, &people(), &[("id", Some("2"))])
    }

    fn row(cells: &[(&str, Option<&str>)]) -> Row {
        Replica::row(cells)
    }

    fn names(replica: &Replica, instant: i64, table: &TableId) -> Vec<String> {
        replica
            .at(at(instant))
            .table(table)
            .unwrap()
            .columns()
            .map(|column| column.name().to_string())
            .collect()
    }

    #[test]
    fn a_replica_holds_each_key_as_the_last_change_before_an_instant_left_it() {
        assert_eq!(
            replica().at(at(4)).rows(&people()),
            Some(vec![
                row(&[("id", Some("1")), ("name", Some("Alicia"))]),
                row(&[("id", Some("2")), ("name", Some("Bob"))]),
            ])
        );
    }

    #[test]
    fn a_replica_holds_nothing_of_a_change_at_the_instant_it_is_read_at() {
        assert_eq!(
            replica().at(at(3)).rows(&people()),
            Some(vec![
                row(&[("id", Some("1")), ("name", Some("Alice"))]),
                row(&[("id", Some("2")), ("name", Some("Bob"))]),
            ])
        );
    }

    #[test]
    fn a_deleted_key_leaves_a_replica() {
        assert_eq!(
            replica().at(at(5)).rows(&people()),
            Some(vec![row(&[("id", Some("1")), ("name", Some("Alicia"))])])
        );
    }

    #[test]
    fn a_row_holds_every_column_of_its_table_even_one_left_unsaid() {
        let replica = created().insert(1, &people(), &[("id", Some("1"))]);

        assert_eq!(
            replica.at(at(2)).rows(&people()),
            Some(vec![row(&[("id", Some("1")), ("name", None)])])
        );
    }

    #[test]
    fn a_table_is_held_from_its_creation_until_its_drop() {
        let replica = replica().drop(5, &people());

        assert!(replica.at(at(0)).table(&people()).is_none());
        assert!(replica.at(at(5)).table(&people()).is_some());
        assert!(replica.at(at(6)).table(&people()).is_none());
    }

    #[test]
    fn a_table_renamed_carries_its_rows_under_its_new_name() {
        let replica = replica().rename(5, &people(), &persons());

        assert_eq!(replica.at(at(6)).rows(&people()), None);
        assert_eq!(
            replica.at(at(6)).rows(&persons()),
            replica.at(at(5)).rows(&people())
        );
    }

    #[test]
    fn a_table_renamed_and_altered_by_one_statement_is_altered_under_its_new_name() {
        let replica =
            replica().alter_into(5, &people(), &persons(), vec![ColumnDdl::add(text("tier"))]);

        assert_eq!(names(&replica, 6, &persons()), ["id", "name", "tier"]);
    }

    #[test]
    fn a_column_added_fills_the_rows_already_there_with_its_default() {
        let tier = ColumnModel::new("tier", "VARCHAR", ColumnType::Text, true, Some("gold"));
        let replica = replica().alter(5, &people(), vec![ColumnDdl::add(tier)]);

        assert_eq!(
            replica.at(at(6)).rows(&people()),
            Some(vec![row(&[
                ("id", Some("1")),
                ("name", Some("Alicia")),
                ("tier", Some("gold"))
            ])])
        );
    }

    #[test]
    fn a_column_added_lands_where_the_statement_placed_it() {
        let replica = created()
            .alter(
                1,
                &people(),
                vec![ColumnDdl::add(text("tier")).placed(Placed::First)],
            )
            .alter(
                2,
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
        let replica = replica().alter(5, &people(), vec![ColumnDdl::drop("name")]);

        assert_eq!(
            replica.at(at(6)).rows(&people()),
            Some(vec![row(&[("id", Some("1"))])])
        );
    }

    #[test]
    fn a_column_renamed_carries_its_values_under_its_new_name() {
        let replica = replica().alter(5, &people(), vec![ColumnDdl::rename("name", "label")]);

        assert_eq!(
            replica.at(at(6)).rows(&people()),
            Some(vec![row(&[("id", Some("1")), ("label", Some("Alicia"))])])
        );
    }

    #[test]
    fn a_column_retyped_casts_its_values_into_its_new_type() {
        let replica = created()
            .insert(1, &people(), &[("id", Some("1")), ("name", Some(" 42"))])
            .insert(2, &people(), &[("id", Some("2")), ("name", Some("Bob"))])
            .alter(
                3,
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
            replica.at(at(4)).rows(&people()),
            Some(vec![
                row(&[("id", Some("1")), ("name", Some("42"))]),
                row(&[("id", Some("2")), ("name", None)]),
            ])
        );
    }

    #[test]
    fn a_column_changed_is_renamed_retyped_and_moved_at_once() {
        let replica = replica().alter(
            5,
            &people(),
            vec![ColumnDdl::change("name", text("label")).placed(Placed::First)],
        );

        assert_eq!(names(&replica, 6, &people()), ["label", "id"]);
        assert_eq!(
            replica.at(at(6)).rows(&people()),
            Some(vec![row(&[("id", Some("1")), ("label", Some("Alicia"))])])
        );
    }

    #[test]
    fn a_renamed_key_column_keeps_the_key_on_it() {
        let replica = replica().alter(5, &people(), vec![ColumnDdl::rename("id", "person_id")]);

        assert_eq!(
            replica
                .at(at(6))
                .table(&people())
                .unwrap()
                .primary_key()
                .collect::<Vec<_>>(),
            ["person_id"]
        );
    }

    #[test]
    fn a_table_rekeyed_holds_each_row_under_its_new_key() {
        let replica = replica()
            .alter(5, &people(), vec![ColumnDdl::rekey(&["name"])])
            .update(6, &people(), &[("id", Some("7")), ("name", Some("Alicia"))]);

        assert_eq!(
            replica.at(at(7)).rows(&people()),
            Some(vec![row(&[("id", Some("7")), ("name", Some("Alicia"))])])
        );
    }

    #[test]
    fn a_row_is_found_by_its_key() {
        assert_eq!(
            replica()
                .at(at(4))
                .row(&people(), &row(&[("id", Some("2"))])),
            Some(&row(&[("id", Some("2")), ("name", Some("Bob"))]))
        );
    }

    #[test]
    fn changes_of_one_instant_apply_in_the_order_they_were_made() {
        let replica = created()
            .insert(1, &people(), &[("id", Some("1")), ("name", Some("Alice"))])
            .update(1, &people(), &[("id", Some("1")), ("name", Some("Alicia"))]);

        assert_eq!(
            replica.at(at(2)).rows(&people()),
            Some(vec![row(&[("id", Some("1")), ("name", Some("Alicia"))])])
        );
    }
}
