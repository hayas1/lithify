use std::collections::BTreeMap;

use crate::ts::{Event, Ts};

pub type Row = BTreeMap<String, Option<String>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    Upsert(Row),
    Delete(Row),
    AddColumn {
        name: String,
        default: Option<String>,
    },
    DropColumn(String),
    RenameColumn {
        from: String,
        to: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replica {
    key: Vec<String>,
    mutations: Vec<(Ts<Event>, Mutation)>,
}

impl Replica {
    pub fn keyed_by(key: &[&str]) -> Self {
        Self {
            key: key.iter().map(|column| (*column).to_owned()).collect(),
            mutations: Vec::new(),
        }
    }

    pub fn upsert(self, at: i64, row: &[(&str, Option<&str>)]) -> Self {
        self.mutated(at, Mutation::Upsert(Self::row(row)))
    }

    pub fn delete(self, at: i64, key: &[(&str, Option<&str>)]) -> Self {
        self.mutated(at, Mutation::Delete(Self::row(key)))
    }

    pub fn add_column(self, at: i64, name: &str, default: Option<&str>) -> Self {
        self.mutated(
            at,
            Mutation::AddColumn {
                name: name.to_owned(),
                default: default.map(str::to_owned),
            },
        )
    }

    pub fn drop_column(self, at: i64, name: &str) -> Self {
        self.mutated(at, Mutation::DropColumn(name.to_owned()))
    }

    pub fn rename_column(self, at: i64, from: &str, to: &str) -> Self {
        self.mutated(
            at,
            Mutation::RenameColumn {
                from: from.to_owned(),
                to: to.to_owned(),
            },
        )
    }

    pub fn mutations(&self) -> &[(Ts<Event>, Mutation)] {
        &self.mutations
    }

    pub fn at(&self, instant: Ts<Event>) -> Vec<Row> {
        let mut rows: BTreeMap<Vec<Option<String>>, Row> = BTreeMap::new();

        for (_, mutation) in self.mutations.iter().filter(|(at, _)| *at < instant) {
            match mutation {
                Mutation::Upsert(row) => {
                    rows.insert(self.key_of(row), row.clone());
                }
                Mutation::Delete(row) => {
                    rows.remove(&self.key_of(row));
                }
                Mutation::AddColumn { name, default } => {
                    for row in rows.values_mut() {
                        row.insert(name.clone(), default.clone());
                    }
                }
                Mutation::DropColumn(name) => {
                    for row in rows.values_mut() {
                        row.remove(name);
                    }
                }
                Mutation::RenameColumn { from, to } => {
                    for row in rows.values_mut() {
                        if let Some(value) = row.remove(from) {
                            row.insert(to.clone(), value);
                        }
                    }
                }
            }
        }

        rows.into_values().collect()
    }

    fn mutated(mut self, at: i64, mutation: Mutation) -> Self {
        let at = Ts::try_from(at).expect("a replica is mutated at an instant since the epoch");
        self.mutations.push((at, mutation));

        self
    }

    fn key_of(&self, row: &Row) -> Vec<Option<String>> {
        self.key
            .iter()
            .map(|column| row.get(column).cloned().flatten())
            .collect()
    }

    fn row(cells: &[(&str, Option<&str>)]) -> Row {
        cells
            .iter()
            .map(|(column, value)| ((*column).to_owned(), value.map(str::to_owned)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(millis: i64) -> Ts<Event> {
        Ts::try_from(millis).unwrap()
    }

    fn people() -> Replica {
        Replica::keyed_by(&["id"])
            .upsert(1, &[("id", Some("1")), ("name", Some("Alice"))])
            .upsert(2, &[("id", Some("2")), ("name", Some("Bob"))])
            .upsert(3, &[("id", Some("1")), ("name", Some("Alicia"))])
            .delete(4, &[("id", Some("2"))])
    }

    #[test]
    fn a_replica_holds_each_key_as_the_last_change_before_an_instant_left_it() {
        assert_eq!(
            people().at(at(4)),
            [
                Replica::row(&[("id", Some("1")), ("name", Some("Alicia"))]),
                Replica::row(&[("id", Some("2")), ("name", Some("Bob"))]),
            ]
        );
    }

    #[test]
    fn a_replica_holds_nothing_of_a_change_at_the_instant_it_is_read_at() {
        assert_eq!(
            people().at(at(3)),
            [
                Replica::row(&[("id", Some("1")), ("name", Some("Alice"))]),
                Replica::row(&[("id", Some("2")), ("name", Some("Bob"))]),
            ]
        );
    }

    #[test]
    fn a_deleted_key_leaves_a_replica() {
        assert_eq!(
            people().at(at(5)),
            [Replica::row(&[("id", Some("1")), ("name", Some("Alicia"))])]
        );
    }

    #[test]
    fn a_column_added_fills_the_rows_already_there_with_its_default() {
        let replica = people().add_column(5, "tier", Some("gold"));

        assert_eq!(
            replica.at(at(6)),
            [Replica::row(&[
                ("id", Some("1")),
                ("name", Some("Alicia")),
                ("tier", Some("gold"))
            ])]
        );
    }

    #[test]
    fn a_column_dropped_leaves_every_row() {
        assert_eq!(
            people().drop_column(5, "name").at(at(6)),
            [Replica::row(&[("id", Some("1"))])]
        );
    }

    #[test]
    fn a_column_renamed_carries_its_values_under_its_new_name() {
        assert_eq!(
            people().rename_column(5, "name", "label").at(at(6)),
            [Replica::row(&[
                ("id", Some("1")),
                ("label", Some("Alicia"))
            ])]
        );
    }
}
