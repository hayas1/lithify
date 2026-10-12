use std::convert::Infallible;
use std::fmt::Display;

use chrono::{DateTime, Utc};

use event_replay_core::event::SchemaChange;
use event_replay_core::listener::{
    ColumnAdded, ColumnDropped, ColumnRenamed, ColumnRetyped, PrimaryKeyChanged,
    SchemaChangeListener, TableCreated, TableDropped, TableRenamed,
};

#[derive(Debug, Default)]
pub struct Journal {
    entries: Vec<String>,
    failed: bool,
}

impl Journal {
    pub fn entries(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(String::as_str)
    }

    pub fn note(&mut self, note: &str) {
        self.entries.push(note.to_owned());
    }

    pub fn sink(&mut self, name: &str) {
        self.entries.push(format!("--- {name}"));
    }

    pub fn failed(&mut self, name: &str, cause: &impl Display) {
        self.failed = true;
        self.entries.push(format!("{name}: {cause}"));
    }

    pub fn complete(&self) -> bool {
        !self.failed
    }

    fn table(&mut self, entry: String) {
        self.entries.push(format!("  {entry}"));
    }

    fn recorded_at<P>(change: &SchemaChange<P>) -> String {
        match DateTime::<Utc>::try_from(*change.recorded_at()) {
            Ok(at) => at.to_rfc3339(),
            Err(recorded_at) => recorded_at.milliseconds_since_epoch().to_string(),
        }
    }
}

impl<P> SchemaChangeListener<P> for Journal {
    type Context = ();
    type Error = Infallible;

    fn visit_statement(&mut self, _: &(), change: &SchemaChange<P>) -> Result<(), Infallible> {
        self.entries.push(format!(
            "{} {}",
            Self::recorded_at(change),
            change.statement()
        ));

        Ok(())
    }

    fn table_created(&mut self, _: &(), event: &TableCreated<'_>) -> Result<(), Infallible> {
        self.table(format!(
            "{} {} ({} columns, key {:?})",
            event.origin(),
            event.table().id(),
            event.table().columns().count(),
            event.table().primary_key().collect::<Vec<_>>()
        ));

        Ok(())
    }

    fn table_dropped(&mut self, _: &(), event: &TableDropped<'_>) -> Result<(), Infallible> {
        self.table(format!("dropped {}", event.table().id()));

        Ok(())
    }

    fn table_renamed(&mut self, _: &(), event: &TableRenamed<'_>) -> Result<(), Infallible> {
        self.table(format!(
            "renamed {} to {}",
            event.before().id(),
            event.after().id()
        ));

        Ok(())
    }

    fn column_renamed(&mut self, _: &(), event: &ColumnRenamed<'_>) -> Result<(), Infallible> {
        self.table(format!(
            "renamed {}.{} to {}",
            event.after().id(),
            event.previous().name(),
            event.column().name()
        ));

        Ok(())
    }

    fn column_added(&mut self, _: &(), event: &ColumnAdded<'_>) -> Result<(), Infallible> {
        self.table(format!(
            "added {}.{}",
            event.after().id(),
            event.column().name()
        ));

        Ok(())
    }

    fn column_dropped(&mut self, _: &(), event: &ColumnDropped<'_>) -> Result<(), Infallible> {
        self.table(format!(
            "dropped {}.{}",
            event.before().id(),
            event.column().name()
        ));

        Ok(())
    }

    fn column_retyped(&mut self, _: &(), event: &ColumnRetyped<'_>) -> Result<(), Infallible> {
        self.table(format!(
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
        self.table(format!(
            "keyed {} {:?} -> {:?}",
            event.after().id(),
            event.before().primary_key().collect::<Vec<_>>(),
            event.after().primary_key().collect::<Vec<_>>()
        ));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_replay_core::event::TableChange;
    use event_replay_core::listener::{CatalogModel, OutOfOrder};
    use event_replay_core::model::{ColumnModel, ColumnType, TableId, TableModel};
    use event_replay_core::position::{Position, RecordedAt};

    #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
    struct Offset(u64);

    impl Position for Offset {}

    fn customers(columns: Vec<ColumnModel>) -> TableModel {
        TableModel::new(
            TableId::new("shop", "customers"),
            columns,
            vec!["id".to_owned()],
        )
    }

    fn identifier() -> ColumnModel {
        ColumnModel::new("id", "BIGINT", ColumnType::Int64, false, None)
    }

    fn change(at: u64, statement: &str, tables: Vec<TableChange>) -> SchemaChange<Offset> {
        SchemaChange::new(
            Offset(at),
            RecordedAt::from(1789800000000 + at as i64 * 1000),
            statement,
            tables,
        )
    }

    fn journal(changes: Vec<SchemaChange<Offset>>) -> Vec<String> {
        let mut catalog = CatalogModel::default();
        let mut journal = Journal::default();

        for change in &changes {
            catalog
                .accept::<OutOfOrder, _>(&(), change, &mut journal)
                .unwrap();
        }

        journal.entries().map(str::to_owned).collect()
    }

    #[test]
    fn a_statement_is_reported_with_the_time_the_source_recorded_it() {
        let entries = journal(vec![change(
            1,
            "CREATE TABLE `customers` (`id` bigint)",
            vec![TableChange::Created(customers(vec![identifier()]))],
        )]);

        assert_eq!(
            entries,
            [
                "2026-09-19T06:40:01+00:00 CREATE TABLE `customers` (`id` bigint)",
                "  created shop.customers (1 columns, key [\"id\"])",
            ]
        );
    }

    #[test]
    fn a_statement_that_changes_a_table_is_reported_under_the_statement() {
        let nickname = ColumnModel::new("nickname", "varchar", ColumnType::Text, true, None);
        let entries = journal(vec![
            change(
                1,
                "CREATE TABLE `customers` (`id` bigint)",
                vec![TableChange::Created(customers(vec![identifier()]))],
            ),
            change(
                2,
                "ALTER TABLE `customers` ADD COLUMN `nickname` varchar(255)",
                vec![TableChange::Altered(customers(vec![
                    identifier(),
                    nickname,
                ]))],
            ),
            change(
                3,
                "DROP TABLE `customers`",
                vec![TableChange::Dropped(TableId::new("shop", "customers"))],
            ),
        ]);

        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry.starts_with("  "))
                .collect::<Vec<_>>(),
            [
                "  created shop.customers (1 columns, key [\"id\"])",
                "  added shop.customers.nickname",
                "  dropped shop.customers",
            ]
        );
    }

    #[test]
    fn a_note_is_reported_as_it_stands() {
        let mut journal = Journal::default();

        journal.note("primary lands no schema changes");

        assert_eq!(
            journal.entries().collect::<Vec<_>>(),
            ["primary lands no schema changes"]
        );
        assert!(journal.complete());
    }

    #[test]
    fn a_sink_is_reported_before_what_it_holds() {
        let mut journal = Journal::default();

        journal.sink("warehouse");

        assert_eq!(journal.entries().collect::<Vec<_>>(), ["--- warehouse"]);
        assert!(journal.complete());
    }

    #[test]
    fn a_sink_that_cannot_be_read_is_reported_and_leaves_the_run_incomplete() {
        let mut journal = Journal::default();

        journal.failed("warehouse", &"the table is not there");

        assert_eq!(
            journal.entries().collect::<Vec<_>>(),
            ["warehouse: the table is not there"]
        );
        assert!(!journal.complete());
    }

    #[test]
    fn a_rename_is_reported_with_both_names() {
        let name = ColumnModel::new("name", "varchar", ColumnType::Text, true, None);
        let full_name = ColumnModel::new("full_name", "varchar", ColumnType::Text, true, None);
        let clients = TableId::new("shop", "clients");
        let entries = journal(vec![
            change(
                1,
                "CREATE TABLE `customers` (`id` bigint, `name` varchar(255))",
                vec![TableChange::Created(customers(vec![identifier(), name]))],
            ),
            change(
                2,
                "ALTER TABLE `customers` RENAME COLUMN `name` TO `full_name`, RENAME TO `clients`",
                vec![
                    TableChange::ColumnRenamed {
                        table: TableId::new("shop", "customers"),
                        from: "name".to_owned(),
                        to: "full_name".to_owned(),
                    },
                    TableChange::Renamed {
                        from: TableId::new("shop", "customers"),
                        to: clients.clone(),
                    },
                    TableChange::Altered(
                        customers(vec![identifier(), full_name]).with_id(&clients),
                    ),
                ],
            ),
        ]);

        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry.starts_with("  renamed"))
                .collect::<Vec<_>>(),
            [
                "  renamed shop.customers.name to full_name",
                "  renamed shop.customers to shop.clients",
            ]
        );
    }

    #[test]
    fn a_table_is_reported_as_it_appeared_to_the_catalog() {
        let entries = journal(vec![
            change(
                1,
                "CREATE TABLE `customers` (`id` bigint)",
                vec![TableChange::Created(customers(vec![identifier()]))],
            )
            .captured(),
            change(
                2,
                "ALTER TABLE `orders` ADD COLUMN `note` text",
                vec![TableChange::Altered(TableModel::new(
                    TableId::new("shop", "orders"),
                    vec![identifier()],
                    vec!["id".to_owned()],
                ))],
            ),
        ]);

        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry.starts_with("  "))
                .collect::<Vec<_>>(),
            [
                "  captured shop.customers (1 columns, key [\"id\"])",
                "  discovered shop.orders (1 columns, key [\"id\"])",
            ]
        );
    }
}
