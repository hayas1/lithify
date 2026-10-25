use std::fmt::Display;

use lithify_core::build_log::BuildLog;
use lithify_core::window::Window;

use lithify_core::check::Discrepancy;

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

    pub fn window(&mut self, window: &Window) {
        self.entries
            .push(format!("window {} .. {}", window.from(), window.to()));
    }

    pub fn built(&mut self, log: &BuildLog) {
        self.failed |= log.status().failed();
        self.entries.push(format!("  {log}"));
    }

    pub fn discrepancy(&mut self, discrepancy: &Discrepancy) {
        self.failed = true;
        self.entries.push(format!("  {discrepancy}"));
    }

    pub fn complete(&self) -> bool {
        !self.failed
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use lithify_core::build_log::{Run, Status, TableBuilt};
    use lithify_core::model::TableId;
    use lithify_core::sink::TablePath;
    use lithify_core::ts::Ts;

    use super::*;

    #[test]
    fn a_note_is_reported_as_it_stands() {
        let mut journal = Journal::default();

        journal.note("wrote 3 statements to dry-run/rustfs.sql");

        assert_eq!(
            journal.entries().collect::<Vec<_>>(),
            ["wrote 3 statements to dry-run/rustfs.sql"]
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

    fn logged(status: Status) -> BuildLog {
        TableBuilt::new(
            Some(TableId::new("shop", "people")),
            Some(TableId::new("shop", "people")),
            None,
            Some(TablePath::new("mysql_shop", "people_20260920")),
            Window::new(
                Ts::try_from(1789862400000).unwrap(),
                Ts::try_from(1789948800000).unwrap(),
            ),
            status,
            Ts::from(DateTime::from_timestamp_millis(1789949000000).unwrap()),
        )
        .logged(&Run::new(
            ulid::Ulid::nil(),
            Ts::from(DateTime::UNIX_EPOCH),
            "snapshot",
            "daily",
            "primary",
            "{}",
            "0.1.0",
        ))
    }

    #[test]
    fn a_window_is_reported_by_the_instants_it_opens_and_closes_at() {
        let mut journal = Journal::default();

        journal.window(&Window::new(
            Ts::try_from(1789862400000).unwrap(),
            Ts::try_from(1789948800000).unwrap(),
        ));

        assert_eq!(
            journal.entries().collect::<Vec<_>>(),
            ["window 2026-09-20T00:00:00+00:00 .. 2026-09-21T00:00:00+00:00"]
        );
    }

    #[test]
    fn a_discrepancy_is_reported_as_it_reads_and_leaves_the_check_incomplete() {
        let mut journal = Journal::default();

        journal.discrepancy(&Discrepancy::NeverBuilt {
            since: Ts::try_from(1789862400000).unwrap(),
        });

        assert_eq!(
            journal.entries().collect::<Vec<_>>(),
            ["  nothing is built since 2026-09-20T00:00:00+00:00"]
        );
        assert!(!journal.complete());
    }

    #[test]
    fn a_table_built_is_reported_by_where_it_was_built() {
        let mut journal = Journal::default();

        journal.built(&logged(Status::Succeeded));

        assert_eq!(
            journal.entries().collect::<Vec<_>>(),
            ["  built shop.people into mysql_shop.people_20260920"]
        );
        assert!(journal.complete());
    }

    #[test]
    fn a_table_that_failed_to_build_is_reported_with_why_and_leaves_the_run_incomplete() {
        let mut journal = Journal::default();

        journal.built(&logged(Status::Failed("no era".to_owned())));

        assert_eq!(
            journal.entries().collect::<Vec<_>>(),
            ["  failed to build shop.people into mysql_shop.people_20260920: no era"]
        );
        assert!(!journal.complete());
    }
}
