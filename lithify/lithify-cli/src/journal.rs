use std::fmt::Display;

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_note_is_reported_as_it_stands() {
        let mut journal = Journal::default();

        journal.note("wrote 3 statements to plans/rustfs.sql");

        assert_eq!(
            journal.entries().collect::<Vec<_>>(),
            ["wrote 3 statements to plans/rustfs.sql"]
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
}
