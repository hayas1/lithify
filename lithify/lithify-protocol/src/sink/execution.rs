use std::cell::RefCell;
use std::rc::Rc;

use lithify_core::sql::SqlString;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Apply;

// Cloned, a dry run shares what it records, so the one the client executes with and the one the
// caller reads the statements back from after each step are the same.
#[derive(Debug, Clone, Default)]
pub struct DryRun {
    statements: Rc<RefCell<Vec<SqlString>>>,
}

impl DryRun {
    pub fn record(&self, statement: &SqlString) {
        self.statements.borrow_mut().push(statement.clone());
    }

    pub fn take(&self) -> Vec<SqlString> {
        self.statements.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dry_run_cloned_hands_over_what_either_recorded() {
        let dry_run = DryRun::default();
        let cloned = dry_run.clone();

        cloned.record(&SqlString::from("SELECT 1"));

        assert_eq!(dry_run.take(), ["SELECT 1"]);
    }

    #[test]
    fn a_dry_run_hands_over_the_statements_it_recorded_once() {
        let dry_run = DryRun::default();

        dry_run.record(&SqlString::from("SELECT 1"));
        dry_run.record(&SqlString::from("SELECT 2"));

        assert_eq!(dry_run.take(), ["SELECT 1", "SELECT 2"]);
        assert_eq!(dry_run.take(), Vec::<SqlString>::new());
    }
}
