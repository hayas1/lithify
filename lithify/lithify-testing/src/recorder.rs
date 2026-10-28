use std::cell::RefCell;
use std::collections::BTreeSet;

use lithify_core::build_log::{BuildLog, BuildLogQuery, BuildLogRow, Latest};
use lithify_core::sink::{Namespace, TablePath, Warehouse, WarehouseClient};
use lithify_core::sql::SqlString;

pub struct Recorder<W> {
    warehouse: W,
    logged: Vec<BuildLogRow>,
    present: BTreeSet<TablePath>,
    executed: RefCell<Vec<String>>,
}

impl<W> Recorder<W> {
    pub fn new(warehouse: W) -> Self {
        Self {
            warehouse,
            logged: Vec::new(),
            present: BTreeSet::new(),
            executed: RefCell::default(),
        }
    }

    pub fn logging(mut self, namespace: &Namespace, logs: &[BuildLog]) -> Self {
        self.logged.extend(logs.iter().map(BuildLogRow::from));
        self.present.insert(BuildLog::path(namespace));

        self
    }

    pub fn holding(mut self, tables: &[TablePath]) -> Self {
        self.present.extend(tables.iter().cloned());

        self
    }

    pub fn executed(&self) -> Vec<String> {
        self.executed.borrow().clone()
    }
}

impl<W: Warehouse> WarehouseClient for Recorder<W> {
    type Warehouse = W;
    type Error = std::fmt::Error;

    fn warehouse(&self) -> &W {
        &self.warehouse
    }

    async fn execute(&self, statement: &SqlString) -> Result<(), Self::Error> {
        self.executed.borrow_mut().push(statement.to_string());

        Ok(())
    }

    async fn build_log_rows(
        &self,
        _: &Namespace,
        query: &BuildLogQuery,
    ) -> Result<Vec<BuildLogRow>, Self::Error> {
        let logged = self.logged.iter().cloned();

        Ok(match query {
            BuildLogQuery::Latest(Latest::Attempted) => logged.collect(),
            BuildLogQuery::Latest(Latest::Succeeded) => {
                logged.filter(|row| row.status == "succeeded").collect()
            }
            BuildLogQuery::Latest(Latest::SucceededUntil(to)) => logged
                .filter(|row| {
                    row.status == "succeeded" && row.window_to == to.milliseconds_since_epoch()
                })
                .collect(),
            BuildLogQuery::All => logged.rev().collect(),
        })
    }

    async fn exists(&self, table: &TablePath) -> Result<bool, Self::Error> {
        Ok(self.present.contains(table))
    }
}
