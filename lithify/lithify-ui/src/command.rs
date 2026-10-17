use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use chrono::NaiveDate;

use lithify_core::model::TableId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildCommand {
    directory: String,
    snapshot: NaiveDate,
    sink: Option<String>,
    tables: BTreeSet<TableId>,
    dry_run: Option<String>,
}

impl BuildCommand {
    pub fn new(directory: &str, snapshot: NaiveDate) -> Self {
        Self {
            directory: directory.to_owned(),
            snapshot,
            sink: None,
            tables: BTreeSet::new(),
            dry_run: None,
        }
    }

    pub fn sink(self, sink: &str) -> Self {
        Self {
            sink: Some(sink.to_owned()),
            ..self
        }
    }

    pub fn table(mut self, table: TableId) -> Self {
        self.tables.insert(table);

        self
    }

    pub fn dry_run(self, into: &str) -> Self {
        Self {
            dry_run: Some(into.to_owned()),
            ..self
        }
    }

    pub fn args(&self) -> Vec<String> {
        let named = |flag: &str, value: &str| [flag.to_owned(), value.to_owned()];

        ["lithify", "build", &self.directory]
            .map(str::to_owned)
            .into_iter()
            .chain(named("--snapshot", &self.snapshot.to_string()))
            .chain(self.sink.iter().flat_map(|sink| named("--sink", sink)))
            .chain(
                self.tables
                    .iter()
                    .flat_map(|table| named("--table", &table.to_string())),
            )
            .chain(
                self.dry_run
                    .iter()
                    .flat_map(|into| named("--dry-run", into)),
            )
            .collect()
    }

    fn quoted(arg: &str) -> String {
        let plain = |c: char| c.is_ascii_alphanumeric() || "_./:=-".contains(c);

        match arg.chars().all(plain) && !arg.is_empty() {
            true => arg.to_owned(),
            false => format!("'{}'", arg.replace('\'', r"'\''")),
        }
    }
}

impl Display for BuildCommand {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let args: Vec<_> = self.args().iter().map(|arg| Self::quoted(arg)).collect();

        formatter.write_str(&args.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use lithify_core::model::TableId;

    use super::*;

    fn date(iso: &str) -> NaiveDate {
        iso.parse().unwrap()
    }

    #[test]
    fn a_build_names_its_pipeline_and_shard() {
        assert_eq!(
            BuildCommand::new("mysql", date("2026-09-20")).to_string(),
            "lithify build mysql --snapshot 2026-09-20"
        );
    }

    #[test]
    fn a_build_names_the_sink_the_tables_and_the_directory_its_statements_are_written_into() {
        let command = BuildCommand::new("mysql", date("2026-09-20"))
            .sink("rustfs")
            .table(TableId::new("shop", "pets"))
            .table(TableId::new("shop", "people"))
            .dry_run("plans");

        assert_eq!(
            command.args(),
            [
                "lithify",
                "build",
                "mysql",
                "--snapshot",
                "2026-09-20",
                "--sink",
                "rustfs",
                "--table",
                "shop.people",
                "--table",
                "shop.pets",
                "--dry-run",
                "plans",
            ]
        );
    }

    #[test]
    fn a_build_quotes_what_the_shell_would_split() {
        assert_eq!(
            BuildCommand::new("my pipeline", date("2026-09-20"))
                .dry_run("it's")
                .to_string(),
            r#"lithify build 'my pipeline' --snapshot 2026-09-20 --dry-run 'it'\''s'"#
        );
    }
}
