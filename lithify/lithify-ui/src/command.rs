use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use chrono::NaiveDate;
use serde::Deserialize;

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

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct BuildForm {
    pub pipeline: Option<String>,
    pub sink: Option<String>,
    pub snapshot: Option<String>,
    pub tables: Option<String>,
    pub dry_run: Option<String>,
}

impl BuildForm {
    pub fn command(&self, fallback: &str) -> Result<Option<BuildCommand>, String> {
        let Some(snapshot) = Self::filled(&self.snapshot) else {
            return Ok(None);
        };
        let directory = Self::filled(&self.pipeline).unwrap_or(fallback);
        let snapshot = snapshot
            .parse()
            .map_err(|_| format!("{snapshot} is not a date such as 2026-09-20"))?;
        let mut command = BuildCommand::new(directory, snapshot);

        if let Some(sink) = Self::filled(&self.sink) {
            command = command.sink(sink);
        }
        for table in Self::filled(&self.tables)
            .into_iter()
            .flat_map(|tables| tables.split(','))
            .map(str::trim)
            .filter(|table| !table.is_empty())
        {
            command = command.table(
                TableId::try_from(table)
                    .map_err(|_| format!("{table} is not a table such as shop.people"))?,
            );
        }
        if let Some(into) = Self::filled(&self.dry_run) {
            command = command.dry_run(into);
        }

        Ok(Some(command))
    }

    fn filled(field: &Option<String>) -> Option<&str> {
        field
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
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

    fn form(snapshot: &str, tables: &str, dry_run: &str) -> BuildForm {
        BuildForm {
            pipeline: None,
            sink: Some("rustfs".to_owned()),
            snapshot: Some(snapshot.to_owned()),
            tables: Some(tables.to_owned()),
            dry_run: Some(dry_run.to_owned()),
        }
    }

    #[test]
    fn a_form_assembles_the_build_it_was_filled_with() {
        assert_eq!(
            form("2026-09-20", "shop.people, shop.pets", "plans").command("mysql"),
            Ok(Some(
                BuildCommand::new("mysql", date("2026-09-20"))
                    .sink("rustfs")
                    .table(TableId::new("shop", "people"))
                    .table(TableId::new("shop", "pets"))
                    .dry_run("plans")
            ))
        );
    }

    #[test]
    fn a_form_builds_the_pipeline_it_names_rather_than_the_one_it_falls_back_on() {
        let named = BuildForm {
            pipeline: Some("postgres".to_owned()),
            ..form("2026-09-20", "", "")
        };

        assert_eq!(
            named.command("mysql"),
            Ok(Some(
                BuildCommand::new("postgres", date("2026-09-20")).sink("rustfs")
            ))
        );
    }

    #[test]
    fn a_form_reads_past_the_blanks_between_its_tables() {
        assert_eq!(
            form("2026-09-20", "shop.pets, ,", "").command("mysql"),
            Ok(Some(
                BuildCommand::new("mysql", date("2026-09-20"))
                    .sink("rustfs")
                    .table(TableId::new("shop", "pets"))
            ))
        );
    }

    #[test]
    fn a_form_left_blank_assembles_nothing() {
        assert_eq!(BuildForm::default().command("mysql"), Ok(None));
        assert_eq!(form("", "", "").command("mysql"), Ok(None));
    }

    #[test]
    fn a_form_names_what_it_could_not_read() {
        assert!(form("20 September", "", "").command("mysql").is_err());
        assert!(form("2026-09-20", "people", "").command("mysql").is_err());
    }
}
