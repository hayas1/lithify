use event_replay_core::sql::SqlString;

#[derive(Default)]
pub(super) struct Relations {
    named: Vec<(SqlString, SqlString)>,
    steps: usize,
}

impl Relations {
    pub(super) fn step(&mut self) -> usize {
        self.steps += 1;

        self.steps
    }

    pub(super) fn previous(&self) -> Option<&SqlString> {
        self.named.last().map(|(name, _)| name)
    }

    pub(super) fn push(&mut self, name: SqlString, select: SqlString) {
        self.named.push((name, select));
    }

    pub(super) fn statement(&self, target: &SqlString) -> SqlString {
        let ctes: Vec<String> = self
            .named
            .iter()
            .map(|(name, select)| format!("{name} AS (\n{select}\n)"))
            .collect();
        let last = self.previous().map(SqlString::as_str).unwrap_or_default();

        SqlString::from(format!(
            "CREATE OR REPLACE TABLE {target} AS\nWITH {}\nSELECT * FROM {last}",
            ctes.join(",\n"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_relation_comes_before_the_first() {
        assert_eq!(Relations::default().previous(), None);
    }

    #[test]
    fn each_step_is_numbered_after_the_one_before_it() {
        let mut relations = Relations::default();

        assert_eq!(relations.step(), 1);
        assert_eq!(relations.step(), 2);
    }

    #[test]
    fn the_relation_pushed_last_comes_before_the_next() {
        let mut relations = Relations::default();
        relations.push(SqlString::from("base"), SqlString::from("  SELECT 1"));
        relations.push(SqlString::from("era_1"), SqlString::from("  SELECT 2"));

        assert_eq!(relations.previous(), Some(&SqlString::from("era_1")));
    }

    #[test]
    fn a_statement_builds_the_target_from_the_relation_pushed_last() {
        let mut relations = Relations::default();
        relations.push(SqlString::from("base"), SqlString::from("  SELECT 1"));
        relations.push(
            SqlString::from("era_1"),
            SqlString::from("  SELECT * FROM base"),
        );

        assert_eq!(
            relations.statement(&SqlString::from("target")),
            concat!(
                "CREATE OR REPLACE TABLE target AS\n",
                "WITH base AS (\n  SELECT 1\n),\n",
                "era_1 AS (\n  SELECT * FROM base\n)\n",
                "SELECT * FROM era_1"
            )
        );
    }
}
