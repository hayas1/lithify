use std::borrow::Borrow;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Sql(String);

impl Sql {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for Sql {
    fn from(text: String) -> Self {
        Self(text)
    }
}

impl From<&str> for Sql {
    fn from(text: &str) -> Self {
        Self(text.to_owned())
    }
}

impl Borrow<str> for Sql {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Sql {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl PartialEq<str> for Sql {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for Sql {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_reads_as_the_text_it_was_written_with() {
        let sql = Sql::from(format!("SELECT {}", 1));

        assert_eq!(sql.as_str(), "SELECT 1");
        assert_eq!(sql.to_string(), "SELECT 1");
        assert_eq!(sql, "SELECT 1");
    }

    #[test]
    fn statements_join_like_the_text_they_hold() {
        let statements = [Sql::from("SELECT 1"), Sql::from("SELECT 2")];

        assert_eq!(statements.join(";\n"), "SELECT 1;\nSELECT 2");
    }

    #[test]
    fn sql_written_as_text_is_the_sql_of_that_text() {
        assert_eq!(Sql::from("SELECT 1"), Sql::from("SELECT 1".to_owned()));
    }
}
