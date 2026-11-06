use std::borrow::Borrow;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlString(String);

impl SqlString {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for SqlString {
    fn from(text: String) -> Self {
        Self(text)
    }
}

impl From<&str> for SqlString {
    fn from(text: &str) -> Self {
        Self(text.to_owned())
    }
}

impl Borrow<str> for SqlString {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SqlString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl PartialEq<str> for SqlString {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for SqlString {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_reads_as_the_text_it_was_written_with() {
        let sql = SqlString::from(format!("SELECT {}", 1));

        assert_eq!(sql.as_str(), "SELECT 1");
        assert_eq!(sql.to_string(), "SELECT 1");
        assert_eq!(sql, "SELECT 1");
    }

    #[test]
    fn statements_join_like_the_text_they_hold() {
        let statements = [SqlString::from("SELECT 1"), SqlString::from("SELECT 2")];

        assert_eq!(statements.join(";\n"), "SELECT 1;\nSELECT 2");
    }

    #[test]
    fn sql_written_as_text_is_the_sql_of_that_text() {
        assert_eq!(
            SqlString::from("SELECT 1"),
            SqlString::from("SELECT 1".to_owned())
        );
    }
}
