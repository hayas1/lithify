use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Cell {
    Missing,
    Read(String),
}

impl Display for Cell {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => formatter.write_str("NULL"),
            Self::Read(read) => formatter.write_str(read),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row(Vec<Cell>);

impl FromIterator<Cell> for Row {
    fn from_iter<I: IntoIterator<Item = Cell>>(cells: I) -> Self {
        Self(cells.into_iter().collect())
    }
}

impl Display for Row {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let cells: Vec<String> = self.0.iter().map(Cell::to_string).collect();

        write!(formatter, "({})", cells.join(", "))
    }
}

#[derive(Debug, Default)]
pub struct Rows(Vec<Row>);

impl FromIterator<Row> for Rows {
    fn from_iter<I: IntoIterator<Item = Row>>(rows: I) -> Self {
        let mut rows: Vec<Row> = rows.into_iter().collect();

        rows.sort();

        Self(rows)
    }
}

impl Rows {
    pub fn missing_from(&self, other: &Self) -> Vec<&Row> {
        self.0.iter().filter(|row| !other.0.contains(row)).collect()
    }

    pub fn count(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(cells: &[&str]) -> Row {
        cells
            .iter()
            .map(|cell| match *cell {
                "" => Cell::Missing,
                read => Cell::Read(read.to_owned()),
            })
            .collect()
    }

    #[test]
    fn rows_that_hold_the_same_cells_are_the_same_rows() {
        let source = Rows::from_iter([row(&["2", "Bob"]), row(&["1", "Alice"])]);
        let built = Rows::from_iter([row(&["1", "Alice"]), row(&["2", "Bob"])]);

        assert!(source.missing_from(&built).is_empty());
        assert!(built.missing_from(&source).is_empty());
    }

    #[test]
    fn a_row_only_one_side_holds_is_reported_against_it() {
        let source = Rows::from_iter([row(&["1", "Alice"]), row(&["2", "Bob"])]);
        let built = Rows::from_iter([row(&["1", "Alice2"]), row(&["2", "Bob"])]);

        assert_eq!(source.missing_from(&built), [&row(&["1", "Alice"])]);
        assert_eq!(built.missing_from(&source), [&row(&["1", "Alice2"])]);
    }

    #[test]
    fn a_cell_the_source_left_empty_is_not_a_cell_that_reads_null() {
        assert_ne!(row(&[""]), row(&["NULL"]));
        assert_eq!(row(&[""]).to_string(), "(NULL)");
    }
}
