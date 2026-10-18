use lithify_core::artifact::UnbuildableArtifact;
use lithify_core::era::Era;
use lithify_core::model::{ColumnModel, TableId, TableModel};
use lithify_core::position::Coordinate;
use lithify_core::sink::{TablePath, Warehouse};
use lithify_core::source::{ColumnPath, RowChangeLog};
use lithify_core::sql::SqlString;

pub(crate) struct ChangeLogQuery<'a, L> {
    log: &'a L,
}

impl<'a, L: RowChangeLog> ChangeLogQuery<'a, L> {
    pub(crate) fn new(log: &'a L) -> Self {
        Self { log }
    }

    pub(crate) fn change_log<W: Warehouse>(&self, warehouse: &W, table: &TableId) -> SqlString {
        warehouse.change_log_ref(&self.log.dml_topic(table))
    }

    pub(crate) fn captured<W: Warehouse>(
        &self,
        warehouse: &W,
        change_log: &SqlString,
        table: &TableModel,
    ) -> Result<SqlString, UnbuildableArtifact> {
        Ok(SqlString::from(format!(
            "  SELECT\n{}\n  FROM {change_log}\n  WHERE {} = 'r'\n{}",
            self.reads(warehouse, table, "    ")?,
            warehouse.path(&self.log.operation()),
            self.latest(warehouse, table),
        )))
    }

    pub(crate) fn shard<W: Warehouse>(
        &self,
        warehouse: &W,
        shard: &TablePath,
        table: &TableModel,
    ) -> SqlString {
        SqlString::from(format!(
            "  SELECT\n{}\n  FROM {}",
            self.columns(warehouse, table, "    "),
            warehouse.table_ref(shard),
        ))
    }

    pub(crate) fn changes<W: Warehouse>(
        &self,
        warehouse: &W,
        change_log: &SqlString,
        era: &Era<L::Position>,
    ) -> SqlString {
        SqlString::from(format!(
            "  SELECT *\n  FROM {change_log}\n  WHERE {} AND {}\n{}",
            Edge::Since.condition(warehouse, &era.from().placed(self.log)),
            Edge::Until.condition(warehouse, &era.to().placed(self.log)),
            self.latest(warehouse, era.table()),
        ))
    }

    pub(crate) fn versions<W: Warehouse>(
        &self,
        warehouse: &W,
        change_log: &SqlString,
        era: &Era<L::Position>,
    ) -> Result<SqlString, UnbuildableArtifact> {
        let table = era.table();
        let after = warehouse.path(&self.log.after());
        let columns: Vec<String> = table
            .columns()
            .map(|column| {
                let name = warehouse.identifier(column.name());
                let value = match table.primary_key().any(|key| key == column.name()) {
                    true => self.keyed(warehouse, None, &name),
                    false => SqlString::from(format!("{after}.{name}")),
                };

                Ok(format!(
                    "    {} AS {name}",
                    warehouse.read(&value, column.column_type())?
                ))
            })
            .collect::<Result<_, UnbuildableArtifact>>()?;
        let sequence: Vec<String> = self
            .sequence(warehouse)
            .iter()
            .zip(self.sequenced(warehouse))
            .map(|(path, name)| format!("    {path} AS {name}"))
            .collect();

        Ok(SqlString::from(format!(
            "  SELECT\n{},\n    {} AS {},\n    {} AS {},\n{}\n  FROM {change_log}\n  WHERE {} AND {}",
            columns.join(",\n"),
            warehouse.path(&self.log.operation()),
            warehouse.identifier("_op"),
            warehouse.path(&self.log.event_time()),
            warehouse.identifier("_at"),
            sequence.join(",\n"),
            Edge::Since.condition(warehouse, &era.from().placed(self.log)),
            Edge::Until.condition(warehouse, &era.to().placed(self.log)),
        )))
    }

    pub(crate) fn sequenced<W: Warehouse>(&self, warehouse: &W) -> Vec<SqlString> {
        (0..self.log.sequence().len())
            .map(|at| warehouse.identifier(&format!("_s{at}")))
            .collect()
    }

    fn sequence<W: Warehouse>(&self, warehouse: &W) -> Vec<SqlString> {
        self.log
            .sequence()
            .iter()
            .map(|path| warehouse.path(path))
            .collect()
    }

    pub(crate) fn fold_changes<W: Warehouse>(
        &self,
        warehouse: &W,
        table: &TableModel,
        previous: &SqlString,
        changes: &SqlString,
    ) -> Result<SqlString, UnbuildableArtifact> {
        Ok(SqlString::from(format!(
            concat!(
                "  SELECT\n{}\n  FROM {previous}\n",
                "  WHERE NOT EXISTS (\n    SELECT 1\n    FROM {changes}\n{}\n  )\n",
                "  UNION ALL\n",
                "  SELECT\n{}\n  FROM {changes}\n  WHERE {} <> 'd'"
            ),
            self.columns(warehouse, table, "    "),
            self.matched(warehouse, table, previous, changes),
            self.reads(warehouse, table, "    ")?,
            warehouse.path(&self.log.operation()),
            previous = previous,
            changes = changes,
        )))
    }

    pub(crate) fn carry<W: Warehouse>(
        &self,
        warehouse: &W,
        next: &TableModel,
        previous: &SqlString,
        carried: impl Fn(&ColumnModel, &SqlString) -> Result<SqlString, UnbuildableArtifact>,
    ) -> Result<SqlString, UnbuildableArtifact> {
        let columns: Vec<String> = next
            .columns()
            .map(|column| {
                let name = warehouse.identifier(column.name());

                Ok(format!("    {} AS {name}", carried(column, &name)?))
            })
            .collect::<Result<_, UnbuildableArtifact>>()?;

        Ok(SqlString::from(format!(
            "  SELECT\n{}\n  FROM {previous}",
            columns.join(",\n")
        )))
    }

    fn matched<W: Warehouse>(
        &self,
        warehouse: &W,
        table: &TableModel,
        previous: &SqlString,
        changes: &SqlString,
    ) -> SqlString {
        let matched = table
            .primary_key()
            .map(|column| warehouse.identifier(column))
            .enumerate()
            .map(|(at, column)| {
                let matched = format!(
                    "{} = {previous}.{column}",
                    self.keyed(warehouse, Some(changes), &column)
                );

                match at {
                    0 => format!("    WHERE {matched}"),
                    _ => format!("      AND {matched}"),
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        SqlString::from(matched)
    }

    fn latest<W: Warehouse>(&self, warehouse: &W, table: &TableModel) -> SqlString {
        let partition: Vec<String> = table
            .primary_key()
            .map(|column| {
                self.keyed(warehouse, None, &warehouse.identifier(column))
                    .to_string()
            })
            .collect();
        let ordering: Vec<String> = self
            .sequence(warehouse)
            .iter()
            .map(|column| format!("{column} DESC"))
            .collect();

        SqlString::from(format!(
            concat!(
                "  QUALIFY ROW_NUMBER() OVER (\n",
                "    PARTITION BY {}\n",
                "    ORDER BY {}\n",
                "  ) = 1"
            ),
            partition.join(", "),
            ordering.join(", "),
        ))
    }

    fn keyed<W: Warehouse>(
        &self,
        warehouse: &W,
        within: Option<&SqlString>,
        column: &SqlString,
    ) -> SqlString {
        let within = within
            .map(|relation| format!("{relation}."))
            .unwrap_or_default();

        SqlString::from(format!(
            "COALESCE({within}{}.{column}, {within}{}.{column})",
            warehouse.path(&self.log.after()),
            warehouse.path(&self.log.before()),
        ))
    }

    fn columns<W: Warehouse>(&self, warehouse: &W, table: &TableModel, indent: &str) -> SqlString {
        SqlString::from(
            table
                .columns()
                .map(|column| format!("{indent}{}", warehouse.identifier(column.name())))
                .collect::<Vec<_>>()
                .join(",\n"),
        )
    }

    fn reads<W: Warehouse>(
        &self,
        warehouse: &W,
        table: &TableModel,
        indent: &str,
    ) -> Result<SqlString, UnbuildableArtifact> {
        let after = warehouse.path(&self.log.after());
        let columns: Vec<String> = table
            .columns()
            .map(|column| {
                let name = warehouse.identifier(column.name());
                let read = warehouse.read(
                    &SqlString::from(format!("{after}.{name}")),
                    column.column_type(),
                )?;

                Ok(format!("{indent}{read} AS {name}"))
            })
            .collect::<Result<_, UnbuildableArtifact>>()?;

        Ok(SqlString::from(columns.join(",\n")))
    }
}

enum Edge {
    Since,
    Until,
}

impl Edge {
    fn before_last(&self) -> &str {
        match self {
            Self::Since => ">",
            Self::Until => "<",
        }
    }

    fn last(&self) -> &str {
        match self {
            Self::Since => ">=",
            Self::Until => "<",
        }
    }

    fn condition<W: Warehouse>(
        &self,
        warehouse: &W,
        placed: &[(ColumnPath, Coordinate)],
    ) -> SqlString {
        let placed: Vec<(SqlString, SqlString)> = placed
            .iter()
            .map(|(path, coordinate)| (warehouse.path(path), coordinate.sql(warehouse)))
            .collect();
        let Some((last, leading)) = placed.split_last() else {
            return SqlString::from("TRUE");
        };
        let terms: Vec<String> = (0..leading.len())
            .map(|at| Self::term(&leading[..at], &leading[at], self.before_last()))
            .chain([Self::term(leading, last, self.last())])
            .collect();

        match terms.split_first() {
            Some((first, [])) => SqlString::from(first.clone()),
            Some((first, rest)) => SqlString::from(format!(
                "({first}{})",
                rest.iter()
                    .map(|term| format!(" OR ({term})"))
                    .collect::<String>()
            )),
            None => SqlString::from("TRUE"),
        }
    }

    fn term(
        equal: &[(SqlString, SqlString)],
        (column, value): &(SqlString, SqlString),
        compared: &str,
    ) -> String {
        equal
            .iter()
            .map(|(column, value)| format!("{column} = {value}"))
            .chain([format!("{column} {compared} {value}")])
            .collect::<Vec<_>>()
            .join(" AND ")
    }
}
