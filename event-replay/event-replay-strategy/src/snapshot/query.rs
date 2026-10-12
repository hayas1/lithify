use event_replay_core::artifact::UnbuildableArtifact;
use event_replay_core::era::Era;
use event_replay_core::model::{ColumnModel, TableId, TableModel};
use event_replay_core::position::Coordinate;
use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::source::{ColumnPath, RowChangeLog};
use event_replay_core::sql::SqlString;

pub(super) struct ChangeLogQuery<'a, L> {
    log: &'a L,
}

impl<'a, L: RowChangeLog> ChangeLogQuery<'a, L> {
    pub(super) fn new(log: &'a L) -> Self {
        Self { log }
    }

    pub(super) fn change_log<W: Warehouse>(&self, warehouse: &W, table: &TableId) -> SqlString {
        warehouse.change_log_ref(&self.log.dml_topic(table))
    }

    pub(super) fn captured<W: Warehouse>(
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

    pub(super) fn shard<W: Warehouse>(
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

    pub(super) fn changes<W: Warehouse>(
        &self,
        warehouse: &W,
        change_log: &SqlString,
        era: &Era<L::Position>,
    ) -> SqlString {
        SqlString::from(format!(
            "  SELECT *\n  FROM {change_log}\n  WHERE {} AND {}\n{}",
            Edge::Since.condition(warehouse, &self.log.placed(era.from())),
            Edge::Until.condition(warehouse, &self.log.placed(era.to())),
            self.latest(warehouse, era.table()),
        ))
    }

    fn sequence<W: Warehouse>(&self, warehouse: &W) -> Vec<SqlString> {
        self.log
            .sequence()
            .iter()
            .map(|path| warehouse.path(path))
            .collect()
    }

    pub(super) fn fold_changes<W: Warehouse>(
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

    pub(super) fn carry<W: Warehouse>(
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
