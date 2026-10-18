use std::collections::BTreeMap;
use std::marker::PhantomData;

use serde::Deserialize;

use lithify_core::artifact::{Artifact, ArtifactBuilder, UnbuildableArtifact};
use lithify_core::build_log::{BuildLog, Outcome, Run, Series};
use lithify_core::era::Era;
use lithify_core::listener::{
    Alteration, SchemaChangeListener, TableAltered, TableCreated, TableDropped, TableRenamed,
};
use lithify_core::model::{Mask, TableId, TableModel};
use lithify_core::schedule::Schedule;
use lithify_core::sink::{TablePath, Warehouse};
use lithify_core::source::RowChangeLog;
use lithify_core::sql::SqlString;
use lithify_core::window::Window;

use crate::snapshot::Snapshot;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config<L> {
    shard: String,
    place: Vec<L>,
}

impl<L> Config<L> {
    pub fn shard(&self) -> &str {
        &self.shard
    }

    pub fn place(&self) -> &[L] {
        &self.place
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Masked {
    snapshot: Snapshot,
    masks: BTreeMap<TableId, BTreeMap<String, Mask>>,
}

impl Masked {
    pub fn new(snapshot: Snapshot, masks: BTreeMap<TableId, BTreeMap<String, Mask>>) -> Self {
        Self { snapshot, masks }
    }

    fn mask(&self, table: &TableId, column: &str) -> Mask {
        self.masks
            .get(table)
            .and_then(|masks| masks.get(column))
            .copied()
            .unwrap_or_default()
    }
}

impl Artifact for Masked {
    const KIND: &'static str = "masked";

    type From = Snapshot;

    type Builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a> = MaskedBuilder<'a, W>;

    fn series(&self) -> Series {
        Series::new(Self::KIND, &Schedule::default().to_string())
    }

    fn windows(&self, daily: &Window) -> Vec<Window> {
        vec![daily.clone()]
    }

    fn base<'b>(
        &self,
        built: &'b [BuildLog],
        table: &TableId,
        window: &Window,
    ) -> Option<&'b BuildLog> {
        built.iter().find(|log| {
            log.advances(&Snapshot::days())
                && log.table_to() == Some(table)
                && log.window() == window
        })
    }

    fn target(&self, table: &TableId, window: &Window) -> Result<TablePath, UnbuildableArtifact> {
        self.snapshot.target(table, window)
    }

    fn builder<'a, L: RowChangeLog + 'a, W: Warehouse + 'a>(
        &'a self,
        _: &'a L,
        table: TableId,
        window: &'a Window,
        previous: Option<&BuildLog>,
    ) -> Result<Self::Builder<'a, L, W>, UnbuildableArtifact> {
        Ok(MaskedBuilder {
            masked: self,
            target: Some(self.target(&table, window)?),
            table_from: Some(table.clone()),
            table,
            window: window.clone(),
            base: previous.and_then(BuildLog::target).cloned(),
            projection: None,
            warehouse: PhantomData,
        })
    }
}

pub struct MaskedBuilder<'a, W> {
    masked: &'a Masked,
    table_from: Option<TableId>,
    table: TableId,
    window: Window,
    base: Option<TablePath>,
    target: Option<TablePath>,
    projection: Option<SqlString>,
    warehouse: PhantomData<fn(&W)>,
}

impl<W: Warehouse> MaskedBuilder<'_, W> {
    fn project(&self, warehouse: &W, table: &TableModel) -> Result<SqlString, UnbuildableArtifact> {
        let Some(base) = &self.base else {
            return Err(UnbuildableArtifact::WithoutEra(self.table.clone()));
        };
        let columns = table
            .columns()
            .map(|column| {
                let name = warehouse.identifier(column.name());

                match self.masked.mask(&self.table, column.name()) {
                    Mask::Identity => Ok(format!("  {name}")),
                    Mask::Null => Ok(format!(
                        "  CAST(NULL AS {}) AS {name}",
                        warehouse.column_type(column.column_type())?
                    )),
                    mask @ Mask::Hash => Err(UnbuildableArtifact::UnsupportedMask {
                        table: self.table.clone(),
                        column: column.name().to_owned(),
                        mask,
                    }),
                }
            })
            .collect::<Result<Vec<_>, UnbuildableArtifact>>()?;

        Ok(SqlString::from(format!(
            "SELECT\n{}\nFROM {}",
            columns.join(",\n"),
            warehouse.table_ref(base)
        )))
    }

    fn unmasked(&self, alteration: &Alteration<'_>) -> Option<UnbuildableArtifact> {
        let (previous, renamed) = alteration.as_column_renamed()?;
        let masked = self.masked.mask(&self.table, previous.name()) != Mask::Identity;
        let left = self.masked.mask(&self.table, renamed.name()) == Mask::Identity;

        (masked && left).then(|| UnbuildableArtifact::Unmasked {
            table: self.table.clone(),
            masked: previous.name().to_owned(),
            renamed: renamed.name().to_owned(),
        })
    }
}

impl<P, W: Warehouse> SchemaChangeListener<P> for MaskedBuilder<'_, W> {
    type Context = W;
    type Error = UnbuildableArtifact;

    fn table_created(&mut self, _: &W, event: &TableCreated<'_>) -> Result<(), Self::Error> {
        if event.appearance().is_creation() {
            self.table_from = None;
        }

        Ok(())
    }

    fn table_dropped(&mut self, _: &W, _: &TableDropped<'_>) -> Result<(), Self::Error> {
        self.target = None;

        Ok(())
    }

    fn era_boundary(&mut self, warehouse: &W, closed_era: &Era<P>) -> Result<(), Self::Error> {
        if self.base.is_some() {
            self.projection = Some(self.project(warehouse, closed_era.table())?);
        }

        Ok(())
    }

    fn table_renamed(&mut self, _: &W, event: &TableRenamed<'_>) -> Result<(), Self::Error> {
        self.table = event.after().id().clone();
        self.target = Some(self.masked.target(&self.table, &self.window)?);

        Ok(())
    }

    fn table_altered(&mut self, _: &W, event: &TableAltered<'_>) -> Result<(), Self::Error> {
        match event
            .alterations()
            .iter()
            .find_map(|alteration| self.unmasked(alteration))
        {
            Some(unmasked) => Err(unmasked),
            None => Ok(()),
        }
    }
}

impl<W: Warehouse> ArtifactBuilder for MaskedBuilder<'_, W> {
    fn table(&self) -> &TableId {
        &self.table
    }

    fn missing_base(&self) -> Option<&TableId> {
        match (&self.target, &self.base) {
            (Some(_), None) => Some(&self.table),
            _ => None,
        }
    }

    fn select(&self) -> Result<SqlString, UnbuildableArtifact> {
        self.projection
            .clone()
            .ok_or_else(|| UnbuildableArtifact::WithoutEra(self.table.clone()))
    }

    fn create<V: Warehouse>(&self, warehouse: &V) -> Result<Vec<SqlString>, UnbuildableArtifact> {
        let Some(target) = &self.target else {
            return Ok(Vec::new());
        };

        Ok(self.masked.snapshot.created(
            warehouse,
            target,
            &self.table,
            &self.window,
            &self.select()?,
        ))
    }

    fn build_log(&self, run: &Run, outcome: Outcome) -> BuildLog {
        BuildLog::new(
            run,
            self.table_from.clone(),
            self.target.as_ref().map(|_| self.table.clone()),
            self.base.clone(),
            self.target.clone(),
            self.window.clone(),
            outcome,
        )
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use ulid::Ulid;

    use lithify_core::build_log::Status;
    use lithify_core::era::Bound;
    use lithify_core::model::{ColumnModel, ColumnType};
    use lithify_core::testing::Engine;
    use lithify_core::ts::Ts;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct Offset;

    impl lithify_core::position::Position for Offset {}

    struct Log;

    impl lithify_core::source::ChangeLog for Log {
        type Position = Offset;

        fn envelope(&self) -> Option<lithify_core::source::ColumnPath> {
            None
        }

        fn event_time(&self) -> lithify_core::source::ColumnPath {
            lithify_core::source::ColumnPath::from("source.ts_ms")
        }

        fn processing_time(&self) -> lithify_core::source::ColumnPath {
            lithify_core::source::ColumnPath::from("ts_ms")
        }

        fn ordering(&self) -> Vec<lithify_core::source::ColumnPath> {
            Vec::new()
        }

        fn coordinates(&self, _: &Offset) -> Vec<lithify_core::position::Coordinate> {
            Vec::new()
        }
    }

    impl RowChangeLog for Log {
        fn dml_topic(&self, table: &TableId) -> String {
            table.to_string()
        }

        fn operation(&self) -> lithify_core::source::ColumnPath {
            lithify_core::source::ColumnPath::from("op")
        }

        fn before(&self) -> lithify_core::source::ColumnPath {
            lithify_core::source::ColumnPath::from("before")
        }

        fn after(&self) -> lithify_core::source::ColumnPath {
            lithify_core::source::ColumnPath::from("after")
        }
    }

    fn people() -> TableModel {
        TableModel::new(
            TableId::new("shop", "people"),
            vec![
                ColumnModel::new("id", "BIGINT", ColumnType::Int64, false, None),
                ColumnModel::new("name", "VARCHAR", ColumnType::Text, true, None),
            ],
            vec!["id".to_owned()],
        )
    }

    fn window() -> Window {
        Window::new(Ts::try_from(0).unwrap(), Ts::try_from(10).unwrap())
    }

    fn masked(mask: Mask) -> Masked {
        Masked::new(
            Snapshot::new("mysql_shop_masked", "%Y%m%d"),
            BTreeMap::from([(
                TableId::new("shop", "people"),
                BTreeMap::from([("name".to_owned(), mask)]),
            )]),
        )
    }

    fn built(artifact: &str, schedule: &str, (from, to): (i64, i64)) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::nil(),
                DateTime::UNIX_EPOCH,
                artifact,
                schedule,
                "primary",
                "{}",
                "0.1.0",
            ),
            Some(TableId::new("shop", "people")),
            Some(TableId::new("shop", "people")),
            None,
            Some(TablePath::new("mysql_shop", &format!("people_{to}"))),
            Window::new(Ts::try_from(from).unwrap(), Ts::try_from(to).unwrap()),
            Outcome::new(Status::Succeeded, DateTime::UNIX_EPOCH),
        )
    }

    fn projected(masked: &Masked) -> Result<SqlString, UnbuildableArtifact> {
        let window = window();
        let base = built("snapshot", "daily", (0, 10));
        let mut builder = masked
            .builder::<Log, Engine>(&Log, TableId::new("shop", "people"), &window, Some(&base))
            .unwrap();
        SchemaChangeListener::<Offset>::era_boundary(
            &mut builder,
            &Engine::default(),
            &Era::new(
                people(),
                Bound::EventTime(*window.from()),
                Bound::EventTime(*window.to()),
            ),
        )?;

        builder.select()
    }

    #[test]
    fn a_masked_snapshot_projects_the_snapshot_of_the_same_window() {
        let built = [
            built("snapshot", "daily", (-10, 0)),
            built("instant", "09:00", (0, 10)),
            built("snapshot", "daily", (0, 10)),
        ];

        assert_eq!(
            masked(Mask::Null).base(&built, &TableId::new("shop", "people"), &window()),
            Some(&built[2])
        );
    }

    #[test]
    fn a_column_masked_with_null_is_projected_as_null_of_its_type() {
        assert_eq!(
            projected(&masked(Mask::Null)).unwrap(),
            "SELECT\n  \"id\",\n  CAST(NULL AS TEXT) AS \"name\"\nFROM \"mysql_shop\".\"people_10\""
        );
    }

    #[test]
    fn a_mask_no_artifact_applies_yet_is_refused() {
        assert!(matches!(
            projected(&masked(Mask::Hash)),
            Err(UnbuildableArtifact::UnsupportedMask {
                mask: Mask::Hash,
                ..
            })
        ));
    }

    #[test]
    fn a_masked_column_renamed_to_one_left_unmasked_is_refused() {
        let masked = masked(Mask::Null);
        let window = window();
        let builder = masked
            .builder::<Log, Engine>(&Log, TableId::new("shop", "people"), &window, None)
            .unwrap();
        let name = ColumnModel::new("name", "VARCHAR", ColumnType::Text, true, None);
        let label = ColumnModel::new("label", "VARCHAR", ColumnType::Text, true, None);

        assert_eq!(
            builder.unmasked(&Alteration::ColumnRenamed {
                previous: &name,
                column: &label,
            }),
            Some(UnbuildableArtifact::Unmasked {
                table: TableId::new("shop", "people"),
                masked: "name".to_owned(),
                renamed: "label".to_owned(),
            })
        );
        assert_eq!(
            builder.unmasked(&Alteration::ColumnRenamed {
                previous: &label,
                column: &name,
            }),
            None
        );
    }
}
