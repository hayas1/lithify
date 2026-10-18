use lithify_artifact::instant::Instant;
use lithify_config::infra::Sink;
use lithify_core::artifact::Artifact;
use lithify_core::build_log::{BuildLog, Latest};
use lithify_core::sink::WarehouseClient;
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

use crate::artifacts::{ArtifactConfig, DeclaredInstant, DeclaredSnapshot};
use crate::journal::Journal;
use crate::pipeline::WarehouseConfig;
use crate::settlement::Settlement;
use crate::unrunnable::Unrunnable;

pub struct Intraday<'a> {
    sink: &'a Sink<WarehouseConfig, ArtifactConfig>,
    days: Vec<Window>,
    now: Ts<Event>,
}

impl<'a> Intraday<'a> {
    pub fn new(
        sink: &'a Sink<WarehouseConfig, ArtifactConfig>,
        days: Vec<Window>,
        now: Ts<Event>,
    ) -> Self {
        Self { sink, days, now }
    }

    pub fn settle<K, S>(
        &self,
        client: &K,
        settlement: &S,
        journal: &mut Journal,
        mut plan: impl FnMut(&Instant, &Window, &[BuildLog], &S) -> Result<S::Output, Unrunnable>,
    ) -> Result<(), Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
        S: Settlement,
    {
        let snapshot = DeclaredSnapshot::of(self.sink)?;

        for declared in DeclaredInstant::of(self.sink)? {
            let instant = declared.artifact();
            let settlement = settlement.advancing(declared.namespace(), &declared.series());

            for window in self.cuts(&instant) {
                let previous: Vec<BuildLog> = client
                    .build_logs(
                        snapshot.namespace(),
                        &Latest::SucceededUntil(*window.from()).into(),
                    )?
                    .into_iter()
                    .filter(|log| log.advances(&snapshot.series()))
                    .collect();

                journal.window(&window);
                match plan(&instant, &window, &previous, &settlement) {
                    Ok(output) => {
                        settlement.report(&window, output, journal)?;
                    }
                    Err(unrunnable) => journal.failed(self.sink.name(), &unrunnable),
                }
            }
        }

        Ok(())
    }

    fn cuts(&self, instant: &Instant) -> Vec<Window> {
        self.days
            .iter()
            .flat_map(|day| instant.windows(day))
            .filter(|window| *window.to() <= self.now)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use lithify_config::Pipeline;

    use crate::course::Course;
    use crate::course::tests::settle;
    use crate::pipeline::CaptureConfig;
    use crate::plan::tests::{CAPTURE, PEOPLE, built_before, logged, rows};

    use super::*;

    fn cut_at_six(name: &str) -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        let directory =
            std::env::temp_dir().join(format!("lithify-cli-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let compose = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compose/lithify/mysql");
        let infra = std::fs::read_to_string(compose.join("infra.toml")).unwrap();
        std::fs::write(
            directory.join("infra.toml"),
            format!(
                "{infra}\n[[sink.artifact.instant]]\nat = \"06:00\"\nshard = \"%Y%m%d%H\"\nplace = [{{ duckdb.prefix = \"mysql_shop_instant\" }}]\n"
            ),
        )
        .unwrap();
        std::fs::write(
            directory.join("table.toml"),
            "[default]\nartifacts = [\"snapshot\", \"instant\"]\n",
        )
        .unwrap();
        let pipeline = Pipeline::open(&directory).unwrap();
        std::fs::remove_dir_all(&directory).unwrap();

        pipeline
    }

    #[test]
    fn a_catch_up_cuts_each_day_it_builds_at_the_instants_a_sink_declares_onto_the_day_before() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(
            &cut_at_six("cut-each-day"),
            &client,
            &Course::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, base FROM mysql_shop_instant._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                (
                    "mysql_shop_instant.people_2026092005".to_owned(),
                    "mysql_shop.people_20260919".to_owned()
                ),
                (
                    "mysql_shop_instant.people_2026092105".to_owned(),
                    "mysql_shop.people_20260920".to_owned()
                ),
            ]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'people' FROM mysql_shop_instant.people_2026092105 ORDER BY id"
            ),
            [
                ("7".to_owned(), "people".to_owned()),
                ("8".to_owned(), "people".to_owned())
            ]
        );
    }

    #[test]
    fn an_instant_not_yet_reached_is_not_cut() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(
            &cut_at_six("not-yet-cut"),
            &client,
            &Course::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, '' FROM mysql_shop_instant._lithify_log WHERE target LIKE '%20260922%'"
            ),
            []
        );
    }
}
