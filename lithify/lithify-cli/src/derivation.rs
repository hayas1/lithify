use lithify_core::artifact::Artifact;
use lithify_core::build_log::{BuildLog, Latest};
use lithify_core::schedule::Schedule;
use lithify_core::sink::WarehouseClient;
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

use lithify_config::infra::Sink;

use crate::artifacts::{
    ArtifactConfig, DeclaredInstant, DeclaredScd1, DeclaredScd2, DeclaredSnapshot,
};
use crate::course::Itinerary;
use crate::journal::Journal;
use crate::pipeline::WarehouseConfig;
use crate::plan::{Planner, Tables};
use crate::settlement::Settlement;
use crate::unrunnable::Unrunnable;

pub struct Derivation<'a> {
    sink: &'a Sink<WarehouseConfig, ArtifactConfig>,
    windows: Vec<Window>,
    days: Vec<Window>,
    built_until: Option<Ts<Event>>,
    now: Ts<Event>,
}

impl<'a> Derivation<'a> {
    pub fn new(
        sink: &'a Sink<WarehouseConfig, ArtifactConfig>,
        itinerary: &Itinerary,
        schedule: Schedule,
        now: Ts<Event>,
    ) -> Self {
        Self {
            sink,
            windows: itinerary.windows().to_vec(),
            days: itinerary.days(schedule),
            built_until: itinerary.built_until(),
            now,
        }
    }

    pub fn settle<P, S>(
        &self,
        planner: &P,
        settlement: &S,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable>
    where
        P: Planner,
        Unrunnable: From<<P::Client as WarehouseClient>::Error>,
        S: Settlement,
    {
        let snapshot = DeclaredSnapshot::of(self.sink)?;

        for declared in DeclaredInstant::of(self.sink)? {
            let instant = declared.artifact();
            let settlement = settlement.advancing(declared.namespace(), &declared.series());
            let cuts = self
                .days
                .iter()
                .flat_map(|day| instant.windows(day))
                .filter(|window| *window.to() <= self.now);

            for window in cuts {
                let previous = self.built(planner, &snapshot, *window.from())?;
                self.derive(planner, &instant, &window, &previous, &settlement, journal)?;
            }
        }

        if let Some(declared) = DeclaredScd2::of(self.sink)? {
            let scd2 = declared.artifact();
            let settlement = settlement.advancing(declared.namespace(), &declared.series());
            planner
                .client()
                .create_build_log::<Unrunnable>(declared.namespace())?;

            for window in &self.windows {
                let previous = planner
                    .client()
                    .build_logs(
                        declared.namespace(),
                        &Latest::SucceededUntil(*window.from()).into(),
                    )?
                    .into_iter()
                    .filter(|log| log.advances(&declared.series()))
                    .collect::<Vec<_>>();
                self.derive(planner, &scd2, window, &previous, &settlement, journal)?;
            }
        }

        if let Some(declared) = DeclaredScd1::of(self.sink)?
            && let Some(built_until) = self.built_until.filter(|built| *built < self.now)
        {
            let scd1 = declared.artifact();
            let settlement = settlement.advancing(declared.namespace(), &declared.series());
            let window = Window::new(built_until, self.now);
            let previous = self.built(planner, &snapshot, built_until)?;

            self.derive(planner, &scd1, &window, &previous, &settlement, journal)?;
        }

        Ok(())
    }

    fn built<P>(
        &self,
        planner: &P,
        snapshot: &DeclaredSnapshot,
        until: Ts<Event>,
    ) -> Result<Vec<BuildLog>, Unrunnable>
    where
        P: Planner,
        Unrunnable: From<<P::Client as WarehouseClient>::Error>,
    {
        Ok(planner
            .client()
            .build_logs(snapshot.namespace(), &Latest::SucceededUntil(until).into())?
            .into_iter()
            .filter(|log| log.advances(&snapshot.series()))
            .collect())
    }

    fn derive<P: Planner, A: Artifact, S: Settlement>(
        &self,
        planner: &P,
        artifact: &A,
        window: &Window,
        previous: &[BuildLog],
        settlement: &S,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        journal.window(window);
        match planner.plan(artifact, window, previous, &Tables::Every, settlement) {
            Ok(output) => {
                settlement.report(window, output, journal)?;
            }
            Err(unrunnable) => journal.failed(self.sink.name(), &unrunnable),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use lithify_config::Pipeline;

    use crate::course::Course;
    use crate::course::tests::settle;
    use crate::pipeline::CaptureConfig;
    use crate::plan::tests::{CAPTURE, INFRA, PEOPLE, built_before, logged, opened, rows};

    use super::*;

    fn derived(
        name: &str,
        declared: &str,
        placed: &str,
        listed: &str,
    ) -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        opened(name, &format!("{declared}{INFRA}{placed}"), listed)
    }

    fn cut_at_six(name: &str) -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        derived(
            name,
            "[[artifact.instant]]\nat = \"06:00\"\nshard = \"%Y%m%d%H\"\n",
            "place.instant.\"06:00\" = [{ prefix = \"mysql_shop_instant\" }]\n",
            "[default]\nartifacts = [\"snapshot\", \"instant\"]\n",
        )
    }

    #[test]
    fn a_catch_up_folds_the_change_log_after_the_last_daily_snapshot_into_the_current_state() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);
        let pipeline = derived(
            "scd1",
            "[artifact.scd1]\n",
            "place.scd1 = [{ prefix = \"mysql_shop_scd1\" }]\n",
            "[default]\nartifacts = [\"snapshot\", \"scd1\"]\n",
        );

        settle(
            &pipeline,
            &client,
            &Course::CatchUp { to: None },
            "2026-09-21T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, base FROM mysql_shop_scd1._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [(
                "mysql_shop_scd1.people".to_owned(),
                "mysql_shop.people_20260920".to_owned()
            )]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'people' FROM mysql_shop_scd1.people ORDER BY id"
            ),
            [
                ("7".to_owned(), "people".to_owned()),
                ("8".to_owned(), "people".to_owned())
            ]
        );
    }

    #[test]
    fn a_catch_up_keeps_every_version_of_each_window_it_builds() {
        let client = logged(PEOPLE, &[]);
        let pipeline = derived(
            "scd2",
            "[artifact.scd2]\nshard = \"%Y%m%d\"\n",
            "place.scd2 = [{ prefix = \"mysql_shop_scd2\" }]\n",
            "[default]\nartifacts = [\"snapshot\", \"scd2\"]\n",
        );

        settle(
            &pipeline,
            &client,
            &Course::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, COALESCE(base, '') FROM mysql_shop_scd2._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [(
                "mysql_shop_scd2.people_open_20260921".to_owned(),
                String::new()
            )]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, _valid_from::VARCHAR FROM mysql_shop_scd2.people_open_20260921"
            ),
            [("8".to_owned(), "1789948800001".to_owned())]
        );
    }

    #[test]
    fn a_catch_up_builds_an_artifact_only_for_the_tables_that_name_it() {
        let client = logged(PEOPLE, &[]);
        let pipeline = derived(
            "scd2-unnamed",
            "[artifact.scd2]\nshard = \"%Y%m%d\"\n",
            "place.scd2 = [{ prefix = \"mysql_shop_scd2\" }]\n",
            "[default]\nartifacts = [\"snapshot\"]\n",
        );

        settle(
            &pipeline,
            &client,
            &Course::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, '' FROM mysql_shop_scd2._lithify_log WHERE status = 'succeeded'"
            ),
            []
        );
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
