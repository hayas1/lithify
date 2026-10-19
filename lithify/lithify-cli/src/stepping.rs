use std::collections::BTreeMap;

use chrono::NaiveDate;

use lithify_config::infra::{Sink, Source};
use lithify_core::artifact::Artifact;
use lithify_core::build_log::{BuildLog, Latest, Series};
use lithify_core::progress::{Progress, Step};
use lithify_core::schedule::Timeline;
use lithify_core::sink::{Namespace, WarehouseClient};
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

use crate::artifacts::{ArtifactConfig, Declared, DeclaredSnapshot};
use crate::journal::Journal;
use crate::pipeline::{CaptureConfig, WarehouseConfig};
use crate::plan::{Planner, Tables};
use crate::settlement::Settlement;
use crate::unrunnable::Unrunnable;

pub struct Stepping<'a, P> {
    source: &'a Source<CaptureConfig>,
    snapshot: DeclaredSnapshot<'a>,
    timeline: Timeline,
    now: Ts<Event>,
    planner: &'a P,
}

impl<'a, P> Stepping<'a, P>
where
    P: Planner,
    Unrunnable: From<<P::Client as WarehouseClient>::Error>,
{
    pub fn new(
        sink: &'a Sink<WarehouseConfig, ArtifactConfig>,
        source: &'a Source<CaptureConfig>,
        now: Ts<Event>,
        planner: &'a P,
    ) -> Result<Self, Unrunnable> {
        let snapshot = DeclaredSnapshot::of(sink)?;

        Ok(Self {
            source,
            timeline: Timeline::new(snapshot.schedule(), source.since(), now),
            snapshot,
            now,
            planner,
        })
    }

    pub fn up_to(self, date: NaiveDate) -> Result<Self, Unrunnable> {
        let until = *self.dated(self.shard(date), date)?.to();
        let standing = self
            .progress(self.snapshot.namespace(), &self.snapshot.series())?
            .standing();

        match standing.filter(|standing| *standing > until) {
            Some(built_until) => Err(Unrunnable::Overtaken { date, built_until }),
            None => Ok(Self {
                timeline: Timeline::new(self.snapshot.schedule(), self.source.since(), until),
                ..self
            }),
        }
    }

    pub fn catch_up<S: Settlement>(
        &self,
        declared: &[Declared],
        settlement: &S,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        let mut taken: BTreeMap<Series, Vec<Window>> = BTreeMap::new();

        while let Some((series, window)) =
            self.step(declared.iter(), &taken, settlement, journal)?
        {
            taken.entry(series).or_default().push(window);
        }

        Ok(())
    }

    pub fn step<'d, S: Settlement>(
        &self,
        declared: impl Iterator<Item = &'d Declared<'d>>,
        taken: &BTreeMap<Series, Vec<Window>>,
        settlement: &S,
        journal: &mut Journal,
    ) -> Result<Option<(Series, Window)>, Unrunnable> {
        let Some((declared, step)) = self.first(declared, taken)? else {
            return Ok(None);
        };
        declared.take(
            self,
            step.window(),
            &Tables::Except(step.left_out().clone()),
            settlement,
            journal,
        )?;

        Ok(Some((declared.series(), step.window().clone())))
    }

    fn first<'d>(
        &self,
        declared: impl Iterator<Item = &'d Declared<'d>>,
        taken: &BTreeMap<Series, Vec<Window>>,
    ) -> Result<Option<(&'d Declared<'d>, Step)>, Unrunnable> {
        for declared in declared {
            let taken = taken
                .get(&declared.series())
                .map(Vec::as_slice)
                .unwrap_or_default();

            if let Some(step) = declared.next(self, taken)? {
                return Ok(Some((declared, step)));
            }
        }

        Ok(None)
    }

    pub fn next<A: Artifact>(
        &self,
        artifact: &A,
        namespace: &Namespace,
        taken: &[Window],
    ) -> Result<Option<Step>, Unrunnable> {
        let progress = self.progress(namespace, &artifact.series())?;
        let base = match A::From::KIND == DeclaredSnapshot::ARTIFACT {
            false => None,
            true => Some(self.progress(self.snapshot.namespace(), &self.snapshot.series())?),
        };

        Ok(progress.step(base.as_ref(), taken, |built, base| {
            artifact.next(&self.timeline, built, base)
        }))
    }

    pub fn take<A: Artifact, S: Settlement>(
        &self,
        artifact: &A,
        namespace: &Namespace,
        window: &Window,
        tables: &Tables,
        settlement: &S,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable> {
        let previous = match A::From::KIND == DeclaredSnapshot::ARTIFACT {
            false => self.built(namespace, &artifact.series(), window)?,
            true => self.built(self.snapshot.namespace(), &self.snapshot.series(), window)?,
        };
        let settlement = settlement.advancing(namespace, &artifact.series());

        journal.window(window);
        let output = self
            .planner
            .plan(artifact, window, &previous, tables, &settlement)?;

        settlement.report(window, output, journal)
    }

    pub fn shard(&self, date: NaiveDate) -> Option<Window> {
        self.snapshot.schedule().shard(self.source.since(), date)
    }

    pub fn dated(&self, window: Option<Window>, date: NaiveDate) -> Result<Window, Unrunnable> {
        match window {
            None => Err(Unrunnable::Unsharded(date)),
            Some(window) if !window.is_open() && *window.to() > self.now => {
                Err(Unrunnable::Unclosed(window))
            }
            Some(window) => Ok(window),
        }
    }

    fn built(
        &self,
        namespace: &Namespace,
        series: &Series,
        window: &Window,
    ) -> Result<Vec<BuildLog>, Unrunnable> {
        let client = self.planner.client();
        client.create_build_log::<Unrunnable>(namespace)?;

        Ok(client
            .build_logs(namespace, &Latest::SucceededUntil(*window.from()).into())?
            .into_iter()
            .filter(|log| log.advances(series))
            .collect())
    }

    fn progress(&self, namespace: &Namespace, series: &Series) -> Result<Progress, Unrunnable> {
        let client = self.planner.client();
        client.create_build_log::<Unrunnable>(namespace)?;
        let progress = Progress::read(
            &client.build_logs(namespace, &Latest::Attempted.into())?,
            &client.build_logs(namespace, &Latest::Succeeded.into())?,
            series,
        );
        let configured = self.source.capture().to_json()?;

        let changed = progress
            .captures()
            .find(|logged| *logged != configured)
            .map(str::to_owned);

        match changed {
            Some(logged) => Err(Unrunnable::CaptureChanged { logged, configured }),
            None => Ok(progress),
        }
    }
}

#[cfg(test)]
mod tests {
    use lithify_protocol::sink::duckdb::client::DuckDbClient;

    use crate::command::RunCommand;
    use crate::command::tests::settle;
    use crate::plan::tests::{
        BUILT_CHANGE_LOGS, CAPTURE, PEOPLE, built_before, logged, pipeline, rows,
    };
    use crate::unrunnable::Unrunnable;

    #[test]
    fn a_pipeline_catches_up_window_by_window_folding_each_onto_the_one_before() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, COALESCE(base, '') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                ("mysql_shop.people_20260919".to_owned(), String::new()),
                (
                    "mysql_shop.people_20260920".to_owned(),
                    "mysql_shop.people_20260919".to_owned()
                ),
                (
                    "mysql_shop.people_20260921".to_owned(),
                    "mysql_shop.people_20260920".to_owned()
                ),
            ]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260921 ORDER BY id"
            ),
            [
                ("7".to_owned(), "kept".to_owned()),
                ("8".to_owned(), "kept".to_owned())
            ]
        );
    }

    #[test]
    fn a_pipeline_built_for_the_first_time_builds_only_the_shard_closing_at_the_last_midnight() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, strftime(window_from, '%Y-%m-%d') || '..' || strftime(window_to, '%Y-%m-%d') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [(
                "mysql_shop.people_20260921".to_owned(),
                "1970-01-01..2026-09-22".to_owned()
            )]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260921 ORDER BY id"
            ),
            [("8".to_owned(), "kept".to_owned())]
        );
    }

    #[test]
    fn a_pipeline_caught_up_to_a_date_builds_no_shard_dated_after_it() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, COALESCE(base, '') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                ("mysql_shop.people_20260919".to_owned(), String::new()),
                (
                    "mysql_shop.people_20260920".to_owned(),
                    "mysql_shop.people_20260919".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn a_pipeline_built_for_the_first_time_up_to_a_date_is_caught_up_from_there_after() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();
        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, strftime(window_from, '%Y-%m-%d') || '..' || strftime(window_to, '%Y-%m-%d') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                (
                    "mysql_shop.people_20260920".to_owned(),
                    "1970-01-01..2026-09-21".to_owned()
                ),
                (
                    "mysql_shop.people_20260921".to_owned(),
                    "2026-09-21..2026-09-22".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn a_pipeline_is_not_caught_up_to_a_date_whose_shard_has_not_closed() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        let unclosed = settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-22"),
            "2026-09-22T05:00:00Z",
        );

        assert!(matches!(unclosed, Err(Unrunnable::Unclosed(_))));
    }

    #[test]
    fn a_pipeline_is_not_caught_up_to_a_date_before_its_source_began() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        let unsharded = settle(
            &pipeline,
            &client,
            &caught_up_to("1969-12-31"),
            "2026-09-22T05:00:00Z",
        );

        assert!(matches!(unsharded, Err(Unrunnable::Unsharded(_))));
    }

    #[test]
    fn a_pipeline_is_not_caught_up_to_a_date_the_build_log_has_already_built_past() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);
        settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        let overtaken = settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-19"),
            "2026-09-22T05:00:00Z",
        );

        assert!(matches!(
            overtaken,
            Err(Unrunnable::Overtaken { date, .. }) if date == "2026-09-19".parse().unwrap()
        ));
    }

    #[test]
    fn a_pipeline_caught_up_to_the_date_it_was_last_built_to_builds_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);
        settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        let journal = settle(
            &pipeline,
            &client,
            &caught_up_to("2026-09-20"),
            "2026-09-22T05:00:00Z",
        )
        .unwrap();

        assert_eq!(journal.entries().count(), 0);
    }

    fn caught_up_to(date: &str) -> RunCommand {
        RunCommand::CatchUp {
            to: Some(date.parse().unwrap()),
        }
    }

    #[test]
    fn a_pipeline_built_for_the_first_time_before_its_first_midnight_builds_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        let journal = settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp { to: None },
            "1970-01-01T23:00:00Z",
        )
        .unwrap();

        assert_eq!(journal.entries().count(), 0);
    }

    #[test]
    fn a_pipeline_that_has_caught_up_builds_nothing() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        let journal = settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp { to: None },
            "2026-09-20T23:00:00Z",
        )
        .unwrap();

        assert_eq!(journal.entries().count(), 0);
    }

    fn attempts(client: &DuckDbClient) -> Vec<(String, String)> {
        rows(
            client,
            "SELECT target, status FROM mysql_shop._lithify_log WHERE strftime(window_from, '%Y%m%d') >= '20260920' ORDER BY id",
        )
    }

    #[test]
    fn a_table_failing_in_a_window_stops_itself_alone_and_is_tried_again_by_the_next_catch_up() {
        let pipeline = pipeline();
        let client = logged(
            BUILT_CHANGE_LOGS,
            &[
                built_before("people", CAPTURE),
                built_before("pets", CAPTURE),
            ],
        );
        let attempted = |target: &str, status: &str| (target.to_owned(), status.to_owned());

        let journal = settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        )
        .unwrap();
        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp { to: None },
            "2026-09-22T06:00:00Z",
        )
        .unwrap();

        assert!(!journal.complete());
        assert_eq!(
            attempts(&client),
            [
                attempted("mysql_shop.people_20260920", "succeeded"),
                attempted("mysql_shop.pets_20260920", "failed"),
                attempted("mysql_shop.people_20260921", "succeeded"),
                attempted("mysql_shop.pets_20260920", "failed"),
            ]
        );
    }

    #[test]
    fn a_pipeline_whose_capture_changed_since_its_last_run_is_not_caught_up() {
        let pipeline = pipeline();
        let client = logged(
            PEOPLE,
            &[built_before(
                "people",
                r#"{"debezium":{"dialect":"postgres"}}"#,
            )],
        );

        let changed = settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp { to: None },
            "2026-09-22T05:00:00Z",
        );

        assert!(matches!(changed, Err(Unrunnable::CaptureChanged { .. })));
    }
}

#[cfg(test)]
mod derived {
    use lithify_config::Pipeline;

    use crate::command::RunCommand;
    use crate::command::tests::settle;
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
            &RunCommand::CatchUp { to: None },
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
    fn a_catch_up_builds_the_current_state_again_only_once_its_snapshot_has_stepped() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);
        let pipeline = derived(
            "scd1-once",
            "[artifact.scd1]\n",
            "place.scd1 = [{ prefix = \"mysql_shop_scd1\" }]\n",
            "[default]\nartifacts = [\"snapshot\", \"scd1\"]\n",
        );

        for now in [
            "2026-09-21T05:00:00Z",
            "2026-09-21T06:00:00Z",
            "2026-09-22T05:00:00Z",
        ] {
            settle(&pipeline, &client, &RunCommand::CatchUp { to: None }, now).unwrap();
        }

        assert_eq!(
            rows(
                &client,
                "SELECT base, '' FROM mysql_shop_scd1._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                ("mysql_shop.people_20260920".to_owned(), String::new()),
                ("mysql_shop.people_20260921".to_owned(), String::new())
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
            &RunCommand::CatchUp { to: None },
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
            &RunCommand::CatchUp { to: None },
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
    fn a_catch_up_cuts_an_instant_first_in_the_last_day_its_snapshot_was_built_for_and_then_each_day_after()
     {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);
        let pipeline = cut_at_six("cut-each-day");
        let cut = |target: &str, base: &str| (target.to_owned(), base.to_owned());

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp { to: None },
            "2026-09-21T07:00:00Z",
        )
        .unwrap();
        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp { to: None },
            "2026-09-23T07:00:00Z",
        )
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, base FROM mysql_shop_instant._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                cut(
                    "mysql_shop_instant.people_2026092005",
                    "mysql_shop.people_20260919"
                ),
                cut(
                    "mysql_shop_instant.people_2026092105",
                    "mysql_shop.people_20260920"
                ),
                cut(
                    "mysql_shop_instant.people_2026092205",
                    "mysql_shop.people_20260921"
                ),
                cut(
                    "mysql_shop_instant.people_2026092305",
                    "mysql_shop.people_20260922"
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
            &RunCommand::CatchUp { to: None },
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
