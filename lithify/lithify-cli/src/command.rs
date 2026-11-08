use chrono::NaiveDate;

use lithify_build::sink::WarehouseClient;
use lithify_pipeline::build_log::Series;
use lithify_pipeline::capture::Capture;
use lithify_pipeline::infra::Sink;
use lithify_pipeline::stepping::{Stepping, Target};
use lithify_pipeline::warehouse::WarehouseControl;
use lithify_schema::window::Window;

use crate::artifacts::ArtifactConfig;
use crate::journal::Journal;
use crate::pipeline::{CaptureConfig, WarehouseConfig};
use crate::report::Report;
use crate::unrunnable::Unrunnable;

pub enum RunCommand {
    CatchUp {
        target: Target,
        to: Option<NaiveDate>,
    },
    Build(Target),
}

impl RunCommand {
    pub async fn settle<K, B>(
        &self,
        sink: &Sink<WarehouseConfig, ArtifactConfig>,
        stepping: Stepping<'_, K, CaptureConfig, Unrunnable>,
        writer: &B,
        journal: &mut Journal,
    ) -> Result<(), Unrunnable>
    where
        K: WarehouseControl,
        CaptureConfig: Capture<K>,
        Unrunnable: From<<K as WarehouseClient>::Error>,
        B: Report,
    {
        match self {
            Self::CatchUp { target, to: None } => {
                stepping
                    .catch_up(sink, target, writer, Self::reporting(writer, journal))
                    .await
            }
            Self::CatchUp {
                target,
                to: Some(date),
            } => {
                stepping
                    .up_to(*date, sink.scheduled())
                    .await?
                    .catch_up(sink, target, writer, Self::reporting(writer, journal))
                    .await
            }
            Self::Build(target) => {
                let step = stepping
                    .step(sink, target, writer, Self::reporting(writer, journal))
                    .await?;
                if step.is_none() {
                    journal.note("nothing is left to build");
                }

                Ok(())
            }
        }
    }

    fn reporting<B: Report>(
        writer: &B,
        journal: &mut Journal,
    ) -> impl FnMut(&Series, &Window, Result<B::Output, Unrunnable>) -> Result<(), Unrunnable> {
        move |series, window, output| {
            journal.window(window);
            writer.report(series, window, output?, journal)
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use lithify_build::artifact::ArtifactName;
    use lithify_pipeline::Pipeline;
    use lithify_pipeline::check::Expectation;
    use lithify_pipeline::runner::BuildLogWrite;
    use lithify_protocol::sink::duckdb::client::DuckDbClient;
    use lithify_schema::ts::Ts;

    use super::*;
    use crate::pipeline::CaptureConfig;
    use crate::step::tests::{CAPTURE, PEOPLE, at, built_before, logged, pipeline, rows};

    const NOW: &str = "2026-09-22T05:00:00Z";

    pub(crate) async fn settle(
        pipeline: &Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig>,
        client: &DuckDbClient,
        command: &RunCommand,
        now: &str,
    ) -> Result<Journal, Unrunnable> {
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "duckdb_s3")
            .unwrap();
        let writer =
            BuildLogWrite::of(source, Ts::from(at(now)), env!("CARGO_PKG_VERSION")).unwrap();
        let mut journal = Journal::default();

        command
            .settle(
                sink,
                Stepping::new(
                    client,
                    source,
                    Expectation::of(sink, source, Ts::from(at(now))).unwrap(),
                    pipeline.tables(),
                ),
                &writer,
                &mut journal,
            )
            .await?;

        Ok(journal)
    }

    fn built(client: &DuckDbClient) -> Vec<(String, String)> {
        rows(
            client,
            "SELECT target, COALESCE(base, '') FROM mysql_shop._lithify_log WHERE status = 'succeeded' ORDER BY id",
        )
    }

    fn folded(target: &str, base: &str) -> (String, String) {
        (target.to_owned(), base.to_owned())
    }

    const PERSON_PROCESSED_PAST_THE_MIDNIGHT_ITS_EVENT_TIME_FALLS_BEFORE: &str = include_str!(
        "../testdata/command/person_processed_past_the_midnight_its_event_time_falls_before.sql"
    );

    const PERSON_DELETED_AT_AN_EVENT_TIME_BEFORE_ITS_CREATION: &str =
        include_str!("../testdata/command/person_deleted_at_an_event_time_before_its_creation.sql");

    #[tokio::test]
    async fn a_change_is_built_into_the_shard_of_the_day_its_event_time_falls_on() {
        let pipeline = pipeline();
        let client = logged(
            PERSON_PROCESSED_PAST_THE_MIDNIGHT_ITS_EVENT_TIME_FALLS_BEFORE,
            &[],
        )
        .await;
        let to = RunCommand::CatchUp {
            target: Target::default(),
            to: Some("2026-09-20".parse().unwrap()),
        };

        settle(&pipeline, &client, &to, NOW).await.unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260920"
            ),
            [("8".to_owned(), "kept".to_owned())]
        );
    }

    #[tokio::test]
    async fn changes_within_a_shard_are_folded_in_the_order_of_their_processing_time() {
        let pipeline = pipeline();
        let client = logged(PERSON_DELETED_AT_AN_EVENT_TIME_BEFORE_ITS_CREATION, &[]).await;
        let to = RunCommand::CatchUp {
            target: Target::default(),
            to: Some("2026-09-20".parse().unwrap()),
        };

        settle(&pipeline, &client, &to, NOW).await.unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'kept' FROM mysql_shop.people_20260920"
            ),
            []
        );
    }

    fn next(artifact: Option<&str>) -> RunCommand {
        RunCommand::Build(Target::new(artifact.map(ArtifactName::from)))
    }

    #[tokio::test]
    async fn a_build_named_nothing_takes_the_one_step_the_build_log_says_is_next() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]).await;

        settle(&pipeline, &client, &next(None), NOW).await.unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
            ]
        );
    }

    #[tokio::test]
    async fn a_build_named_an_artifact_takes_the_next_step_of_that_artifact() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]).await;
        let pipeline = crate::step::tests::opened(
            "next-scd2",
            &format!(
                "{}[[artifact]]\nname = \"scd2\"\nkind = \"scd2\"\nshard = \"%Y%m%d\"\n",
                crate::step::tests::ARTIFACTS
            ),
            &format!(
                "{}place.scd2 = {{ prefix = \"mysql_shop_scd2\" }}\n",
                crate::step::tests::INFRA
            ),
            "[default]\nartifacts = [\"snapshot\", \"scd2\"]\n",
        );

        settle(&pipeline, &client, &next(Some("scd2")), NOW)
            .await
            .unwrap();

        assert_eq!(built(&client), [folded("mysql_shop.people_20260919", "")]);
        assert_eq!(
            rows(
                &client,
                "SELECT target, '' FROM mysql_shop_scd2._lithify_log WHERE status = 'succeeded'"
            ),
            [(
                "mysql_shop_scd2.people_open_20260921".to_owned(),
                String::new()
            )]
        );
    }

    const PETS_STANDING_BEFORE_THEY_WERE_BUILT: &str =
        include_str!("../testdata/command/pets_standing_before_they_were_built.sql");

    #[tokio::test]
    async fn a_catch_up_builds_a_table_with_nothing_to_fold_onto_again_from_the_start_of_its_source()
     {
        let pipeline = pipeline();
        let client = logged(
            PETS_STANDING_BEFORE_THEY_WERE_BUILT,
            &[built_before("people", CAPTURE)],
        )
        .await;

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            NOW,
        )
        .await
        .unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
                folded("mysql_shop.pets_20260921", ""),
                folded("mysql_shop.people_20260921", "mysql_shop.people_20260920"),
            ]
        );
        assert_eq!(ids(&client, "pets_20260921"), ["9"]);
    }

    fn ids(client: &DuckDbClient, table: &str) -> Vec<String> {
        rows(
            client,
            &format!("SELECT id::VARCHAR, '' FROM mysql_shop.{table} ORDER BY id"),
        )
        .into_iter()
        .map(|(id, _)| id)
        .collect()
    }
}

#[cfg(test)]
mod catch_up {
    use lithify_pipeline::runner::Unsteppable;

    use crate::command::tests::settle;
    use crate::command::{RunCommand, Target};
    use crate::step::tests::{PEOPLE, built_before, logged, pipeline};
    use crate::unrunnable::Unrunnable;

    #[tokio::test]
    async fn a_pipeline_whose_capture_changed_since_its_last_run_is_not_caught_up() {
        let pipeline = pipeline();
        let client = logged(
            PEOPLE,
            &[built_before(
                "people",
                r#"{"debezium":{"postgres":{"topic_prefix":"postgres"}}}"#,
            )],
        )
        .await;

        let changed = settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        )
        .await;

        assert!(matches!(
            changed,
            Err(Unrunnable::Unsteppable(Unsteppable::CaptureChanged(_)))
        ));
    }
}

#[cfg(test)]
mod derived {
    use lithify_pipeline::Pipeline;

    use crate::command::tests::settle;
    use crate::command::{RunCommand, Target};
    use crate::pipeline::CaptureConfig;
    use crate::step::tests::{
        ARTIFACTS, CAPTURE, INFRA, PEOPLE, built_before, logged, opened, rows,
    };

    use super::*;

    fn derived(
        name: &str,
        declared: &str,
        placed: &str,
        listed: &str,
    ) -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        opened(
            name,
            &format!("{ARTIFACTS}{declared}"),
            &format!("{INFRA}{placed}"),
            listed,
        )
    }

    fn cut_at_six(name: &str) -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        derived(
            name,
            "[[artifact]]\nname = \"instant_0600\"\nkind = \"instant\"\nbase = \"snapshot\"\nat = \"06:00\"\nshard = \"%Y%m%d%H\"\n",
            "place.instant_0600 = { prefix = \"mysql_shop_instant\" }\n",
            "[default]\nartifacts = [\"snapshot\", \"instant_0600\"]\n",
        )
    }

    fn provided(name: &str) -> Pipeline<CaptureConfig, WarehouseConfig, ArtifactConfig> {
        derived(
            name,
            "[[artifact]]\nname = \"provided\"\nkind = \"snapshot_provide\"\nbase = \"snapshot\"\nshard = \"%Y%m%d\"\n",
            "place.provided = { prefix = \"mysql_shop_provided\" }\n",
            "[default]\nartifacts = [\"snapshot\", \"provided\"]\n",
        )
    }

    #[tokio::test]
    async fn a_catch_up_folds_the_change_log_after_the_last_daily_snapshot_into_the_current_state()
    {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]).await;
        let pipeline = derived(
            "scd1",
            "[[artifact]]\nname = \"scd1\"\nkind = \"scd1\"\nbase = \"snapshot\"\n",
            "place.scd1 = { prefix = \"mysql_shop_scd1\" }\n",
            "[default]\nartifacts = [\"snapshot\", \"scd1\"]\n",
        );

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-21T05:00:00Z",
        )
        .await
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, base FROM mysql_shop_scd1._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [(
                "mysql_shop_scd1.people_scd_type_1".to_owned(),
                "mysql_shop.people_20260920".to_owned()
            )]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'people' FROM mysql_shop_scd1.people_scd_type_1 ORDER BY id"
            ),
            [
                ("7".to_owned(), "people".to_owned()),
                ("8".to_owned(), "people".to_owned())
            ]
        );
    }

    #[tokio::test]
    async fn a_catch_up_builds_the_current_state_again_only_once_its_snapshot_has_stepped() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]).await;
        let pipeline = derived(
            "scd1-once",
            "[[artifact]]\nname = \"scd1\"\nkind = \"scd1\"\nbase = \"snapshot\"\n",
            "place.scd1 = { prefix = \"mysql_shop_scd1\" }\n",
            "[default]\nartifacts = [\"snapshot\", \"scd1\"]\n",
        );

        for now in [
            "2026-09-21T05:00:00Z",
            "2026-09-21T06:00:00Z",
            "2026-09-22T05:00:00Z",
        ] {
            settle(
                &pipeline,
                &client,
                &RunCommand::CatchUp {
                    target: Target::default(),
                    to: None,
                },
                now,
            )
            .await
            .unwrap();
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

    #[tokio::test]
    async fn a_catch_up_keeps_every_version_of_each_window_it_builds() {
        let client = logged(PEOPLE, &[]).await;
        let pipeline = derived(
            "scd2",
            "[[artifact]]\nname = \"scd2\"\nkind = \"scd2\"\nshard = \"%Y%m%d\"\n",
            "place.scd2 = { prefix = \"mysql_shop_scd2\" }\n",
            "[default]\nartifacts = [\"snapshot\", \"scd2\"]\n",
        );

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        )
        .await
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

    #[tokio::test]
    async fn a_catch_up_builds_an_artifact_only_for_the_tables_that_name_it() {
        let client = logged(PEOPLE, &[]).await;
        let pipeline = derived(
            "scd2-unnamed",
            "[[artifact]]\nname = \"scd2\"\nkind = \"scd2\"\nshard = \"%Y%m%d\"\n",
            "place.scd2 = { prefix = \"mysql_shop_scd2\" }\n",
            "[default]\nartifacts = [\"snapshot\"]\n",
        );

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        )
        .await
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, '' FROM mysql_shop_scd2._lithify_log WHERE status = 'succeeded'"
            ),
            []
        );
    }

    #[tokio::test]
    async fn a_catch_up_cuts_an_instant_first_in_the_last_day_its_snapshot_was_built_for_and_then_each_day_after()
     {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]).await;
        let pipeline = cut_at_six("cut-each-day");
        let cut = |target: &str, base: &str| (target.to_owned(), base.to_owned());

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-21T07:00:00Z",
        )
        .await
        .unwrap();
        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-23T07:00:00Z",
        )
        .await
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, base FROM mysql_shop_instant._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                cut(
                    "mysql_shop_instant.people_2026092106",
                    "mysql_shop.people_20260920"
                ),
                cut(
                    "mysql_shop_instant.people_2026092206",
                    "mysql_shop.people_20260921"
                ),
                cut(
                    "mysql_shop_instant.people_2026092306",
                    "mysql_shop.people_20260922"
                ),
            ]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'people' FROM mysql_shop_instant.people_2026092106 ORDER BY id"
            ),
            [
                ("7".to_owned(), "people".to_owned()),
                ("8".to_owned(), "people".to_owned())
            ]
        );
    }

    #[tokio::test]
    async fn a_catch_up_provides_each_shard_its_snapshot_built_in_turn_from_the_last_built_before_it_was_first_provided()
     {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]).await;
        let pipeline = provided("provide-each-shard");
        let shard = |target: &str, base: &str| (target.to_owned(), base.to_owned());

        for now in ["2026-09-21T05:00:00Z", "2026-09-23T05:00:00Z"] {
            settle(
                &pipeline,
                &client,
                &RunCommand::CatchUp {
                    target: Target::default(),
                    to: None,
                },
                now,
            )
            .await
            .unwrap();
        }

        assert_eq!(
            rows(
                &client,
                "SELECT target, base FROM mysql_shop_provided._lithify_log WHERE status = 'succeeded' ORDER BY id"
            ),
            [
                shard(
                    "mysql_shop_provided.people_20260920",
                    "mysql_shop.people_20260920"
                ),
                shard(
                    "mysql_shop_provided.people_20260921",
                    "mysql_shop.people_20260921"
                ),
                shard(
                    "mysql_shop_provided.people_20260922",
                    "mysql_shop.people_20260922"
                ),
            ]
        );
        assert_eq!(
            rows(
                &client,
                "SELECT id::VARCHAR, 'people' FROM mysql_shop_provided.people_20260920 ORDER BY id"
            ),
            rows(
                &client,
                "SELECT id::VARCHAR, 'people' FROM mysql_shop.people_20260920 ORDER BY id"
            )
        );
    }

    #[tokio::test]
    async fn a_catch_up_provides_first_the_last_shard_its_snapshot_built_before_it() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]).await;
        let pipeline = provided("provide-from-the-last");

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::new(Some("snapshot".into())),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        )
        .await
        .unwrap();
        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T06:00:00Z",
        )
        .await
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, base FROM mysql_shop_provided._lithify_log ORDER BY id"
            ),
            [(
                "mysql_shop_provided.people_20260921".to_owned(),
                "mysql_shop.people_20260921".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn a_catch_up_provides_no_shard_its_snapshot_has_not_built_yet() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]).await;
        let pipeline = provided("provide-what-is-built");

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-21T05:00:00Z",
        )
        .await
        .unwrap();
        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::new(Some("provided".into())),
                to: None,
            },
            "2026-09-23T05:00:00Z",
        )
        .await
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, status FROM mysql_shop_provided._lithify_log ORDER BY id"
            ),
            [(
                "mysql_shop_provided.people_20260920".to_owned(),
                "succeeded".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn a_catch_up_cuts_an_instant_never_cut_on_the_last_shard_its_snapshot_built_while_it_lags()
     {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]).await;
        let pipeline = cut_at_six("cut-behind");

        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::new(Some("snapshot".into())),
                to: None,
            },
            "2026-09-21T05:00:00Z",
        )
        .await
        .unwrap();
        settle(
            &pipeline,
            &client,
            &RunCommand::CatchUp {
                target: Target::new(Some("instant_0600".into())),
                to: None,
            },
            "2026-09-23T07:00:00Z",
        )
        .await
        .unwrap();

        assert_eq!(
            rows(
                &client,
                "SELECT target, base FROM mysql_shop_instant._lithify_log ORDER BY id"
            ),
            [(
                "mysql_shop_instant.people_2026092106".to_owned(),
                "mysql_shop.people_20260920".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn a_catch_up_named_an_artifact_advances_that_one_alone() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]).await;
        let pipeline = cut_at_six("one-at-a-time");
        let caught_up = |artifact: &str| RunCommand::CatchUp {
            target: Target::new(Some(artifact.into())),
            to: None,
        };
        let instants = |client: &lithify_protocol::sink::duckdb::client::DuckDbClient| {
            rows(
                client,
                "SELECT target, base FROM mysql_shop_instant._lithify_log WHERE status = 'succeeded' ORDER BY id",
            )
        };

        settle(
            &pipeline,
            &client,
            &caught_up("snapshot"),
            "2026-09-21T07:00:00Z",
        )
        .await
        .unwrap();
        let untouched = !lithify_build::sink::WarehouseClient::exists(
            &client,
            &lithify_build::sink::TablePath::new("mysql_shop_instant", "_lithify_log"),
        )
        .await
        .unwrap();
        settle(
            &pipeline,
            &client,
            &caught_up("instant_0600"),
            "2026-09-21T07:00:00Z",
        )
        .await
        .unwrap();

        assert!(untouched);
        assert_eq!(
            instants(&client),
            [(
                "mysql_shop_instant.people_2026092106".to_owned(),
                "mysql_shop.people_20260920".to_owned()
            )]
        );
    }

    #[tokio::test]
    async fn an_instant_not_yet_reached_is_not_cut() {
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]).await;

        settle(
            &cut_at_six("not-yet-cut"),
            &client,
            &RunCommand::CatchUp {
                target: Target::default(),
                to: None,
            },
            "2026-09-22T05:00:00Z",
        )
        .await
        .unwrap();

        assert!(
            !lithify_build::sink::WarehouseClient::exists(
                &client,
                &lithify_pipeline::build_log::BuildLog::path(
                    &lithify_build::sink::Namespace::from("mysql_shop_instant")
                ),
            )
            .await
            .unwrap()
        );
    }
}
