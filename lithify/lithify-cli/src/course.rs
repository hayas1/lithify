use chrono::{DateTime, Utc};

use lithify_config::infra::{Sink, Source};
use lithify_core::position::RecordedAt;
use lithify_core::sink::WarehouseClient;
use lithify_core::window::Window;

use crate::catch_up::CatchUp;
use crate::journal::Journal;
use crate::pipeline::{Capture, Warehouse};
use crate::plan::Base;
use crate::settlement::Settlement;
use crate::unrunnable::Unrunnable;

pub enum Course {
    CatchUp,
    Window(Window, Base),
}

impl Course {
    pub fn window(window: Window, onto: Option<RecordedAt>) -> Self {
        let base = match onto {
            Some(onto) => Base::Shard(Window::new(onto, *window.from())),
            None => Base::default(),
        };

        Self::Window(window, base)
    }

    pub fn itinerary<K>(
        &self,
        sink: &Sink<Warehouse>,
        source: &Source<Capture>,
        now: DateTime<Utc>,
        client: &K,
    ) -> Result<Itinerary, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        match self {
            Self::CatchUp => CatchUp::new(sink, source, now).itinerary(client),
            Self::Window(window, base) => Ok(Itinerary::new(vec![window.clone()], base.clone())),
        }
    }
}

pub struct Itinerary {
    windows: Vec<Window>,
    base: Base,
}

impl Itinerary {
    pub fn new(windows: Vec<Window>, base: Base) -> Self {
        Self { windows, base }
    }

    pub fn settle<S: Settlement>(
        self,
        settlement: &S,
        journal: &mut Journal,
        mut plan: impl FnMut(&Window, &Base) -> Result<S::Output, Unrunnable>,
    ) -> Result<(), Unrunnable> {
        let mut base = self.base;

        for window in self.windows {
            journal.window(&window);
            if !settlement.report(&window, plan(&window, &base)?, journal)? {
                break;
            }
            base = Base::Shard(window);
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use lithify_config::Pipeline;
    use lithify_protocol::sink::duckdb::client::{DuckDbCapture, DuckDbClient};

    use super::*;
    use crate::plan::SnapshotPlan;
    use crate::plan::tests::{CAPTURE, PEOPLE, at, built_before, logged, pipeline, rows};
    use crate::settlement::Build;

    const NOW: &str = "2026-09-22T05:00:00Z";

    fn day(from: &str, to: &str) -> Window {
        Window::new(RecordedAt::from(at(from)), RecordedAt::from(at(to)))
    }

    pub(crate) fn settle(
        pipeline: &Pipeline<Capture, Warehouse>,
        client: &DuckDbClient,
        course: &Course,
        now: &str,
    ) -> Result<Journal, Unrunnable> {
        let source = pipeline.sources().next().unwrap();
        let sink = pipeline
            .sinks()
            .find(|sink| sink.name() == "rustfs")
            .unwrap();
        let settlement = Build::new(sink, source).unwrap();
        let mut journal = Journal::default();

        course.itinerary(sink, source, at(now), client)?.settle(
            &settlement,
            &mut journal,
            |window, base| {
                source.capture().with_duckdb_change_logs(
                    source.topic_prefix().unwrap(),
                    SnapshotPlan::new(client, sink, window, base, &settlement),
                )
            },
        )?;

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

    #[test]
    fn a_window_given_without_a_base_is_built_from_nothing_whatever_was_built_before() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(
            &pipeline,
            &client,
            &Course::window(day("2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z"), None),
            NOW,
        )
        .unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", ""),
            ]
        );
    }

    #[test]
    fn a_window_given_onto_an_earlier_one_folds_onto_its_snapshot() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[]);

        settle(
            &pipeline,
            &client,
            &Course::window(
                day("2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z"),
                Some(RecordedAt::from(at("2026-09-19T00:00:00Z"))),
            ),
            NOW,
        )
        .unwrap();

        assert_eq!(
            built(&client),
            [folded(
                "mysql_shop.people_20260920",
                "mysql_shop.people_20260919"
            )]
        );
    }

    #[test]
    fn a_window_rebuilt_behind_the_latest_makes_the_next_catch_up_rebuild_what_folded_onto_it() {
        let pipeline = pipeline();
        let client = logged(PEOPLE, &[built_before("people", CAPTURE)]);

        settle(&pipeline, &client, &Course::CatchUp, NOW).unwrap();
        settle(
            &pipeline,
            &client,
            &Course::window(
                day("2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z"),
                Some(RecordedAt::from(at("2026-09-19T00:00:00Z"))),
            ),
            NOW,
        )
        .unwrap();
        settle(&pipeline, &client, &Course::CatchUp, NOW).unwrap();

        assert_eq!(
            built(&client),
            [
                folded("mysql_shop.people_20260919", ""),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
                folded("mysql_shop.people_20260921", "mysql_shop.people_20260920"),
                folded("mysql_shop.people_20260920", "mysql_shop.people_20260919"),
                folded("mysql_shop.people_20260921", "mysql_shop.people_20260920"),
            ]
        );
    }
}
