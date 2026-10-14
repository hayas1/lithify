use lithify_core::build_log::BuildLog;
use lithify_core::position::RecordedAt;

use crate::unrunnable::Unrunnable;

pub struct Resumption {
    start: RecordedAt,
}

impl Resumption {
    pub fn read(
        latest: &[BuildLog],
        succeeded: &[BuildLog],
        since: RecordedAt,
        capture: &str,
    ) -> Result<Self, Unrunnable> {
        let Some(last_run) = latest.iter().map(BuildLog::run_id).max() else {
            return Ok(Self { start: since });
        };
        let built_last = latest.iter().filter(|log| log.run_id() == last_run);

        if let Some(changed) = built_last.clone().find(|log| log.capture() != capture) {
            return Err(Unrunnable::CaptureChanged {
                logged: changed.capture().to_owned(),
                configured: capture.to_owned(),
            });
        }

        let start = built_last
            .filter(|log| log.table_to().is_some())
            .map(|log| {
                log.last_success(succeeded)
                    .map_or(since, |built| *built.window().to())
            })
            .min()
            .unwrap_or(since);

        Ok(Self { start })
    }

    pub fn start(&self) -> RecordedAt {
        self.start
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use ulid::Ulid;

    use lithify_core::build_log::{BuildLog, Outcome, Run, Status};
    use lithify_core::model::TableId;
    use lithify_core::position::RecordedAt;
    use lithify_core::sink::TablePath;
    use lithify_core::window::Window;

    use super::*;

    const CAPTURE: &str = r#"{"debezium":{"dialect":"mysql"}}"#;
    const SINCE: i64 = 0;
    const W1: (i64, i64) = (1789862400000, 1789948800000);
    const W2: (i64, i64) = (1789948800000, 1790035200000);

    fn logged(run: u64, table: &str, (from, to): (i64, i64), status: Status) -> BuildLog {
        logged_under(run, CAPTURE, table, (from, to), status)
    }

    fn logged_under(
        run: u64,
        capture: &str,
        table: &str,
        (from, to): (i64, i64),
        status: Status,
    ) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::from_parts(run, 0),
                "snapshot",
                "daily",
                "primary",
                capture,
                "0.1.0",
            ),
            Some(TableId::new("shop", table)),
            Some(TableId::new("shop", table)),
            None,
            TablePath::new("mysql_shop", &format!("{table}_{from}")),
            Window::new(RecordedAt::from(from), RecordedAt::from(to)),
            Outcome::new(status, DateTime::from_timestamp_millis(to).unwrap()),
        )
    }

    fn renamed(run: u64, from: &str, to: &str, window: (i64, i64), status: Status) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::from_parts(run, 0),
                "snapshot",
                "daily",
                "primary",
                CAPTURE,
                "0.1.0",
            ),
            Some(TableId::new("shop", from)),
            Some(TableId::new("shop", to)),
            None,
            TablePath::new("mysql_shop", &format!("{to}_{}", window.0)),
            Window::new(RecordedAt::from(window.0), RecordedAt::from(window.1)),
            Outcome::new(status, DateTime::from_timestamp_millis(window.1).unwrap()),
        )
    }

    fn start(latest: &[BuildLog], succeeded: &[BuildLog]) -> RecordedAt {
        Resumption::read(latest, succeeded, RecordedAt::from(SINCE), CAPTURE)
            .unwrap()
            .start()
    }

    #[test]
    fn a_pipeline_nothing_was_built_for_starts_from_when_its_source_is_captured_since() {
        assert_eq!(start(&[], &[]), RecordedAt::from(SINCE));
    }

    #[test]
    fn a_pipeline_resumes_where_its_tables_were_last_built() {
        let people = logged(2, "people", W2, Status::Succeeded);
        let pets = logged(2, "pets", W2, Status::Succeeded);

        assert_eq!(
            start(&[people.clone(), pets.clone()], &[people, pets]),
            RecordedAt::from(W2.1)
        );
    }

    #[test]
    fn a_table_that_failed_last_time_takes_the_pipeline_back_to_where_it_was_last_built() {
        let people = logged(2, "people", W2, Status::Succeeded);
        let pets = logged(2, "pets", W2, Status::Failed("no era".to_owned()));
        let pets_before = logged(1, "pets", W1, Status::Succeeded);

        assert_eq!(
            start(&[people.clone(), pets], &[people, pets_before]),
            RecordedAt::from(W1.1)
        );
    }

    #[test]
    fn a_table_never_built_takes_the_pipeline_back_to_when_its_source_is_captured_since() {
        let people = logged(2, "people", W2, Status::Succeeded);
        let pets = logged(2, "pets", W2, Status::Failed("no era".to_owned()));

        assert_eq!(
            start(&[people.clone(), pets], &[people]),
            RecordedAt::from(SINCE)
        );
    }

    #[test]
    fn a_table_the_last_run_did_not_build_holds_the_pipeline_back_no_longer() {
        let people = logged(2, "people", W2, Status::Succeeded);
        let gone = logged(1, "gone", W1, Status::Succeeded);

        assert_eq!(
            start(&[gone.clone(), people.clone()], &[gone, people]),
            RecordedAt::from(W2.1)
        );
    }

    #[test]
    fn a_table_whose_rename_failed_to_build_resumes_where_it_was_last_built_under_its_old_name() {
        let before = logged(1, "customers", W1, Status::Succeeded);
        let renamed = renamed(
            2,
            "customers",
            "clients",
            W2,
            Status::Failed("no era".to_owned()),
        );

        assert_eq!(start(&[renamed], &[before]), RecordedAt::from(W1.1));
    }

    #[test]
    fn a_pipeline_whose_capture_changed_since_its_last_run_does_not_resume() {
        let people = logged_under(
            2,
            r#"{"debezium":{"dialect":"postgres"}}"#,
            "people",
            W2,
            Status::Succeeded,
        );

        let changed = Resumption::read(
            std::slice::from_ref(&people),
            std::slice::from_ref(&people),
            RecordedAt::from(SINCE),
            CAPTURE,
        );

        assert!(matches!(changed, Err(Unrunnable::CaptureChanged { .. })));
    }
}
