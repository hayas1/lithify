use lithify_core::build_log::{BuildLog, Latest};
use lithify_core::schedule::Schedule;
use lithify_core::sink::WarehouseClient;
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;

use crate::unrunnable::Unrunnable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resumption {
    FromOrigin(Ts<Event>),
    FromBuilt(Ts<Event>),
}

impl Resumption {
    pub fn at(start: Ts<Event>, origin: Ts<Event>) -> Self {
        match start > origin {
            true => Self::FromBuilt(start),
            false => Self::FromOrigin(origin),
        }
    }

    pub fn read(
        latest: &[BuildLog],
        succeeded: &[BuildLog],
        origin: Ts<Event>,
        capture: &str,
    ) -> Result<Self, Unrunnable> {
        let Some(last_run) = latest.iter().map(BuildLog::run_id).max() else {
            return Ok(Self::FromOrigin(origin));
        };
        let built_last = latest.iter().filter(|log| log.run_id() == last_run);

        if let Some(changed) = built_last.clone().find(|log| log.capture() != capture) {
            return Err(Unrunnable::CaptureChanged {
                logged: changed.capture().to_owned(),
                configured: capture.to_owned(),
            });
        }

        // TODO: rewind to the origin for a table the catalog holds but the build log never saw, and
        // stop waiting on one the build log holds but the catalog no longer does.
        let start = latest
            .iter()
            .filter(|log| log.table_to().is_some())
            .map(|log| {
                log.last_success(succeeded)
                    .map_or(origin, |built| *built.window().to())
            })
            .min()
            .unwrap_or(origin);

        Ok(Self::at(start, origin))
    }

    pub fn windows(&self, schedule: Schedule, until: Ts<Event>) -> Vec<Window> {
        match self {
            Self::FromOrigin(origin) => schedule.span(*origin, until).into_iter().collect(),
            Self::FromBuilt(start) => schedule.windows(*start, until).collect(),
        }
    }

    pub fn previous<K>(&self, client: &K, namespace: &str) -> Result<Vec<BuildLog>, Unrunnable>
    where
        K: WarehouseClient,
        Unrunnable: From<K::Error>,
    {
        match self {
            Self::FromOrigin(_) => Ok(Vec::new()),
            Self::FromBuilt(start) => {
                Ok(client.build_logs(namespace, &Latest::SucceededUntil(*start).into())?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use ulid::Ulid;

    use lithify_core::build_log::{BuildLog, Outcome, Run, Status};
    use lithify_core::model::TableId;
    use lithify_core::sink::TablePath;
    use lithify_core::ts::Ts;
    use lithify_core::window::Window;

    use super::*;

    const CAPTURE: &str = r#"{"debezium":{"dialect":"mysql"}}"#;
    const ORIGIN: i64 = 0;
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
                DateTime::UNIX_EPOCH,
                "snapshot",
                "daily",
                "primary",
                capture,
                "0.1.0",
            ),
            Some(TableId::new("shop", table)),
            Some(TableId::new("shop", table)),
            None,
            Some(TablePath::new("mysql_shop", &format!("{table}_{from}"))),
            Window::new(Ts::try_from(from).unwrap(), Ts::try_from(to).unwrap()),
            Outcome::new(status, DateTime::from_timestamp_millis(to).unwrap()),
        )
    }

    fn renamed(run: u64, from: &str, to: &str, window: (i64, i64), status: Status) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::from_parts(run, 0),
                DateTime::UNIX_EPOCH,
                "snapshot",
                "daily",
                "primary",
                CAPTURE,
                "0.1.0",
            ),
            Some(TableId::new("shop", from)),
            Some(TableId::new("shop", to)),
            None,
            Some(TablePath::new("mysql_shop", &format!("{to}_{}", window.0))),
            Window::new(
                Ts::try_from(window.0).unwrap(),
                Ts::try_from(window.1).unwrap(),
            ),
            Outcome::new(status, DateTime::from_timestamp_millis(window.1).unwrap()),
        )
    }

    fn resumed(latest: &[BuildLog], succeeded: &[BuildLog]) -> Resumption {
        Resumption::read(latest, succeeded, Ts::try_from(ORIGIN).unwrap(), CAPTURE).unwrap()
    }

    #[test]
    fn a_pipeline_nothing_was_built_for_starts_from_its_origin() {
        assert_eq!(
            resumed(&[], &[]),
            Resumption::FromOrigin(Ts::try_from(ORIGIN).unwrap())
        );
    }

    #[test]
    fn a_pipeline_resumes_where_its_tables_were_last_built() {
        let people = logged(2, "people", W2, Status::Succeeded);
        let pets = logged(2, "pets", W2, Status::Succeeded);

        assert_eq!(
            resumed(&[people.clone(), pets.clone()], &[people, pets]),
            Resumption::FromBuilt(Ts::try_from(W2.1).unwrap())
        );
    }

    #[test]
    fn a_table_that_failed_last_time_takes_the_pipeline_back_to_where_it_was_last_built() {
        let people = logged(2, "people", W2, Status::Succeeded);
        let pets = logged(2, "pets", W2, Status::Failed("no era".to_owned()));
        let pets_before = logged(1, "pets", W1, Status::Succeeded);

        assert_eq!(
            resumed(&[people.clone(), pets], &[people, pets_before]),
            Resumption::FromBuilt(Ts::try_from(W1.1).unwrap())
        );
    }

    #[test]
    fn a_table_never_built_takes_the_pipeline_back_to_its_origin() {
        let people = logged(2, "people", W2, Status::Succeeded);
        let pets = logged(2, "pets", W2, Status::Failed("no era".to_owned()));

        assert_eq!(
            resumed(&[people.clone(), pets], &[people]),
            Resumption::FromOrigin(Ts::try_from(ORIGIN).unwrap())
        );
    }

    #[test]
    fn a_table_the_last_run_did_not_build_still_holds_the_pipeline_back() {
        let people = logged(2, "people", W2, Status::Succeeded);
        let left = logged(1, "pets", W1, Status::Succeeded);

        assert_eq!(
            resumed(&[left.clone(), people.clone()], &[left, people]),
            Resumption::FromBuilt(Ts::try_from(W1.1).unwrap())
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

        assert_eq!(
            resumed(&[renamed], &[before]),
            Resumption::FromBuilt(Ts::try_from(W1.1).unwrap())
        );
    }

    #[test]
    fn a_pipeline_built_up_to_its_origin_starts_from_its_origin() {
        assert_eq!(
            Resumption::at(Ts::try_from(ORIGIN).unwrap(), Ts::try_from(ORIGIN).unwrap()),
            Resumption::FromOrigin(Ts::try_from(ORIGIN).unwrap())
        );
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
            Ts::try_from(ORIGIN).unwrap(),
            CAPTURE,
        );

        assert!(matches!(changed, Err(Unrunnable::CaptureChanged { .. })));
    }
}
