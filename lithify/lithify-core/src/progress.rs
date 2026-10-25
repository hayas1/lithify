use std::collections::{BTreeMap, BTreeSet};

use crate::build_log::BuildLog;
use crate::model::TableId;
use crate::ts::{Event, Ts};
use crate::window::Window;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Progress {
    built: BTreeMap<TableId, Option<Window>>,
    captures: BTreeSet<String>,
}

impl Progress {
    pub fn read(latest: &[BuildLog], succeeded: &[BuildLog]) -> Self {
        let last_run = latest.iter().map(BuildLog::run_id).max();

        Self {
            built: latest
                .iter()
                .filter_map(|log| {
                    let built = log
                        .last_success(succeeded)
                        .map(|built| built.window().clone());

                    Some((log.table_to()?.clone(), built))
                })
                .collect(),
            captures: latest
                .iter()
                .filter(|log| Some(log.run_id()) == last_run)
                .map(|log| log.capture().to_owned())
                .collect(),
        }
    }

    pub fn captures(&self) -> impl Iterator<Item = &str> {
        self.captures.iter().map(String::as_str)
    }

    pub fn standing(&self) -> Option<Ts<Event>> {
        self.built
            .values()
            .map(|built| built.as_ref().map(|built| *built.to()))
            .min()
            .flatten()
    }

    fn frontier(&self, table: &TableId) -> Option<Ts<Event>> {
        self.built.get(table)?.as_ref().map(|built| *built.to())
    }

    pub fn step(
        &self,
        base: Option<&Progress>,
        taken: &[Window],
        next: impl Fn(Option<&Window>, Option<Ts<Event>>) -> Option<Window>,
    ) -> Option<Step> {
        let untaken = |next: &Window| !taken.contains(next);
        let tables: BTreeSet<_> = self
            .built
            .keys()
            .chain(base.into_iter().flat_map(|base| base.built.keys()))
            .collect();
        let nexts: BTreeMap<_, _> = tables
            .into_iter()
            .map(|table| {
                let built = self.built.get(table).and_then(Option::as_ref);
                let base = base.and_then(|base| base.frontier(table));

                (table, next(built, base).filter(untaken))
            })
            .collect();
        let window = match nexts.is_empty() {
            true => next(None, None).filter(untaken)?,
            false => nexts
                .values()
                .flatten()
                .min_by_key(|next| (*next.from(), *next.to()))?
                .clone(),
        };

        Some(Step {
            left_out: nexts
                .into_iter()
                .filter(|(_, next)| next.as_ref() != Some(&window))
                .map(|(table, _)| table.clone())
                .collect(),
            window,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    window: Window,
    left_out: BTreeSet<TableId>,
}

impl Step {
    pub fn window(&self) -> &Window {
        &self.window
    }

    pub fn left_out(&self) -> &BTreeSet<TableId> {
        &self.left_out
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use ulid::Ulid;

    use crate::build_log::{Outcome, Run, Series, Status};
    use crate::sink::TablePath;

    use super::*;

    const CAPTURE: &str = r#"{"debezium":{"dialect":"mysql"}}"#;
    const DAY: i64 = 86_400_000;

    fn day(from: i64) -> Window {
        Window::new(
            Ts::try_from(from * DAY).unwrap(),
            Ts::try_from((from + 1) * DAY).unwrap(),
        )
    }

    fn logged(
        run: u64,
        series: &Series,
        table: Option<&str>,
        window: Window,
        status: Status,
    ) -> BuildLog {
        BuildLog::new(
            &Run::new(
                Ulid::from_parts(run, 0),
                Ts::from(DateTime::UNIX_EPOCH),
                series.artifact(),
                series.schedule(),
                "primary",
                CAPTURE,
                "0.1.0",
            ),
            Some(TableId::new("shop", table.unwrap_or("pets"))),
            table.map(|table| TableId::new("shop", table)),
            None,
            Some(TablePath::new("mysql_shop", "people")),
            window,
            Outcome::new(status, Ts::from(DateTime::UNIX_EPOCH)),
        )
    }

    fn daily() -> Series {
        Series::new("snapshot", "daily")
    }

    fn people() -> TableId {
        TableId::new("shop", "people")
    }

    fn pets() -> TableId {
        TableId::new("shop", "pets")
    }

    fn progress(logs: &[BuildLog]) -> Progress {
        let succeeded: Vec<_> = logs
            .iter()
            .filter(|log| !log.status().failed())
            .cloned()
            .collect();

        Progress::read(logs, &succeeded)
    }

    fn following(built: Option<&Window>, _: Option<Ts<Event>>) -> Option<Window> {
        Some(match built {
            Some(built) => day(built.to().milliseconds_since_epoch() / DAY),
            None => Window::new(Ts::try_from(0).unwrap(), Ts::try_from(3 * DAY).unwrap()),
        })
    }

    #[test]
    fn a_series_never_built_steps_every_table_into_its_first_window() {
        assert_eq!(
            Progress::default().step(None, &[], following),
            Some(Step {
                window: Window::new(Ts::try_from(0).unwrap(), Ts::try_from(3 * DAY).unwrap()),
                left_out: BTreeSet::new(),
            })
        );
    }

    #[test]
    fn a_step_takes_the_earliest_next_window_and_leaves_out_the_tables_it_is_not_next_for() {
        let progress = progress(&[
            logged(1, &daily(), Some("people"), day(3), Status::Succeeded),
            logged(1, &daily(), Some("pets"), day(1), Status::Succeeded),
        ]);

        assert_eq!(
            progress.step(None, &[], following),
            Some(Step {
                window: day(2),
                left_out: BTreeSet::from([people()]),
            })
        );
    }

    #[test]
    fn a_table_failing_stays_where_it_was_last_built() {
        let progress = progress(&[
            logged(1, &daily(), Some("people"), day(1), Status::Succeeded),
            logged(
                2,
                &daily(),
                Some("people"),
                day(2),
                Status::Failed("broken".to_owned()),
            ),
        ]);

        assert_eq!(
            progress.step(None, &[], following).unwrap().window(),
            &day(2)
        );
    }

    #[test]
    fn a_table_never_built_starts_from_its_first_window() {
        let progress = progress(&[logged(
            1,
            &daily(),
            Some("people"),
            day(1),
            Status::Failed("baseless".to_owned()),
        )]);

        assert_eq!(
            progress.step(None, &[], following).unwrap().window(),
            &Window::new(Ts::try_from(0).unwrap(), Ts::try_from(3 * DAY).unwrap())
        );
    }

    #[test]
    fn a_window_already_taken_leaves_out_the_tables_it_was_next_for() {
        let progress = progress(&[
            logged(1, &daily(), Some("people"), day(3), Status::Succeeded),
            logged(1, &daily(), Some("pets"), day(1), Status::Succeeded),
        ]);

        assert_eq!(
            progress.step(None, &[day(2)], following),
            Some(Step {
                window: day(4),
                left_out: BTreeSet::from([pets()]),
            })
        );
        assert_eq!(progress.step(None, &[day(2), day(4)], following), None);
    }

    #[test]
    fn a_table_dropped_is_followed_no_more() {
        let progress = progress(&[
            logged(1, &daily(), Some("people"), day(3), Status::Succeeded),
            logged(1, &daily(), None, day(1), Status::Succeeded),
        ]);

        assert_eq!(
            progress.step(None, &[], following).unwrap().window(),
            &day(4)
        );
    }

    #[test]
    fn a_series_built_from_another_steps_the_tables_built_into_that_one() {
        let base = progress(&[logged(
            1,
            &daily(),
            Some("people"),
            day(3),
            Status::Succeeded,
        )]);
        let cut = |built: Option<&Window>, base: Option<Ts<Event>>| match built {
            Some(_) => None,
            None => base.map(Window::until),
        };

        assert_eq!(
            Progress::default()
                .step(Some(&base), &[], cut)
                .unwrap()
                .window(),
            &Window::until(*day(3).to())
        );
    }

    #[test]
    fn a_series_stands_where_its_least_built_table_stands() {
        let progress = progress(&[
            logged(1, &daily(), Some("people"), day(3), Status::Succeeded),
            logged(1, &daily(), Some("pets"), day(1), Status::Succeeded),
        ]);

        assert_eq!(progress.standing(), Some(*day(1).to()));
        assert_eq!(progress.captures().collect::<Vec<_>>(), [CAPTURE]);
    }
}
