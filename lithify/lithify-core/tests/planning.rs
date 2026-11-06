use std::collections::BTreeSet;

use proptest::prelude::*;

use lithify_core::artifact::UnbuildableArtifact;
use lithify_core::build::{Base, BuildContext, TableBuild};
use lithify_core::event::{SchemaChange, TableChange};
use lithify_core::model::{ColumnModel, ColumnType, TableId, TableModel};
use lithify_core::runner::Replayer;
use lithify_core::sink::{Namespace, TablePath};
use lithify_core::source::SchemaHistory;
use lithify_core::ts::{Event, Ts};
use lithify_core::window::Window;
use lithify_testing::artifact::Mirror;
use lithify_testing::{ColumnDdl, Engine, Log, Offset, Replica, Untold};

fn at(millis: i64) -> Ts<Event> {
    Ts::try_from(millis).unwrap()
}

fn window() -> Window {
    Window::new(at(100), at(200))
}

fn table(name: &str) -> TableId {
    TableId::new("shop", name)
}

fn keyed(name: &str) -> TableModel {
    TableModel::new(
        table(name),
        vec![ColumnModel::new(
            "id",
            "BIGINT",
            ColumnType::Int64,
            false,
            None,
        )],
        ["id"].into(),
    )
}

fn built_before(table: &TableId) -> TablePath {
    TablePath::new("shelf", &format!("{}_before", table.table()))
}

fn planned_with(
    history: &SchemaHistory<Offset>,
    base: impl Fn(&TableId) -> Option<TablePath>,
    finishes: impl Fn(&TableId) -> bool,
) -> Vec<TableBuild> {
    let window = window();
    let engine = Engine::default();
    let namespace = Namespace::from("shelf");

    Replayer::new(BuildContext::new(&engine, &namespace), &Log)
        .planned::<_, Untold>(history, &Mirror, &window, base, finishes)
        .unwrap()
}

fn planned(history: &SchemaHistory<Offset>) -> Vec<TableBuild> {
    planned_with(history, |table| Some(built_before(table)), |_| true)
}

fn of<'b>(builds: &'b [TableBuild], name: &str) -> &'b TableBuild {
    builds
        .iter()
        .find(|build| build.table_to().or(build.table_from()) == Some(&table(name)))
        .unwrap()
}

fn opened_with_people() -> Replica {
    Replica::default().create(at(0), keyed("people"))
}

fn change(millis: i64, table: TableChange) -> SchemaChange<Offset> {
    SchemaChange::new(
        Offset(millis as u64),
        Ts::try_from(millis).unwrap(),
        "",
        vec![table],
        vec![],
    )
}

#[test]
fn a_table_the_window_opened_with_is_planned_onto_what_its_name_was_built_into() {
    let builds = planned(&opened_with_people().history(&window()));

    assert_eq!(
        builds,
        [TableBuild::new(
            Some(table("people")),
            Some(table("people")),
            Base::Built(built_before(&table("people"))),
            Some(TablePath::new("shelf", "people")),
            window(),
            Ok(vec![
                "CREATE SCHEMA IF NOT EXISTS \"shelf\"".into(),
                "shop.people onto shelf.people_before into shelf.people".into()
            ]),
        )]
    );
}

#[test]
fn a_table_renamed_within_the_window_is_planned_from_its_first_name_onto_what_that_name_was_built_into()
 {
    let builds = planned(
        &opened_with_people()
            .rename(at(150), &table("people"), &table("persons"))
            .history(&window()),
    );
    let renamed = of(&builds, "persons");

    assert_eq!(renamed.table_from(), Some(&table("people")));
    assert_eq!(renamed.base(), &Base::Built(built_before(&table("people"))));
    assert_eq!(renamed.target(), Some(&TablePath::new("shelf", "persons")));
}

#[test]
fn a_table_created_within_the_window_is_planned_from_nothing_onto_the_change_log() {
    let builds = planned(
        &opened_with_people()
            .create(at(150), keyed("pets"))
            .history(&window()),
    );
    let created = of(&builds, "pets");

    assert_eq!(created.table_from(), None);
    assert_eq!(created.base(), &Base::ChangeLog);
    assert_eq!(created.refusal(), None);
}

#[test]
fn a_table_captured_within_the_window_is_planned_from_its_name_onto_the_change_log() {
    let history = SchemaHistory::new(
        vec![],
        vec![change(150, TableChange::Created(keyed("pets"))).captured()],
    );

    let builds = planned(&history);

    assert_eq!(builds[0].table_from(), Some(&table("pets")));
    assert_eq!(builds[0].base(), &Base::ChangeLog);
}

#[test]
fn a_table_first_seen_altered_within_the_window_is_refused_without_what_its_name_was_built_into() {
    let history = SchemaHistory::new(
        vec![],
        vec![change(150, TableChange::Altered(keyed("pets")))],
    );

    let builds = planned_with(&history, |_| None, |_| true);

    assert_eq!(builds[0].base(), &Base::Missing);
    assert_eq!(
        builds[0].refusal(),
        Some(UnbuildableArtifact::WithoutBase(table("pets")))
    );
}

#[test]
fn a_table_dropped_within_the_window_is_planned_into_nothing_and_builds_nothing() {
    let builds = planned(
        &opened_with_people()
            .drop(at(150), &table("people"))
            .history(&window()),
    );

    assert_eq!(builds[0].table_from(), Some(&table("people")));
    assert_eq!(builds[0].table_to(), None);
    assert_eq!(builds[0].target(), None);
    assert_eq!(builds[0].statements(), Ok(&[][..]));
}

#[test]
fn a_table_renamed_and_then_dropped_within_the_window_is_planned_from_its_first_name() {
    let builds = planned(
        &opened_with_people()
            .rename(at(150), &table("people"), &table("persons"))
            .drop(at(160), &table("persons"))
            .history(&window()),
    );

    assert_eq!(builds.len(), 1);
    assert_eq!(builds[0].table_from(), Some(&table("people")));
    assert_eq!(builds[0].table_to(), None);
}

#[test]
fn a_table_the_planning_is_told_to_leave_out_is_not_planned() {
    let builds = planned_with(
        &opened_with_people()
            .create(at(50), keyed("pets"))
            .history(&window()),
        |table| Some(built_before(table)),
        |table| table == &self::table("pets"),
    );

    assert_eq!(builds.len(), 1);
    assert_eq!(builds[0].table_to(), Some(&table("pets")));
}

#[derive(Debug, Clone)]
enum Statement {
    Create(usize),
    Drop(usize),
    Rename(usize, usize),
    Add(usize),
}

const NAMES: [&str; 4] = ["people", "pets", "orders", "items"];

fn statement() -> impl Strategy<Value = Statement> {
    prop_oneof![
        (0..NAMES.len()).prop_map(Statement::Create),
        (0..NAMES.len()).prop_map(Statement::Drop),
        (0..NAMES.len(), 0..NAMES.len()).prop_map(|(from, to)| Statement::Rename(from, to)),
        (0..NAMES.len()).prop_map(Statement::Add),
    ]
}

// Runs the statements a source could have run in order, leaving out the ones it would have
// rejected, such as dropping a table it does not hold.
struct Source {
    replica: Replica,
    standing: BTreeSet<&'static str>,
    columns: usize,
}

impl Source {
    fn new() -> Self {
        Self {
            replica: Replica::default(),
            standing: BTreeSet::new(),
            columns: 0,
        }
    }

    fn ran(mut self, at: i64, statement: &Statement) -> Self {
        let at = self::at(at);
        let replica = std::mem::take(&mut self.replica);

        self.replica = match *statement {
            Statement::Create(name) if self.standing.insert(NAMES[name]) => {
                replica.create(at, keyed(NAMES[name]))
            }
            Statement::Drop(name) if self.standing.remove(NAMES[name]) => {
                replica.drop(at, &table(NAMES[name]))
            }
            Statement::Rename(from, to)
                if self.standing.contains(NAMES[from]) && !self.standing.contains(NAMES[to]) =>
            {
                self.standing.remove(NAMES[from]);
                self.standing.insert(NAMES[to]);
                replica.rename(at, &table(NAMES[from]), &table(NAMES[to]))
            }
            Statement::Add(name) if self.standing.contains(NAMES[name]) => {
                self.columns += 1;
                replica.alter(
                    at,
                    &table(NAMES[name]),
                    vec![ColumnDdl::add(ColumnModel::new(
                        &format!("column_{}", self.columns),
                        "BIGINT",
                        ColumnType::Int64,
                        true,
                        None,
                    ))],
                )
            }
            _ => replica,
        };

        self
    }
}

proptest! {
    #[test]
    fn every_table_is_planned_to_pass_through_the_window_as_the_source_passed_it(
        before in prop::collection::vec(statement(), 0..8),
        within in prop::collection::vec(statement(), 0..8),
    ) {
        let source = before
            .iter()
            .enumerate()
            .fold(Source::new(), |source, (at, statement)| source.ran(at as i64, statement));
        let source = within
            .iter()
            .enumerate()
            .fold(source, |source, (at, statement)| source.ran(100 + at as i64, statement));
        let passages: BTreeSet<_> = source
            .replica
            .passages(&window())
            .into_iter()
            .filter(|(from, to)| from.is_some() || to.is_some())
            .collect();

        let builds = planned(&source.replica.history(&window()));

        prop_assert_eq!(
            builds
                .iter()
                .map(|build| (build.table_from().cloned(), build.table_to().cloned()))
                .collect::<BTreeSet<_>>(),
            passages
        );
        for build in &builds {
            prop_assert_eq!(
                build.base(),
                &match build.table_from() {
                    Some(from) => Base::Built(built_before(from)),
                    None => Base::ChangeLog,
                }
            );
            prop_assert_eq!(build.target().is_some(), build.table_to().is_some());
            prop_assert_eq!(build.refusal(), None);
        }
    }
}
