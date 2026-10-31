use chrono::DateTime;
use chrono_tz::Tz;
use leptos::prelude::*;

use lithify_core::model::TableId;
use lithify_pipeline::build_log::{BuildLog, Status};
use lithify_pipeline::schedule::Schedule;

use lithify_pipeline::pipelines::PipelineLocation;

use crate::pipelines::{
    BuildLogView, PipelineOverview, SinkInspection, SinkOverview, SourceOverview,
};
use crate::route::{AppRoute, Section};

const STYLE: &str = include_str!("../css/style.css");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub table: TableId,
    pub status: Status,
    pub values: [Option<String>; BuildLog::COLUMNS.len()],
    pub closed: DateTime<Tz>,
}

impl TableRow {
    pub fn listed(inspection: &SinkInspection) -> Vec<Self> {
        inspection
            .logs()
            .iter()
            .filter_map(|log| Self::new(log, inspection.schedule()))
            .collect()
    }

    fn new(log: &BuildLog, schedule: Schedule) -> Option<Self> {
        Some(Self {
            table: log.table()?.clone(),
            status: log.status().clone(),
            values: log.values(),
            closed: schedule.local(*log.window().to()),
        })
    }

    pub fn value(&self, column: &str) -> Option<&str> {
        BuildLog::COLUMNS
            .iter()
            .position(|(name, _)| *name == column)
            .and_then(|at| self.values[at].as_deref())
    }
}

#[derive(Debug, Clone)]
pub struct ShownSink {
    route: AppRoute,
    id: String,
    location: PipelineLocation,
    sink_name: String,
    inspection: SinkInspection,
}

impl Section {
    fn health(self, inspection: &SinkInspection) -> &'static str {
        match self {
            Self::BuildLog if inspection.logs().iter().any(|log| log.status().failed()) => {
                "dot bad"
            }
            Self::Checks if !inspection.discrepancies().is_empty() => "dot warn",
            Self::BuildLog | Self::Checks => "dot ok",
        }
    }
}

#[component]
pub fn Shell(route: AppRoute, title: String, children: Children) -> impl IntoView {
    view! {
        <html lang="en">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <title>{format!("{title} · lithify")}</title>
                <style>{STYLE}</style>
            </head>
            <body>
                <Topbar current=route.section() />
                <div class="layout">{children()}</div>
            </body>
        </html>
    }
}

#[component]
fn Topbar(current: Section) -> impl IntoView {
    let links = Section::ALL
        .into_iter()
        .map(|section| {
            let class = match section == current {
                true => "current",
                false => "",
            };

            view! { <a class=class href=section.index().href()>{section.label()}</a> }
        })
        .collect_view();

    view! {
        <header class="topbar">
            <a class="brand" href="/">"lithify"</a>
            <nav>{links}</nav>
        </header>
    }
}

#[component]
pub fn Sidebar(route: AppRoute, pipelines: Vec<PipelineLocation>) -> impl IntoView {
    let links = pipelines
        .iter()
        .map(|pipeline| {
            let class = match route.pipeline() == Some(pipeline.name()) {
                true => "current",
                false => "",
            };

            view! { <a class=class href=route.at(pipeline.name()).href()>{pipeline.name().to_owned()}</a> }
        })
        .collect_view();

    view! {
        <aside class="sidebar">
            <h4>"Pipelines"</h4>
            {links}
        </aside>
    }
}

#[component]
pub fn PipelineCards(route: AppRoute, pipelines: Vec<PipelineLocation>) -> impl IntoView {
    let label = route.section().label();
    let cards = pipelines
        .iter()
        .map(|pipeline| {
            view! {
                <a class="card" href=route.at(pipeline.name()).href()>
                    <span class="kind">"Pipeline"</span>
                    <h2>{pipeline.name().to_owned()}</h2>
                    <div class="mono subtitle">{pipeline.directory().to_owned()}</div>
                </a>
            }
        })
        .collect_view();

    view! {
        <div class="crumbs"><span>{label}</span></div>
        <h1>{label}</h1>
        <div class="grid">{cards}</div>
    }
}

#[component]
pub fn PipelineHeader(route: AppRoute, directory: Option<String>) -> impl IntoView {
    let section = route.section();
    let name = route.pipeline().unwrap_or_default().to_owned();

    view! {
        <div class="crumbs">
            <span><a href=section.index().href()>{section.label()}</a></span>
            <span>{name.clone()}</span>
        </div>
        <h1>{name}</h1>
        {directory.map(|directory| view! { <p class="subtitle mono">{directory}</p> })}
    }
}

#[component]
pub fn MissingPipeline(route: AppRoute) -> impl IntoView {
    let name = route.pipeline().unwrap_or_default().to_owned();

    view! {
        <PipelineHeader route=route directory=None />
        <Callout kind="error">{format!("No pipeline is named {name}")}</Callout>
    }
}

#[component]
pub fn ViewToggle(route: AppRoute) -> impl IntoView {
    let views = BuildLogView::ALL
        .into_iter()
        .map(|view| {
            let class = match view == route.view() {
                true => "current",
                false => "",
            };

            view! { <a class=class href=route.viewing(view).href()>{view.name()}</a> }
        })
        .collect_view();

    view! { <nav class="toggle">{views}</nav> }
}

#[component]
pub fn Sources(
    route: AppRoute,
    pipeline: PipelineOverview,
    panel: fn(ShownSink) -> AnyView,
) -> impl IntoView {
    match pipeline.sources() {
        Ok(sources) => sources
            .iter()
            .enumerate()
            .map(|(at, source)| {
                view! {
                    <SourceCard
                        route=route.clone()
                        at=at
                        location=pipeline.location().clone()
                        source=source.clone()
                        panel=panel
                    />
                }
            })
            .collect_view()
            .into_any(),
        Err(cause) => view! {
            <Callout kind="error">{format!("The pipeline could not be read: {cause}")}</Callout>
        }
        .into_any(),
    }
}

#[component]
fn SourceCard(
    route: AppRoute,
    at: usize,
    location: PipelineLocation,
    source: SourceOverview,
    panel: fn(ShownSink) -> AnyView,
) -> impl IntoView {
    let shown = source.shown(route.sink_name());
    let tabs = view! {
        <SinkTabs
            route=route.clone()
            sinks=source.sinks().to_vec()
            selected=shown.map(|(selected, _)| selected)
        />
    };
    let content = shown.map(|(selected, sink)| match sink.found() {
        Ok(inspection) => panel(ShownSink {
            route: route.clone(),
            id: format!("{at}-{selected}"),
            location: location.clone(),
            sink_name: sink.name().to_owned(),
            inspection: inspection.clone(),
        }),
        Err(cause) => view! {
            <Callout kind="error">{format!("{} could not be read: {cause}", sink.name())}</Callout>
        }
        .into_any(),
    });

    view! {
        <section class="card">
            <div class="card-head">
                <span class="kind">"Source"</span>
                <h2>{source.name().to_owned()}</h2>
            </div>
            <div class="tabs">{tabs}</div>
            <div class="panel">{content}</div>
        </section>
    }
}

#[component]
fn SinkTabs(route: AppRoute, sinks: Vec<SinkOverview>, selected: Option<usize>) -> impl IntoView {
    sinks
        .iter()
        .enumerate()
        .map(|(at, sink)| {
            let health = match sink.found() {
                Ok(inspection) => route.section().health(inspection),
                Err(_) => "dot bad",
            };
            let class = match Some(at) == selected {
                true => "current",
                false => "",
            };

            view! {
                <a class=class href=route.showing(sink.name()).href()>
                    <span class=health></span>
                    <span class="kind">"Sink"</span>
                    {sink.name().to_owned()}
                </a>
            }
        })
        .collect_view()
}

#[component]
pub fn BuildLogPanel(shown: ShownSink) -> impl IntoView {
    let ShownSink {
        route,
        id,
        location,
        sink_name,
        inspection,
    } = shown;
    let rows = TableRow::listed(&inspection);
    let failed = rows.iter().filter(|row| row.status.failed()).count();
    let counted = match route.view() {
        BuildLogView::Latest => format!("{} tables", rows.len()),
        BuildLogView::All => format!("{} rows", rows.len()),
    };
    let checks = Section::Checks
        .pipeline(location.name())
        .showing(&sink_name)
        .href();

    view! {
        <div class="summary">
            <span class="chip">{counted}</span>
            <span class="chip">{format!("{failed} failed")}</span>
            <a class="chip" href=checks>
                {format!("{} discrepancies", inspection.discrepancies().len())}
            </a>
        </div>
        <BuildLogTable id=id sink_name=sink_name rows=rows />
    }
}

#[component]
pub fn CheckPanel(shown: ShownSink) -> impl IntoView {
    let discrepancies = shown.inspection.discrepancies().to_vec();
    let count = discrepancies.len();
    let found = discrepancies
        .into_iter()
        .map(|discrepancy| view! { <Callout kind="warn">{discrepancy}</Callout> })
        .collect_view();

    view! {
        <div class="summary">
            <span class="chip">{format!("{count} discrepancies")}</span>
        </div>
        {found}
    }
}

#[component]
fn BuildLogTable(id: String, sink_name: String, rows: Vec<TableRow>) -> impl IntoView {
    if rows.is_empty() {
        return view! {
            <Callout kind="empty">{format!("No build log in {sink_name} yet")}</Callout>
        }
        .into_any();
    }

    let anchor = |at: usize| format!("row-{id}-{}", at + 1);
    let headers = BuildLog::COLUMNS
        .iter()
        .map(|(name, _)| view! { <th>{*name}</th> })
        .collect_view();
    let lines = rows
        .iter()
        .enumerate()
        .map(|(at, row)| view! { <RowLine id=anchor(at) number=at + 1 row=row.clone() /> })
        .collect_view();
    let panes = rows
        .iter()
        .enumerate()
        .map(|(at, row)| {
            view! { <RowPane id=anchor(at) number=at + 1 row=row.clone() /> }
        })
        .collect_view();

    view! {
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th class="number">"#"</th>
                        {headers}
                    </tr>
                </thead>
                <tbody>{lines}</tbody>
            </table>
        </div>
        {panes}
    }
    .into_any()
}

#[component]
fn RowLine(id: String, number: usize, row: TableRow) -> impl IntoView {
    let cells = BuildLog::COLUMNS
        .iter()
        .zip(row.values)
        .map(|((name, _), value)| view! { <Cell column=name value=value /> })
        .collect_view();

    view! {
        <tr>
            <td class="number"><a href=format!("#{id}")>{number}</a></td>
            {cells}
        </tr>
    }
}

#[component]
fn Cell(column: &'static str, value: Option<String>) -> impl IntoView {
    match (column, value) {
        (_, None) => view! { <td class="null">"NULL"</td> }.into_any(),
        ("status", Some(status)) => {
            view! { <td><span class=format!("badge {status}")>{status.clone()}</span></td> }
                .into_any()
        }
        (_, Some(value)) => view! { <td class="mono">{value}</td> }.into_any(),
    }
}

#[component]
fn RowPane(id: String, number: usize, row: TableRow) -> impl IntoView {
    let fields = BuildLog::COLUMNS
        .iter()
        .zip(&row.values)
        .map(|((name, _), value)| {
            let value = match value {
                Some(value) => view! { <dd class="mono">{value.clone()}</dd> }.into_any(),
                None => view! { <dd class="null">"NULL"</dd> }.into_any(),
            };

            view! { <dt>{*name}</dt>{value} }
        })
        .collect_view();

    view! {
        <aside class="pane" id=id>
            <div class="pane-head">
                <span class="kind">{format!("Row {number}")}</span>
                <h2 class="mono">{row.table.to_string()}</h2>
                <a class="close" href="#closed" title="Close">"✕"</a>
            </div>
            <h3>"Build log"</h3>
            <dl class="fields">{fields}</dl>
            <h3>"Derived"</h3>
            <dl class="fields">
                <dt>"closed"</dt>
                <dd class="mono">{row.closed.to_rfc3339()}</dd>
            </dl>
        </aside>
        <a class="backdrop" href="#closed" title="Close"></a>
    }
}

#[component]
pub fn Callout(kind: &'static str, children: Children) -> impl IntoView {
    view! { <div class=format!("callout {kind}")>{children()}</div> }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use ulid::Ulid;

    use lithify_core::sink::TablePath;
    use lithify_core::ts::Ts;
    use lithify_core::window::Window;
    use lithify_pipeline::build_log::{Run, TableBuilt};

    use super::*;
    use lithify_testing::pipeline::Kept;

    fn listed(logs: Vec<BuildLog>) -> Vec<TableRow> {
        TableRow::listed(&SinkInspection::new(Schedule::default(), logs, vec![]))
    }

    #[test]
    fn a_sink_lists_its_builds_in_the_order_they_were_read() {
        let rows = listed(Kept::logs());

        assert_eq!(
            rows.iter().map(|row| row.table.clone()).collect::<Vec<_>>(),
            [
                TableId::new("shop", "people"),
                TableId::new("shop", "people"),
                TableId::new("shop", "pets")
            ]
        );
        assert_eq!(rows[2].status, Status::Failed("no such column".to_owned()));
        assert_eq!(rows[1].closed.to_rfc3339(), "2026-09-21T00:00:00+00:00");
    }

    #[test]
    fn a_row_holds_what_each_column_of_the_build_log_holds() {
        let rows = listed(Kept::logs());

        assert_eq!(
            rows[1].value("window_from"),
            Some("2026-09-20T00:00:00+00:00")
        );
        assert_eq!(
            rows[1].value("window_to"),
            Some("2026-09-21T00:00:00+00:00")
        );
        assert_eq!(rows[2].value("base"), None);
        assert_eq!(rows[2].value("error"), Some("no such column"));
    }

    #[test]
    fn a_table_renamed_within_its_window_is_listed_under_its_new_name_as_renamed() {
        let at = |rfc3339: &str| rfc3339.parse::<DateTime<chrono::Utc>>().unwrap();
        let renamed = TableBuilt::new(
            Some(TableId::new("shop", "people")),
            Some(TableId::new("shop", "persons")),
            None,
            Some(TablePath::new("mysql_shop", "persons_20260920")),
            Window::new(
                Ts::from(at("2026-09-20T00:00:00Z")),
                Ts::from(at("2026-09-21T00:00:00Z")),
            ),
            Status::Succeeded,
            Ts::from(at("2026-09-21T00:10:00Z")),
        )
        .logged(&Run::new(
            Ulid::from_parts(1, 0),
            Ts::from(DateTime::UNIX_EPOCH),
            &"snapshot".into(),
            "daily",
            "primary",
            "{}",
            "0.1.0",
        ));
        let rows = listed(vec![renamed]);

        assert_eq!(rows[0].table, TableId::new("shop", "persons"));
        assert_eq!(rows[0].value("lifecycle"), Some("renamed"));
    }
}
