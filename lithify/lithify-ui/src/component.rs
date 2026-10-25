use chrono::DateTime;
use chrono_tz::Tz;
use leptos::prelude::*;

use lithify_core::build_log::{BuildLog, Status};
use lithify_core::model::TableId;
use lithify_core::schedule::Schedule;

use crate::pipelines::{
    BuildLogView, PipelineLocation, PipelineOverview, SinkInspection, SinkOverview, SourceOverview,
};
use crate::route::{AppRoute, Section};

const STYLE: &str = r#"
        :root {
            --bg: #f5f6f8; --surface: #ffffff; --surface-muted: #f9fafb;
            --border: #e2e5ea; --text: #1f2430; --muted: #6b7280;
            --accent: #3b5bdb; --accent-soft: #edf1fd;
            --ok: #1f8a4c; --ok-soft: #e6f4ec;
            --bad: #c92a2a; --bad-soft: #fdecec;
            --warn: #b7791f; --warn-soft: #fdf6e7;
            --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        }
        @media (prefers-color-scheme: dark) {
            :root {
                --bg: #12141a; --surface: #1b1e26; --surface-muted: #20242e;
                --border: #2e333f; --text: #e6e8ee; --muted: #9aa1ad;
                --accent: #7c9cff; --accent-soft: #232b45;
                --ok: #5cc98a; --ok-soft: #173326;
                --bad: #ff7b7b; --bad-soft: #3a1c1f;
                --warn: #e6b35a; --warn-soft: #3a2f17;
            }
        }
        * { box-sizing: border-box; }
        body {
            margin: 0; background: var(--bg); color: var(--text);
            font: 14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
        }
        a { color: var(--accent); text-decoration: none; }
        a:hover { text-decoration: underline; }
        code, .mono { font-family: var(--mono); font-size: 12.5px; }

        .topbar {
            display: flex; align-items: center; gap: 24px; height: 52px; padding: 0 24px;
            background: var(--surface); border-bottom: 1px solid var(--border);
        }
        .brand { font-weight: 700; font-size: 16px; color: var(--text); letter-spacing: 0.02em; }
        .topbar nav { display: flex; gap: 4px; }
        .topbar nav a { padding: 6px 12px; border-radius: 6px; color: var(--muted); }
        .topbar nav a.current { background: var(--accent-soft); color: var(--accent); font-weight: 600; }
        .topbar nav a:hover { text-decoration: none; color: var(--text); }

        .layout { display: flex; min-height: calc(100vh - 52px); }
        .sidebar {
            width: 220px; flex-shrink: 0; padding: 20px 12px;
            background: var(--surface); border-right: 1px solid var(--border);
        }
        .sidebar h4 {
            margin: 0 8px 8px; font-size: 11px; text-transform: uppercase;
            letter-spacing: 0.08em; color: var(--muted);
        }
        .sidebar a { display: block; padding: 6px 10px; border-radius: 6px; color: var(--text); }
        .sidebar a.current { background: var(--accent-soft); color: var(--accent); font-weight: 600; }
        .sidebar a:hover { text-decoration: none; background: var(--surface-muted); }
        main { flex: 1; min-width: 0; padding: 24px 32px 48px; }

        .crumbs { color: var(--muted); font-size: 13px; margin-bottom: 4px; }
        .crumbs span + span::before { content: "/"; margin: 0 8px; color: var(--border); }
        h1 { margin: 0 0 4px; font-size: 22px; }
        .subtitle { color: var(--muted); margin: 0 0 24px; }

        .card {
            background: var(--surface); border: 1px solid var(--border);
            border-radius: 10px; margin-bottom: 20px; overflow: hidden;
        }
        .card-head {
            display: flex; align-items: baseline; gap: 10px; padding: 14px 20px;
            border-bottom: 1px solid var(--border);
        }
        .card-head h2 { margin: 0; font-size: 16px; }
        .kind {
            font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em;
            color: var(--muted); font-weight: 600;
        }
        .card-body { padding: 16px 20px; }

        .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 16px; }
        .grid .card { margin: 0; }
        .grid a.card { display: block; padding: 18px 20px; color: var(--text); }
        .grid a.card:hover { text-decoration: none; border-color: var(--accent); }
        .grid a.card h2 { margin: 0 0 4px; font-size: 16px; }

        .tabs { display: flex; flex-wrap: wrap; padding: 0 12px; border-bottom: 1px solid var(--border); }
        .tabs > a {
            display: flex; align-items: center; gap: 8px; padding: 10px 12px;
            color: var(--muted); border-bottom: 2px solid transparent; margin-bottom: -1px;
        }
        .tabs > a:hover { color: var(--text); text-decoration: none; }
        .tabs > a.current { color: var(--accent); border-bottom-color: var(--accent); font-weight: 600; }
        .card > .panel { padding: 16px 20px; }
        .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
        .dot.ok { background: var(--ok); } .dot.bad { background: var(--bad); } .dot.warn { background: var(--warn); }

        .toggle { display: inline-flex; margin: 0 0 16px; border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
        .toggle a { padding: 4px 14px; color: var(--muted); background: var(--surface); }
        .toggle a + a { border-left: 1px solid var(--border); }
        .toggle a.current { background: var(--accent-soft); color: var(--accent); font-weight: 600; }
        .toggle a:hover { text-decoration: none; color: var(--text); }
        .summary { display: flex; gap: 8px; margin-bottom: 14px; flex-wrap: wrap; }
        a.chip:hover { text-decoration: none; border-color: var(--accent); color: var(--accent); }
        .chip {
            padding: 2px 10px; border-radius: 999px; font-size: 12px;
            background: var(--surface-muted); border: 1px solid var(--border); color: var(--muted);
        }

        .table-wrap { overflow-x: auto; border: 1px solid var(--border); border-radius: 8px; }
        table { width: 100%; border-collapse: collapse; }
        th, td {
            padding: 8px 12px; text-align: left; vertical-align: top; white-space: nowrap;
            border-bottom: 1px solid var(--border);
        }
        td.null, dd.null { color: var(--muted); font-style: italic; }
        th.number, td.number { position: sticky; left: 0; background: var(--surface-muted); text-align: right; }
        td.number a { display: block; font-weight: 600; }

        .pane {
            display: none; position: fixed; top: 52px; right: 0; bottom: 0; width: min(480px, 100vw);
            overflow-y: auto; padding: 0 20px 24px; z-index: 10;
            background: var(--surface); border-left: 1px solid var(--border);
            box-shadow: -8px 0 24px rgba(0, 0, 0, 0.12);
        }
        .pane:target { display: block; }
        .backdrop { display: none; position: fixed; inset: 52px 0 0 0; z-index: 9; background: rgba(0, 0, 0, 0.08); }
        .pane:target + .backdrop { display: block; }
        .pane-head {
            position: sticky; top: 0; display: flex; align-items: baseline; gap: 10px;
            padding: 16px 0 12px; background: var(--surface); border-bottom: 1px solid var(--border);
        }
        .pane-head h2 { margin: 0; font-size: 15px; flex: 1; }
        .pane-head .close { color: var(--muted); font-size: 16px; }
        dl.fields { display: grid; grid-template-columns: max-content 1fr; gap: 6px 14px; margin: 0; }
        dl.fields dt { color: var(--muted); font-size: 12px; }
        dl.fields dd { margin: 0; overflow-wrap: anywhere; }
        th + th, td + td { border-left: 1px solid var(--border); }
        th {
            background: var(--surface-muted); font-size: 12px; font-weight: 600;
            color: var(--muted); white-space: nowrap;
        }
        tr:last-child td { border-bottom: none; }
        tbody tr:hover td { background: var(--surface-muted); }

        .badge { display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 12px; font-weight: 600; }
        .badge.succeeded { background: var(--ok-soft); color: var(--ok); }
        .badge.failed { background: var(--bad-soft); color: var(--bad); }

        .callout { padding: 10px 14px; border-radius: 8px; border: 1px solid; margin: 0 0 8px; }
        .callout.error { background: var(--bad-soft); border-color: var(--bad); color: var(--bad); }
        .callout.warn { background: var(--warn-soft); border-color: var(--warn); color: var(--warn); }
        .callout.empty { background: var(--surface-muted); border-color: var(--border); color: var(--muted); }
        h3 { font-size: 13px; margin: 18px 0 8px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; }

    "#;

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

    use lithify_core::build_log::{Outcome, Run};
    use lithify_core::sink::TablePath;
    use lithify_core::ts::Ts;
    use lithify_core::window::Window;

    use super::*;
    use crate::testing::Kept;

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
        let renamed = BuildLog::new(
            &Run::new(
                Ulid::from_parts(1, 0),
                Ts::from(DateTime::UNIX_EPOCH),
                "snapshot",
                "daily",
                "primary",
                "{}",
                "0.1.0",
            ),
            Some(TableId::new("shop", "people")),
            Some(TableId::new("shop", "persons")),
            None,
            Some(TablePath::new("mysql_shop", "persons_20260920")),
            Window::new(
                Ts::from(at("2026-09-20T00:00:00Z")),
                Ts::from(at("2026-09-21T00:00:00Z")),
            ),
            Outcome::new(Status::Succeeded, Ts::from(at("2026-09-21T00:10:00Z"))),
        );
        let rows = listed(vec![renamed]);

        assert_eq!(rows[0].table, TableId::new("shop", "persons"));
        assert_eq!(rows[0].value("lifecycle"), Some("renamed"));
    }
}
