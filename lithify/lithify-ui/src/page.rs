use leptos::prelude::*;

use lithify_core::build_log::BuildLog;

use crate::command::BuildCommand;
use crate::overview::{
    PipelineLocation, PipelineOverview, SinkInspection, SinkOverview, SourceOverview, TableRow,
};

pub enum Page {
    Pipelines {
        pipelines: Vec<PipelineLocation>,
    },
    Pipeline {
        pipelines: Vec<PipelineLocation>,
        pipeline: PipelineOverview,
    },
    Missing {
        pipelines: Vec<PipelineLocation>,
        name: String,
    },
    Unsurveyed {
        cause: String,
    },
    Checks,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Section {
    BuildLog,
    Checks,
}

impl Section {
    const ALL: [Self; 2] = [Self::BuildLog, Self::Checks];

    fn label(self) -> &'static str {
        match self {
            Self::BuildLog => "Build log",
            Self::Checks => "Checks",
        }
    }

    fn href(self) -> &'static str {
        match self {
            Self::BuildLog => "/",
            Self::Checks => "/checks",
        }
    }
}

impl Page {
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
        .tabs > input { display: none; }
        .tabs > label {
            display: flex; align-items: center; gap: 8px; padding: 10px 12px; cursor: pointer;
            color: var(--muted); border-bottom: 2px solid transparent; margin-bottom: -1px;
        }
        .tabs > label:hover { color: var(--text); }
        .tabs > input:checked + label { color: var(--accent); border-bottom-color: var(--accent); font-weight: 600; }
        .tabs > .panel {
            display: none; order: 1; width: calc(100% + 24px); margin: 0 -12px;
            padding: 16px 20px; border-top: 1px solid var(--border);
        }
        .tabs > input:checked + label + .panel { display: block; }
        .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
        .dot.ok { background: var(--ok); } .dot.bad { background: var(--bad); } .dot.warn { background: var(--warn); }

        .summary { display: flex; gap: 8px; margin-bottom: 14px; flex-wrap: wrap; }
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

        pre.command {
            margin: 0; padding: 12px 14px; border-radius: 8px; overflow-x: auto;
            background: var(--surface-muted); border: 1px solid var(--border);
        }
    "#;

    pub fn render(self) -> String {
        let (title, section, body) = match self {
            Self::Pipelines { pipelines } => (
                Section::BuildLog.label().to_owned(),
                Section::BuildLog,
                Self::with_sidebar(&pipelines, None, Self::pipelines(&pipelines).into_any())
                    .into_any(),
            ),
            Self::Pipeline {
                pipelines,
                pipeline,
            } => (
                pipeline.location().name().to_owned(),
                Section::BuildLog,
                Self::with_sidebar(
                    &pipelines,
                    Some(pipeline.location().name()),
                    Self::pipeline(&pipeline).into_any(),
                ),
            ),
            Self::Missing { pipelines, name } => (
                name.clone(),
                Section::BuildLog,
                Self::with_sidebar(&pipelines, None, Self::missing(&name).into_any()),
            ),
            Self::Unsurveyed { cause } => (
                "Unsurveyed".to_owned(),
                Section::BuildLog,
                view! { <main><div class="callout error">{cause}</div></main> }.into_any(),
            ),
            Self::Checks => (
                Section::Checks.label().to_owned(),
                Section::Checks,
                view! { <main>{Self::checks()}</main> }.into_any(),
            ),
        };
        let document = view! {
            <html lang="en">
                <head>
                    <meta charset="utf-8" />
                    <meta name="viewport" content="width=device-width, initial-scale=1" />
                    <title>{format!("{title} · lithify")}</title>
                    <style>{Self::STYLE}</style>
                </head>
                <body>
                    {Self::topbar(section)}
                    <div class="layout">{body}</div>
                </body>
            </html>
        };

        format!("<!DOCTYPE html>{}", document.to_html())
    }

    fn topbar(current: Section) -> impl IntoView {
        let links = Section::ALL
            .into_iter()
            .map(|section| {
                let class = match section == current {
                    true => "current",
                    false => "",
                };

                view! { <a class=class href=section.href()>{section.label()}</a> }
            })
            .collect_view();

        view! {
            <header class="topbar">
                <a class="brand" href="/">"lithify"</a>
                <nav>{links}</nav>
            </header>
        }
    }

    fn checks() -> impl IntoView {
        view! {
            <div class="crumbs"><span>{Section::Checks.label()}</span></div>
            <h1>{Section::Checks.label()}</h1>
            <div class="callout empty">
                "TODO: missing shards, reconciliation against the source"
            </div>
        }
    }

    fn with_sidebar(
        pipelines: &[PipelineLocation],
        current: Option<&str>,
        main: AnyView,
    ) -> AnyView {
        let links = pipelines
            .iter()
            .map(|pipeline| {
                let class = match current == Some(pipeline.name()) {
                    true => "current",
                    false => "",
                };

                view! { <a class=class href=pipeline.href()>{pipeline.name().to_owned()}</a> }
            })
            .collect_view();

        view! {
            <aside class="sidebar">
                <h4>"Pipelines"</h4>
                {links}
            </aside>
            <main>{main}</main>
        }
        .into_any()
    }

    fn pipelines(pipelines: &[PipelineLocation]) -> impl IntoView + use<> {
        let cards = pipelines
            .iter()
            .map(|pipeline| {
                view! {
                    <a class="card" href=pipeline.href()>
                        <span class="kind">"Pipeline"</span>
                        <h2>{pipeline.name().to_owned()}</h2>
                        <div class="mono subtitle">{pipeline.directory().to_owned()}</div>
                    </a>
                }
            })
            .collect_view();

        view! {
            <div class="crumbs"><span>{Section::BuildLog.label()}</span></div>
            <h1>{Section::BuildLog.label()}</h1>
            <div class="grid">{cards}</div>
        }
    }

    fn missing(name: &str) -> impl IntoView + use<> {
        view! {
            <div class="crumbs"><span><a href="/">{Section::BuildLog.label()}</a></span><span>{name.to_owned()}</span></div>
            <h1>{name.to_owned()}</h1>
            <div class="callout error">{format!("No pipeline is named {name}")}</div>
        }
    }

    fn pipeline(pipeline: &PipelineOverview) -> impl IntoView + use<> {
        let location = pipeline.location();
        let sources = match pipeline.sources() {
            Ok(sources) => sources
                .iter()
                .enumerate()
                .map(|(at, source)| Self::source(at, location.directory(), source))
                .collect_view()
                .into_any(),
            Err(cause) => view! {
                <div class="callout error">{format!("The pipeline could not be read: {cause}")}</div>
            }
            .into_any(),
        };

        view! {
            <div class="crumbs">
                <span><a href="/">{Section::BuildLog.label()}</a></span>
                <span>{location.name().to_owned()}</span>
            </div>
            <h1>{location.name().to_owned()}</h1>
            <p class="subtitle mono">{location.directory().to_owned()}</p>
            {sources}
        }
    }

    fn source(at: usize, directory: &str, source: &SourceOverview) -> impl IntoView + use<> {
        let tabs = source
            .sinks()
            .iter()
            .enumerate()
            .map(|(tab, sink)| Self::tab(at, tab, directory, sink))
            .collect_view();

        view! {
            <section class="card">
                <div class="card-head">
                    <span class="kind">"Source"</span>
                    <h2>{source.name().to_owned()}</h2>
                </div>
                <div class="tabs">{tabs}</div>
            </section>
        }
    }

    fn tab(
        source: usize,
        at: usize,
        directory: &str,
        sink: &SinkOverview,
    ) -> impl IntoView + use<> {
        let id = format!("{source}-{at}");
        let health = match sink.inspection() {
            Err(_) => "dot bad",
            Ok(inspection) if inspection.rows().iter().any(|row| row.status.failed()) => "dot bad",
            Ok(inspection) if !inspection.discrepancies().is_empty() => "dot warn",
            Ok(_) => "dot ok",
        };

        view! {
            <input type="radio" name=format!("sinks-{source}") id=format!("sink-{id}") checked=at == 0 />
            <label for=format!("sink-{id}")>
                <span class=health></span>
                <span class="kind">"Sink"</span>
                {sink.name().to_owned()}
            </label>
            <div class="panel">{Self::panel(&id, directory, sink)}</div>
        }
    }

    fn panel(id: &str, directory: &str, sink: &SinkOverview) -> AnyView {
        match sink.inspection() {
            Ok(inspection) => Self::inspected(id, directory, sink.name(), inspection).into_any(),
            Err(cause) => view! {
                <div class="callout error">{format!("{} could not be read: {cause}", sink.name())}</div>
            }
            .into_any(),
        }
    }

    fn inspected(
        id: &str,
        directory: &str,
        sink: &str,
        inspection: &SinkInspection,
    ) -> impl IntoView + use<> {
        let rows = inspection.rows();
        let failed = rows.iter().filter(|row| row.status.failed()).count();
        let discrepancies = match inspection.discrepancies() {
            [] => ().into_any(),
            discrepancies => {
                let found = discrepancies
                    .iter()
                    .map(|discrepancy| view! { <div class="callout warn">{discrepancy.clone()}</div> })
                    .collect_view();

                view! { <h3>"Discrepancies"</h3>{found} }.into_any()
            }
        };

        view! {
            <div class="summary">
                <span class="chip">{format!("{} tables", rows.len())}</span>
                <span class="chip">{format!("{failed} failed")}</span>
                <span class="chip">{format!("{} discrepancies", inspection.discrepancies().len())}</span>
            </div>
            {Self::build_log(id, directory, sink, &rows)}
            {discrepancies}
        }
    }

    fn build_log(id: &str, directory: &str, sink: &str, rows: &[TableRow]) -> AnyView {
        if rows.is_empty() {
            return view! {
                <div class="callout empty">{format!("No build log in {sink} yet")}</div>
            }
            .into_any();
        }

        let headers = BuildLog::COLUMNS
            .iter()
            .map(|(name, _)| view! { <th>{*name}</th> })
            .collect_view();
        let lines = rows
            .iter()
            .enumerate()
            .map(|(at, row)| Self::row(&format!("row-{id}-{}", at + 1), at + 1, row))
            .collect_view();
        let panes = rows
            .iter()
            .enumerate()
            .map(|(at, row)| {
                Self::pane(
                    &format!("row-{id}-{}", at + 1),
                    at + 1,
                    row,
                    row.rebuild(directory, sink),
                )
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

    fn row(id: &str, number: usize, row: &TableRow) -> impl IntoView + use<> {
        let cells = BuildLog::COLUMNS
            .iter()
            .zip(&row.values)
            .map(|((name, _), value)| Self::cell(name, value.as_deref()))
            .collect_view();

        view! {
            <tr>
                <td class="number"><a href=format!("#{id}")>{number}</a></td>
                {cells}
            </tr>
        }
    }

    fn cell(column: &str, value: Option<&str>) -> AnyView {
        match (column, value) {
            (_, None) => view! { <td class="null">"NULL"</td> }.into_any(),
            ("status", Some(status)) => {
                view! { <td><span class=format!("badge {status}")>{status.to_owned()}</span></td> }
                    .into_any()
            }
            (_, Some(value)) => view! { <td class="mono">{value.to_owned()}</td> }.into_any(),
        }
    }

    fn pane(
        id: &str,
        number: usize,
        row: &TableRow,
        rebuild: Option<BuildCommand>,
    ) -> impl IntoView + use<> {
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
            <aside class="pane" id=id.to_owned()>
                <div class="pane-head">
                    <span class="kind">{format!("Row {number}")}</span>
                    <h2 class="mono">{row.table.to_string()}</h2>
                    <a class="close" href="#closed" title="Close">"✕"</a>
                </div>
                <h3>"Build log"</h3>
                <dl class="fields">{fields}</dl>
                <h3>"Derived"</h3>
                <dl class="fields">
                    <dt>"shard"</dt>
                    <dd class="mono">{row.shard.map(|shard| shard.to_string()).unwrap_or_default()}</dd>
                    <dt>"rebuild"</dt>
                    <dd><pre class="command"><code>{rebuild.map(|command| command.to_string()).unwrap_or_default()}</code></pre></dd>
                </dl>
            </aside>
            <a class="backdrop" href="#closed" title="Close"></a>
        }
    }
}
