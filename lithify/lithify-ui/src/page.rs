use leptos::prelude::*;

use crate::command::{BuildCommand, BuildForm};
use crate::overview::{PipelineOverview, SinkInspection, SinkOverview, SourceOverview, TableRow};

pub enum Page {
    Overview {
        pipelines: Vec<PipelineOverview>,
    },
    Unsurveyed {
        cause: String,
    },
    Build {
        pipelines: Vec<String>,
        sinks: Vec<String>,
        form: Box<BuildForm>,
        command: Result<Option<BuildCommand>, String>,
    },
}

impl Page {
    const STYLE: &str = "
        .tabs { display: flex; flex-wrap: wrap; }
        .tabs > input { display: none; }
        .tabs > label { padding: 0.25em 0.75em; border: 1px solid #ccc; cursor: pointer; }
        .tabs > input:checked + label { font-weight: bold; background: #eee; }
        .tabs > .panel { display: none; order: 1; width: 100%; }
        .tabs > input:checked + label + .panel { display: block; }
        .error { color: #b00; }
    ";

    pub fn render(self) -> String {
        let body = match self {
            Self::Overview { pipelines } => Self::overview(pipelines).into_any(),
            Self::Unsurveyed { cause } => view! { <p class="error">{cause}</p> }.into_any(),
            Self::Build {
                pipelines,
                sinks,
                form,
                command,
            } => Self::build(pipelines, sinks, *form, command).into_any(),
        };
        let document = view! {
            <html>
                <head>
                    <meta charset="utf-8" />
                    <title>"lithify"</title>
                    <style>{Self::STYLE}</style>
                </head>
                <body>
                    <nav>
                        <a href="/">"Overview"</a>
                        " | "
                        <a href="/build">"Build"</a>
                    </nav>
                    {body}
                </body>
            </html>
        };

        format!("<!DOCTYPE html>{}", document.to_html())
    }

    fn overview(pipelines: Vec<PipelineOverview>) -> impl IntoView {
        pipelines
            .iter()
            .enumerate()
            .map(|(at, pipeline)| Self::pipeline(at, pipeline))
            .collect_view()
    }

    fn pipeline(at: usize, pipeline: &PipelineOverview) -> AnyView {
        let directory = pipeline.directory().to_owned();

        match pipeline.sources() {
            Ok(sources) => sources
                .iter()
                .enumerate()
                .map(|(within, source)| {
                    Self::source(&format!("{at}-{within}"), pipeline.directory(), source)
                })
                .collect_view()
                .into_any(),
            Err(cause) => view! {
                <section>
                    <h2>{directory}</h2>
                    <p class="error">{cause.to_owned()}</p>
                </section>
            }
            .into_any(),
        }
    }

    fn source(id: &str, directory: &str, source: &SourceOverview) -> impl IntoView + use<> {
        let tabs = source
            .sinks()
            .iter()
            .enumerate()
            .map(|(at, sink)| Self::tab(id, at, directory, sink))
            .collect_view();

        view! {
            <section>
                <h2>{source.name().to_owned()} " (" {directory.to_owned()} ")"</h2>
                <div class="tabs">{tabs}</div>
            </section>
        }
    }

    fn tab(id: &str, at: usize, directory: &str, sink: &SinkOverview) -> impl IntoView + use<> {
        let tab = format!("tab-{id}-{at}");

        view! {
            <input type="radio" name=format!("tabs-{id}") id=tab.clone() checked=at == 0 />
            <label for=tab>{sink.name().to_owned()}</label>
            <div class="panel">{Self::panel(directory, sink)}</div>
        }
    }

    fn panel(directory: &str, sink: &SinkOverview) -> AnyView {
        match sink.inspection() {
            Ok(inspection) => Self::inspected(directory, sink.name(), inspection).into_any(),
            Err(cause) => view! {
                <p class="error">{format!("{} could not be read: {cause}", sink.name())}</p>
            }
            .into_any(),
        }
    }

    fn inspected(
        directory: &str,
        sink: &str,
        inspection: &SinkInspection,
    ) -> impl IntoView + use<> {
        let discrepancies = inspection
            .discrepancies()
            .iter()
            .map(|discrepancy| view! { <li>{discrepancy.clone()}</li> })
            .collect_view();

        view! {
            {Self::build_log(directory, sink, inspection)}
            <h3>"Discrepancies"</h3>
            <ul>{discrepancies}</ul>
        }
    }

    fn build_log(directory: &str, sink: &str, inspection: &SinkInspection) -> AnyView {
        let rows = inspection.rows();

        if rows.is_empty() {
            return view! { <p class="error">{format!("No build log in {sink} yet")}</p> }
                .into_any();
        }

        let rows = rows
            .into_iter()
            .map(|row| {
                let rebuild = row.rebuild(directory, sink);

                Self::row(row, rebuild)
            })
            .collect_view();

        view! {
            <table>
                <tr>
                    <th>"Table"</th>
                    <th>"Lifecycle"</th>
                    <th>"Status"</th>
                    <th>"Window"</th>
                    <th>"Base"</th>
                    <th>"Target"</th>
                    <th>"Error"</th>
                    <th>"Rebuild"</th>
                </tr>
                {rows}
            </table>
        }
        .into_any()
    }

    fn row(row: TableRow, rebuild: Option<BuildCommand>) -> impl IntoView {
        view! {
            <tr>
                <td>{row.table.to_string()}</td>
                <td>{row.lifecycle.map(|lifecycle| lifecycle.to_string()).unwrap_or_default()}</td>
                <td>{row.status.to_string()}</td>
                <td>{row.window}</td>
                <td>{row.base.map(|base| base.to_string()).unwrap_or_default()}</td>
                <td>{row.target.map(|target| target.to_string()).unwrap_or_default()}</td>
                <td>{row.status.error().unwrap_or_default().to_owned()}</td>
                <td><code>{rebuild.map(|command| command.to_string()).unwrap_or_default()}</code></td>
            </tr>
        }
    }

    fn build(
        pipelines: Vec<String>,
        sinks: Vec<String>,
        form: BuildForm,
        command: Result<Option<BuildCommand>, String>,
    ) -> impl IntoView {
        view! {
            <form method="get" action="/build">
                <label>
                    "Pipeline "
                    <select name="pipeline">{Self::options(pipelines, form.pipeline.as_deref())}</select>
                </label>
                <label>
                    "Sink " <select name="sink">{Self::options(sinks, form.sink.as_deref())}</select>
                </label>
                <label>
                    "Snapshot "
                    <input type="date" name="snapshot" value=form.snapshot.unwrap_or_default() />
                </label>
                <label>
                    "Tables "
                    <input
                        name="tables"
                        placeholder="shop.people, shop.pets"
                        value=form.tables.unwrap_or_default()
                    />
                </label>
                <label>
                    "Dry run into "
                    <input name="dry_run" value=form.dry_run.unwrap_or_default() />
                </label>
                <button type="submit">"Assemble"</button>
            </form>
            {Self::assembled(command)}
        }
    }

    fn options(choices: Vec<String>, chosen: Option<&str>) -> impl IntoView + use<> {
        choices
            .into_iter()
            .map(|choice| {
                let selected = chosen == Some(choice.as_str());
                let label = choice.clone();

                view! { <option value=choice selected=selected>{label}</option> }
            })
            .collect_view()
    }

    fn assembled(command: Result<Option<BuildCommand>, String>) -> AnyView {
        match command {
            Ok(Some(command)) => view! { <pre><code>{command.to_string()}</code></pre> }.into_any(),
            Ok(None) => view! { <p>"Name a shard to assemble its build."</p> }.into_any(),
            Err(cause) => view! { <p class="error">{cause}</p> }.into_any(),
        }
    }
}
