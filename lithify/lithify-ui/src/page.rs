use leptos::prelude::*;

use crate::command::{BuildCommand, BuildForm};
use crate::overview::{SinkOverview, TableRow};

pub enum Page {
    Overview {
        directory: String,
        sinks: Vec<SinkOverview>,
    },
    Unsurveyed {
        cause: String,
    },
    Build {
        sinks: Vec<String>,
        form: BuildForm,
        command: Result<Option<BuildCommand>, String>,
    },
}

impl Page {
    pub fn render(self) -> String {
        let body = match self {
            Self::Overview { directory, sinks } => Self::overview(&directory, sinks).into_any(),
            Self::Unsurveyed { cause } => view! { <p class="error">{cause}</p> }.into_any(),
            Self::Build {
                sinks,
                form,
                command,
            } => Self::build(sinks, form, command).into_any(),
        };
        let document = view! {
            <html>
                <head>
                    <meta charset="utf-8" />
                    <title>"lithify"</title>
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

    fn overview(directory: &str, sinks: Vec<SinkOverview>) -> impl IntoView + use<> {
        sinks
            .iter()
            .map(|sink| Self::section(directory, sink))
            .collect_view()
    }

    fn section(directory: &str, sink: &SinkOverview) -> impl IntoView + use<> {
        let rows = sink
            .rows()
            .into_iter()
            .map(|row| {
                let rebuild = row.rebuild(directory, sink.name());

                Self::row(row, rebuild)
            })
            .collect_view();
        let discrepancies = sink
            .discrepancies()
            .iter()
            .map(|discrepancy| view! { <li>{discrepancy.clone()}</li> })
            .collect_view();

        view! {
            <section>
                <h2>{sink.name().to_owned()}</h2>
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
                <h3>"Discrepancies"</h3>
                <ul>{discrepancies}</ul>
            </section>
        }
    }

    fn row(row: TableRow, rebuild: Option<BuildCommand>) -> impl IntoView {
        view! {
            <tr>
                <td>{row.table.to_string()}</td>
                <td>{row.lifecycle.map(|lifecycle| lifecycle.to_string()).unwrap_or_default()}</td>
                <td>{row.status.to_string()}</td>
                <td>{row.window}</td>
                <td>{row.base.map(|base| base.to_string()).unwrap_or_default()}</td>
                <td>{row.target.to_string()}</td>
                <td>{row.status.error().unwrap_or_default().to_owned()}</td>
                <td><code>{rebuild.map(|command| command.to_string()).unwrap_or_default()}</code></td>
            </tr>
        }
    }

    fn build(
        sinks: Vec<String>,
        form: BuildForm,
        command: Result<Option<BuildCommand>, String>,
    ) -> impl IntoView {
        view! {
            <form method="get" action="/build">
                <label>"Sink " <select name="sink">{Self::options(sinks, &form)}</select></label>
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

    fn options(sinks: Vec<String>, form: &BuildForm) -> impl IntoView + use<> {
        sinks
            .into_iter()
            .map(|sink| {
                let selected = form.sink.as_deref() == Some(sink.as_str());
                let label = sink.clone();

                view! { <option value=sink selected=selected>{label}</option> }
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
