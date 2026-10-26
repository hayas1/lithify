use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;

use leptos::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

use lithify_config::{Declaration, Placing};
use lithify_core::sink::SinkWarehouse;

use crate::pipelines::{PipelineLocation, PipelineOverview, Pipelines};
use crate::route::{AppRoute, BuildLogQueryParam, CheckQueryParam, QueryState};
use crate::screen::{BuildLogScreen, CheckScreen, PipelinesScreen, UnreadScreen};

pub struct Board<C, W: SinkWarehouse, A> {
    pipelines: Arc<Pipelines<C, W, A>>,
}

impl<C, W, A> Board<C, W, A>
where
    C: Clone + DeserializeOwned + Serialize + 'static,
    W: Clone + DeserializeOwned + Placing + Send + Sync + 'static,
    W::Credential: Send + Sync,
    A: Clone + DeserializeOwned + Declaration + 'static,
{
    pub fn new(pipelines: Pipelines<C, W, A>) -> Self {
        Self {
            pipelines: Arc::new(pipelines),
        }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/", get(Self::build_logs))
            .route("/pipelines/{name}", get(Self::build_log))
            .route("/checks", get(Self::checks))
            .route("/checks/{name}", get(Self::check))
            .with_state(self.pipelines)
    }

    pub async fn serve(self, address: SocketAddr) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(address).await?;
        let located = Self::blocking(self.pipelines.clone(), async |pipelines| {
            pipelines.locations()
        })
        .await;

        tracing::info!("serving on http://{}", listener.local_addr()?);
        match located {
            Ok(located) => tracing::info!(
                "reading {}",
                located
                    .iter()
                    .map(PipelineLocation::directory)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Err(cause) => tracing::warn!("reading no pipeline: {cause}"),
        }

        axum::serve(listener, self.router()).await
    }

    async fn blocking<T: Send + 'static>(
        pipelines: Arc<Pipelines<C, W, A>>,
        read: impl AsyncFnOnce(&Pipelines<C, W, A>) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        // A page waits on its read on a thread of its own: the pipeline is read from disk, and a
        // warehouse client may block, like DuckDB does, or hold what cannot be sent across threads.
        // TODO: await the read in the handler itself once the warehouse clients are Send:
        // - DuckDbClient holds an Arc<Mutex<Connection>> and runs each call in spawn_blocking
        //   itself, as a DuckDB connection is Send but not Sync and its calls block; not
        //   block_in_place, which panics on a current-thread runtime such as #[tokio::test]'s
        // - DuckDbSchemaChanges reads its records whole like BigQuerySchemaChanges, as a Statement
        //   cannot outlive the lock on the connection it borrows
        // - DryRun records into an Arc<Mutex<_>> rather than an Rc<RefCell<_>>
        // - trait_variant makes Send variants of WarehouseClient, SinkWarehouse and WarehouseFn,
        //   which the pages ask for while the replay keeps asking for the local ones
        // - the test clients, such as Recorder, are Send too
        // No crate is relied on for it: async-duckdb does no more than a background thread, and
        // duckdb-rs has no async API in sight.
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(read(&pipelines))
        })
        .await
        .map_err(|unjoined| unjoined.to_string())?
    }

    fn html(status: StatusCode, screen: AnyView) -> (StatusCode, Html<String>) {
        (status, Html(format!("<!DOCTYPE html>{}", screen.to_html())))
    }

    fn unread(route: AppRoute, cause: String) -> (StatusCode, Html<String>) {
        Self::html(
            StatusCode::INTERNAL_SERVER_ERROR,
            view! { <UnreadScreen route=route cause=cause /> }.into_any(),
        )
    }

    async fn build_logs(
        State(pipelines): State<Arc<Pipelines<C, W, A>>>,
    ) -> (StatusCode, Html<String>) {
        Self::index(pipelines, AppRoute::BuildLogs).await
    }

    async fn checks(
        State(pipelines): State<Arc<Pipelines<C, W, A>>>,
    ) -> (StatusCode, Html<String>) {
        Self::index(pipelines, AppRoute::Checks).await
    }

    async fn build_log(
        State(pipelines): State<Arc<Pipelines<C, W, A>>>,
        Path(name): Path<String>,
        Query(query): Query<QueryState<BuildLogQueryParam>>,
    ) -> (StatusCode, Html<String>) {
        let route = AppRoute::BuildLog {
            pipeline: name.clone(),
            query,
        };

        Self::pipeline(pipelines, name, route, |route, pipelines, pipeline| {
            view! { <BuildLogScreen route=route pipelines=pipelines pipeline=pipeline /> }
                .into_any()
        })
        .await
    }

    async fn check(
        State(pipelines): State<Arc<Pipelines<C, W, A>>>,
        Path(name): Path<String>,
        Query(query): Query<QueryState<CheckQueryParam>>,
    ) -> (StatusCode, Html<String>) {
        let route = AppRoute::Check {
            pipeline: name.clone(),
            query,
        };

        Self::pipeline(pipelines, name, route, |route, pipelines, pipeline| {
            view! { <CheckScreen route=route pipelines=pipelines pipeline=pipeline /> }.into_any()
        })
        .await
    }

    async fn index(
        pipelines: Arc<Pipelines<C, W, A>>,
        route: AppRoute,
    ) -> (StatusCode, Html<String>) {
        match Self::blocking(pipelines, async |pipelines| pipelines.locations()).await {
            Ok(located) => Self::html(
                StatusCode::OK,
                view! { <PipelinesScreen route=route pipelines=located /> }.into_any(),
            ),
            Err(cause) => Self::unread(route, cause),
        }
    }

    async fn pipeline(
        pipelines: Arc<Pipelines<C, W, A>>,
        name: String,
        route: AppRoute,
        screen: fn(AppRoute, Vec<PipelineLocation>, Option<PipelineOverview>) -> AnyView,
    ) -> (StatusCode, Html<String>) {
        let view = route.view();
        let read = Self::blocking(pipelines, async move |pipelines| {
            Ok((
                pipelines.locations()?,
                pipelines.inspect(&name, view).await?,
            ))
        })
        .await;

        match read {
            Ok((located, pipeline)) => {
                let status = match pipeline {
                    Some(_) => StatusCode::OK,
                    None => StatusCode::NOT_FOUND,
                };

                Self::html(status, screen(route, located, pipeline))
            }
            Err(cause) => Self::unread(route, cause),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::testing::{INFRA, configure, logged, scratch, under};

    fn located(name: &str) -> PathBuf {
        let root = scratch(&format!("server-{name}"));
        configure(&root.join("mysql"), &logged());
        configure(&root.join("postgres"), INFRA);

        root
    }

    async fn get(root: &Path, uri: &str) -> (StatusCode, String) {
        let response = Board::new(under(root))
            .router()
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn a_build_log_shows_the_latest_build_of_each_table_unless_all_are_asked_for() {
        let root = located("viewed");

        let (_, latest) = get(&root, "/pipelines/mysql").await;
        let (_, all) = get(&root, "/pipelines/mysql?view=all").await;

        assert!(latest.contains("3 tables"));
        assert!(latest.contains("href=\"/pipelines/mysql?view=all\""));
        assert!(all.contains("3 rows"));
        assert!(all.contains("href=\"/pipelines/mysql?view=latest\""));
    }

    #[tokio::test]
    async fn the_checks_take_no_view() {
        let (_, page) = get(&located("unviewed"), "/checks/mysql?view=all").await;

        assert!(!page.contains("?view="));
    }

    #[tokio::test]
    async fn the_index_links_to_a_page_of_each_pipeline_without_reading_any() {
        let (status, page) = get(&located("index"), "/").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("href=\"/pipelines/mysql\""));
        assert!(page.contains("href=\"/pipelines/postgres\""));
        assert!(!page.contains("mysql_shop.people_20260920"));
    }

    #[tokio::test]
    async fn a_pipeline_page_lists_each_source_with_a_tab_for_each_sink_it_is_sunk_into() {
        let (status, page) = get(&located("tabs"), "/pipelines/mysql").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("primary"));
        assert!(page.contains("rustfs"));
        assert!(page.contains("warehouse"));
        assert!(page.contains("mysql_shop.people_20260920"));
        assert!(page.contains("no such column"));
        assert!(!page.contains("failed to build the window"));
    }

    #[tokio::test]
    async fn a_row_links_by_its_number_to_a_pane_holding_what_the_build_log_does_not() {
        let (_, page) = get(&located("pane"), "/pipelines/mysql").await;

        assert!(page.contains("href=\"#row-0-0-2\""));
        assert!(page.contains("id=\"row-0-0-2\""));
        assert!(page.contains("class=\"backdrop\""));
        assert!(page.contains("href=\"#closed\""));
        assert!(!page.contains("href=\"#\""));
        assert!(page.contains("<dt>closed</dt><dd class=\"mono\">2026-09-21T00:00:00+00:00</dd>"));
    }

    #[tokio::test]
    async fn a_build_log_links_to_the_checks_of_the_sink_it_shows_by_how_many_they_found() {
        let (_, page) = get(&located("chip"), "/pipelines/mysql").await;

        assert!(page.contains("href=\"/checks/mysql?sink=rustfs\""));
        assert!(page.contains(" discrepancies</a>"));
    }

    #[tokio::test]
    async fn a_pipeline_page_holds_that_pipeline_alone() {
        let (_, page) = get(&located("alone"), "/pipelines/postgres").await;

        assert!(page.contains("No build log"));
        assert!(!page.contains("mysql_shop.people_20260920"));
    }

    #[tokio::test]
    async fn a_pipeline_page_shows_the_sink_its_query_names_or_else_the_first() {
        let root = located("named");

        let (_, first) = get(&root, "/pipelines/mysql").await;
        let (_, named) = get(&root, "/pipelines/mysql?sink=warehouse").await;

        assert!(first.contains("mysql_shop.people_20260920"));
        assert!(!first.contains("JobCreationMode"));
        assert!(named.contains("JobCreationMode"));
        assert!(!named.contains("mysql_shop.people_20260920"));
    }

    #[tokio::test]
    async fn switching_the_view_keeps_the_sink_and_switching_the_sink_keeps_the_view() {
        let (_, page) = get(
            &located("switched"),
            "/pipelines/mysql?view=all&sink=warehouse",
        )
        .await;

        assert!(page.contains("href=\"/pipelines/mysql?view=latest&amp;sink=warehouse\""));
        assert!(page.contains("href=\"/pipelines/mysql?view=all&amp;sink=rustfs\""));
    }

    #[tokio::test]
    async fn a_sink_that_could_not_be_read_says_why_where_its_build_log_would_be() {
        let (status, page) = get(&located("unreached"), "/pipelines/mysql?sink=warehouse").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("warehouse could not be read: JobCreationMode"));
    }

    #[tokio::test]
    async fn a_sink_nothing_was_logged_in_says_so_where_its_build_log_would_be() {
        let (_, page) = get(&located("unbuilt"), "/pipelines/postgres").await;

        assert!(page.contains("No build log in rustfs yet"));
        assert!(!page.contains("<th>target</th>"));
    }

    #[tokio::test]
    async fn a_pipeline_that_could_not_be_opened_says_why_on_its_page() {
        let root = located("unopened");
        configure(&root.join("broken"), "[[source]]\nname = \"primary\"");

        let (status, page) = get(&root, "/pipelines/broken").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("The pipeline could not be read"));
    }

    #[tokio::test]
    async fn a_pipeline_that_is_not_there_is_not_found() {
        let (status, page) = get(&located("missing"), "/pipelines/oracle").await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(page.contains("No pipeline is named oracle"));
    }

    #[tokio::test]
    async fn the_index_says_why_no_pipeline_could_be_read() {
        let (status, page) = get(&scratch("server-empty"), "/").await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(page.contains("configures no pipeline"));
    }

    #[tokio::test]
    async fn every_page_links_to_the_build_log_and_the_checks() {
        let (_, page) = get(&located("topbar"), "/pipelines/mysql").await;

        assert!(page.contains(">Build log</a>"));
        assert!(page.contains("href=\"/checks\""));
    }

    #[tokio::test]
    async fn the_checks_link_to_a_page_of_each_pipeline() {
        let (status, page) = get(&located("checks"), "/checks").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("href=\"/checks/mysql\""));
        assert!(page.contains("href=\"/checks/postgres\""));
    }

    #[tokio::test]
    async fn a_check_page_lists_what_the_check_of_the_sink_it_shows_found() {
        let root = located("checked");

        let (status, page) = get(&root, "/checks/mysql").await;
        let (_, named) = get(&root, "/checks/mysql?sink=warehouse").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("shop.pets failed to build the window"));
        assert!(!page.contains("<table>"));
        assert!(page.contains("href=\"/checks/postgres\""));
        assert!(page.contains("href=\"/checks/mysql?sink=warehouse\""));
        assert!(named.contains("JobCreationMode"));
        assert!(!named.contains("failed to build the window"));
    }

    #[tokio::test]
    async fn a_check_page_of_a_pipeline_that_is_not_there_is_not_found() {
        let (status, _) = get(&located("unchecked"), "/checks/oracle").await;

        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
