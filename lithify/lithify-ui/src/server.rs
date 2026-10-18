use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;

use serde::Deserialize;

use crate::overview::{BuildLogView, PipelineLocation, PipelineOverview};
use crate::page::{Page, Section};

pub trait Survey: Send + Sync + 'static {
    fn pipelines(&self) -> Result<Vec<PipelineLocation>, String>;

    fn inspect(&self, name: &str, view: BuildLogView) -> Result<Option<PipelineOverview>, String>;
}

#[derive(Debug, Default, Deserialize)]
struct Viewing {
    #[serde(default)]
    view: BuildLogView,
}

pub struct Board<S> {
    survey: Arc<S>,
}

impl<S: Survey> Board<S> {
    pub fn new(survey: S) -> Self {
        Self {
            survey: Arc::new(survey),
        }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/", get(Self::build_logs))
            .route("/pipelines/{name}", get(Self::build_log))
            .route("/checks", get(Self::checks))
            .route("/checks/{name}", get(Self::check))
            .with_state(self.survey)
    }

    pub fn serve(self, address: SocketAddr) -> std::io::Result<()> {
        tokio::runtime::Runtime::new()?.block_on(async {
            let listener = tokio::net::TcpListener::bind(address).await?;
            let pipelines = Self::blocking(self.survey.clone(), S::pipelines).await;

            tracing::info!("serving on http://{}", listener.local_addr()?);
            match pipelines {
                Ok(pipelines) => tracing::info!(
                    "surveying {}",
                    pipelines
                        .iter()
                        .map(PipelineLocation::directory)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                Err(cause) => tracing::warn!("surveying no pipeline: {cause}"),
            }

            axum::serve(listener, self.router()).await
        })
    }

    async fn blocking<T: Send + 'static>(
        survey: Arc<S>,
        read: impl FnOnce(&S) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        // The pipeline is read from disk and the warehouse clients block, the BigQuery one on a
        // runtime of its own.
        tokio::task::spawn_blocking(move || read(&survey))
            .await
            .map_err(|unjoined| unjoined.to_string())?
    }

    fn unsurveyed(cause: String) -> (StatusCode, Html<String>) {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Html(Page::Unsurveyed { cause }.render()),
        )
    }

    async fn build_logs(State(survey): State<Arc<S>>) -> (StatusCode, Html<String>) {
        Self::index(survey, Section::BuildLog(BuildLogView::Latest)).await
    }

    async fn checks(State(survey): State<Arc<S>>) -> (StatusCode, Html<String>) {
        Self::index(survey, Section::Checks).await
    }

    async fn build_log(
        State(survey): State<Arc<S>>,
        Path(name): Path<String>,
        Query(viewing): Query<Viewing>,
    ) -> (StatusCode, Html<String>) {
        Self::pipeline(survey, name, Section::BuildLog(viewing.view)).await
    }

    async fn check(
        State(survey): State<Arc<S>>,
        Path(name): Path<String>,
    ) -> (StatusCode, Html<String>) {
        Self::pipeline(survey, name, Section::Checks).await
    }

    async fn index(survey: Arc<S>, section: Section) -> (StatusCode, Html<String>) {
        match Self::blocking(survey, S::pipelines).await {
            Ok(pipelines) => (
                StatusCode::OK,
                Html(Page::Pipelines { section, pipelines }.render()),
            ),
            Err(cause) => Self::unsurveyed(cause),
        }
    }

    async fn pipeline(
        survey: Arc<S>,
        name: String,
        section: Section,
    ) -> (StatusCode, Html<String>) {
        let named = name.clone();
        let surveyed = Self::blocking(survey, move |survey| {
            Ok((survey.pipelines()?, survey.inspect(&named, section.view())?))
        })
        .await;

        match surveyed {
            Ok((pipelines, Some(pipeline))) => (
                StatusCode::OK,
                Html(
                    Page::Pipeline {
                        section,
                        pipelines,
                        pipeline,
                    }
                    .render(),
                ),
            ),
            Ok((pipelines, None)) => (
                StatusCode::NOT_FOUND,
                Html(
                    Page::Missing {
                        section,
                        pipelines,
                        name,
                    }
                    .render(),
                ),
            ),
            Err(cause) => Self::unsurveyed(cause),
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use lithify_core::schedule::Schedule;

    use std::sync::Mutex;

    use super::*;
    use crate::overview::tests::overview;
    use crate::overview::{SinkInspection, SinkOverview, SourceOverview};

    struct Surveyed {
        located: Result<Vec<PipelineLocation>, String>,
        inspected: Vec<PipelineOverview>,
        viewed: Mutex<Vec<BuildLogView>>,
    }

    impl Surveyed {
        fn of(inspected: Vec<PipelineOverview>) -> Self {
            Self {
                located: Ok(vec![
                    PipelineLocation::new("mysql", "mysql"),
                    PipelineLocation::new("postgres", "postgres"),
                ]),
                inspected,
                viewed: Mutex::default(),
            }
        }

        fn unlocated(cause: &str) -> Self {
            Self {
                located: Err(cause.to_owned()),
                inspected: vec![],
                viewed: Mutex::default(),
            }
        }
    }

    impl Survey for Surveyed {
        fn pipelines(&self) -> Result<Vec<PipelineLocation>, String> {
            self.located.clone()
        }

        fn inspect(
            &self,
            name: &str,
            view: BuildLogView,
        ) -> Result<Option<PipelineOverview>, String> {
            self.viewed.lock().unwrap().push(view);

            Ok(self
                .inspected
                .iter()
                .find(|pipeline| pipeline.location().name() == name)
                .cloned())
        }
    }

    fn unbuilt() -> PipelineOverview {
        PipelineOverview::new(
            PipelineLocation::new("postgres", "postgres"),
            Ok(vec![SourceOverview::new(
                "primary",
                vec![SinkOverview::new(
                    "rustfs",
                    Ok(SinkInspection::new(Schedule::Daily, vec![], vec![])),
                )],
            )]),
        )
    }

    async fn get(survey: Surveyed, uri: &str) -> (StatusCode, String) {
        let (status, page, _) = viewed(survey, uri).await;

        (status, page)
    }

    async fn viewed(survey: Surveyed, uri: &str) -> (StatusCode, String, Vec<BuildLogView>) {
        let survey = Arc::new(survey);
        let response = Board {
            survey: survey.clone(),
        }
        .router()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let viewed = survey.viewed.lock().unwrap().clone();

        (status, String::from_utf8(body.to_vec()).unwrap(), viewed)
    }

    #[tokio::test]
    async fn a_build_log_shows_the_latest_build_of_each_table_unless_all_are_asked_for() {
        let (_, latest, asked) = viewed(Surveyed::of(vec![overview()]), "/pipelines/mysql").await;
        assert_eq!(asked, [BuildLogView::Latest]);
        assert!(latest.contains("href=\"?view=all\""));

        let (_, all, asked) =
            viewed(Surveyed::of(vec![overview()]), "/pipelines/mysql?view=all").await;
        assert_eq!(asked, [BuildLogView::All]);
        assert!(all.contains("href=\"?view=latest\""));
        assert!(all.contains("2 rows"));
    }

    #[tokio::test]
    async fn the_checks_read_the_latest_build_of_each_table_alone() {
        let (_, page, asked) =
            viewed(Surveyed::of(vec![overview()]), "/checks/mysql?view=all").await;

        assert_eq!(asked, [BuildLogView::Latest]);
        assert!(!page.contains("?view="));
    }

    #[tokio::test]
    async fn the_index_links_to_a_page_of_each_pipeline_without_reading_any() {
        let (status, page) = get(Surveyed::of(vec![overview()]), "/").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("href=\"/pipelines/mysql\""));
        assert!(page.contains("href=\"/pipelines/postgres\""));
        assert!(!page.contains("mysql_shop.people_20260920"));
    }

    #[tokio::test]
    async fn a_pipeline_page_lists_each_source_with_a_tab_for_each_sink_it_is_sunk_into() {
        let (status, page) = get(Surveyed::of(vec![overview()]), "/pipelines/mysql").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("primary"));
        assert!(page.contains("rustfs"));
        assert!(page.contains("warehouse"));
        assert!(page.contains("mysql_shop.people_20260920"));
        assert!(page.contains("no such column"));
        assert!(!page.contains("shop.pets failed to build"));
        assert!(
            page.contains(
                "lithify build mysql --snapshot 2026-09-20 --sink rustfs --table shop.pets"
            )
        );
    }

    #[tokio::test]
    async fn a_row_links_by_its_number_to_a_pane_holding_what_the_build_log_does_not() {
        let (_, page) = get(Surveyed::of(vec![overview()]), "/pipelines/mysql").await;

        assert!(page.contains("href=\"#row-0-0-2\""));
        assert!(page.contains("id=\"row-0-0-2\""));
        assert!(page.contains("class=\"backdrop\""));
        assert!(page.contains("href=\"#closed\""));
        assert!(!page.contains("href=\"#\""));
        assert!(!page.contains("<th>rebuild</th>"));
        assert!(
            page.contains(
                "lithify build mysql --snapshot 2026-09-20 --sink rustfs --table shop.pets"
            )
        );
    }

    #[tokio::test]
    async fn a_build_log_links_to_the_checks_of_its_pipeline_by_how_many_they_found() {
        let (_, page) = get(Surveyed::of(vec![overview()]), "/pipelines/mysql").await;

        assert!(page.contains("href=\"/checks/mysql\""));
        assert!(page.contains("1 discrepancies"));
    }

    #[tokio::test]
    async fn a_pipeline_page_holds_that_pipeline_alone() {
        let (_, page) = get(
            Surveyed::of(vec![overview(), unbuilt()]),
            "/pipelines/postgres",
        )
        .await;

        assert!(page.contains("No build log"));
        assert!(!page.contains("mysql_shop.people_20260920"));
    }

    #[tokio::test]
    async fn a_sink_that_could_not_be_read_says_why_where_its_build_log_would_be() {
        let (status, page) = get(Surveyed::of(vec![overview()]), "/pipelines/mysql").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("JobCreationMode"));
    }

    #[tokio::test]
    async fn a_sink_nothing_was_logged_in_says_so_where_its_build_log_would_be() {
        let (_, page) = get(Surveyed::of(vec![unbuilt()]), "/pipelines/postgres").await;

        assert!(page.contains("No build log"));
        assert!(!page.contains("<th>target</th>"));
    }

    #[tokio::test]
    async fn a_pipeline_that_could_not_be_opened_says_why_on_its_page() {
        let unopened = PipelineOverview::new(
            PipelineLocation::new("postgres", "postgres"),
            Err("no infra.toml".to_owned()),
        );

        let (status, page) = get(Surveyed::of(vec![unopened]), "/pipelines/postgres").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("no infra.toml"));
    }

    #[tokio::test]
    async fn a_pipeline_the_survey_does_not_hold_is_not_found() {
        let (status, page) = get(Surveyed::of(vec![overview()]), "/pipelines/oracle").await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(page.contains("oracle"));
    }

    #[tokio::test]
    async fn the_index_says_why_it_could_not_survey() {
        let (status, page) = get(Surveyed::unlocated("no pipelines"), "/").await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(page.contains("no pipelines"));
    }

    #[tokio::test]
    async fn every_page_links_to_the_build_log_and_the_checks() {
        let (_, page) = get(Surveyed::of(vec![overview()]), "/pipelines/mysql").await;

        assert!(page.contains(">Build log</a>"));
        assert!(page.contains("href=\"/checks\""));
    }

    #[tokio::test]
    async fn the_checks_link_to_a_page_of_each_pipeline() {
        let (status, page) = get(Surveyed::of(vec![]), "/checks").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("href=\"/checks/mysql\""));
        assert!(page.contains("href=\"/checks/postgres\""));
    }

    #[tokio::test]
    async fn a_check_page_lists_what_the_check_of_each_sink_found() {
        let (status, page) = get(Surveyed::of(vec![overview()]), "/checks/mysql").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("shop.pets failed to build"));
        assert!(page.contains("JobCreationMode"));
        assert!(!page.contains("mysql_shop.people_20260920"));
        assert!(page.contains("href=\"/checks/postgres\""));
    }

    #[tokio::test]
    async fn a_check_page_of_a_pipeline_the_survey_does_not_hold_is_not_found() {
        let (status, _) = get(Surveyed::of(vec![overview()]), "/checks/oracle").await;

        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn no_page_assembles_a_build_any_more() {
        let (status, _) = get(Surveyed::of(vec![]), "/build?snapshot=2026-09-20").await;

        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
