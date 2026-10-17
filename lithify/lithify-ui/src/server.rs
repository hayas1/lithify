use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;

use crate::command::BuildForm;
use crate::overview::PipelineOverview;
use crate::page::Page;

pub trait Survey: Send + Sync + 'static {
    fn pipelines(&self) -> Result<Vec<String>, String>;

    fn sinks(&self) -> Result<Vec<String>, String>;

    fn inspect(&self) -> Result<Vec<PipelineOverview>, String>;
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
            .route("/", get(Self::overview))
            .route("/build", get(Self::build))
            .with_state(self.survey)
    }

    pub fn serve(self, address: SocketAddr) -> std::io::Result<()> {
        tokio::runtime::Runtime::new()?.block_on(async {
            let listener = tokio::net::TcpListener::bind(address).await?;
            let pipelines = Self::blocking(self.survey.clone(), S::pipelines).await;

            tracing::info!("serving on http://{}", listener.local_addr()?);
            match pipelines {
                Ok(pipelines) => tracing::info!("surveying {}", pipelines.join(", ")),
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

    async fn overview(State(survey): State<Arc<S>>) -> (StatusCode, Html<String>) {
        match Self::blocking(survey, S::inspect).await {
            Ok(pipelines) => (StatusCode::OK, Html(Page::Overview { pipelines }.render())),
            Err(cause) => Self::unsurveyed(cause),
        }
    }

    async fn build(
        State(survey): State<Arc<S>>,
        Query(form): Query<BuildForm>,
    ) -> (StatusCode, Html<String>) {
        let configured =
            Self::blocking(survey, |survey| Ok((survey.pipelines()?, survey.sinks()?))).await;
        let (pipelines, sinks) = match configured {
            Ok(configured) => configured,
            Err(cause) => return Self::unsurveyed(cause),
        };
        let command = form.command(pipelines.first().map_or("", String::as_str));
        let status = match command {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::BAD_REQUEST,
        };

        (
            status,
            Html(
                Page::Build {
                    pipelines,
                    sinks,
                    form: Box::new(form),
                    command,
                }
                .render(),
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use lithify_core::schedule::Schedule;

    use super::*;
    use crate::overview::tests::overview;
    use crate::overview::{SinkInspection, SinkOverview, SourceOverview};

    struct Surveyed(Result<Vec<PipelineOverview>, String>);

    impl Survey for Surveyed {
        fn pipelines(&self) -> Result<Vec<String>, String> {
            Ok(vec!["mysql".to_owned(), "postgres".to_owned()])
        }

        fn sinks(&self) -> Result<Vec<String>, String> {
            Ok(vec!["rustfs".to_owned()])
        }

        fn inspect(&self) -> Result<Vec<PipelineOverview>, String> {
            self.0.clone()
        }
    }

    async fn get(survey: Surveyed, uri: &str) -> (StatusCode, String) {
        let response = Board::new(survey)
            .router()
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn the_overview_lists_each_source_with_a_tab_for_each_sink_it_is_sunk_into() {
        let (status, page) = get(Surveyed(Ok(vec![overview()])), "/").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("primary"));
        assert!(page.contains("rustfs"));
        assert!(page.contains("warehouse"));
        assert!(page.contains("mysql_shop.people_20260920"));
        assert!(page.contains("no such column"));
        assert!(page.contains("shop.pets failed to build"));
        assert!(
            page.contains(
                "lithify build mysql --snapshot 2026-09-20 --sink rustfs --table shop.pets"
            )
        );
    }

    #[tokio::test]
    async fn a_sink_that_could_not_be_read_says_why_where_its_build_log_would_be() {
        let (status, page) = get(Surveyed(Ok(vec![overview()])), "/").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("JobCreationMode"));
    }

    #[tokio::test]
    async fn a_sink_nothing_was_logged_in_says_so_where_its_build_log_would_be() {
        let unbuilt = PipelineOverview::new(
            "postgres",
            Ok(vec![SourceOverview::new(
                "primary",
                vec![SinkOverview::new(
                    "rustfs",
                    Ok(SinkInspection::new(Schedule::Daily, vec![], vec![])),
                )],
            )]),
        );

        let (_, page) = get(Surveyed(Ok(vec![unbuilt])), "/").await;

        assert!(page.contains("No build log"));
        assert!(!page.contains("<th>Target</th>"));
    }

    #[tokio::test]
    async fn a_pipeline_that_could_not_be_opened_says_why_beside_the_ones_that_were() {
        let unopened = PipelineOverview::new("postgres", Err("no infra.toml".to_owned()));

        let (status, page) = get(Surveyed(Ok(vec![overview(), unopened])), "/").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("no infra.toml"));
        assert!(page.contains("mysql_shop.people_20260920"));
    }

    #[tokio::test]
    async fn the_overview_says_why_it_could_not_survey() {
        let (status, page) = get(Surveyed(Err("no pipelines".to_owned())), "/").await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(page.contains("no pipelines"));
    }

    #[tokio::test]
    async fn the_build_page_assembles_the_command_its_form_was_filled_with() {
        let (status, page) = get(
            Surveyed(Ok(vec![overview()])),
            "/build?pipeline=postgres&sink=rustfs&snapshot=2026-09-20&tables=shop.pets&dry_run=plans",
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains(
            "lithify build postgres --snapshot 2026-09-20 --sink rustfs --table shop.pets --dry-run plans"
        ));
    }

    #[tokio::test]
    async fn the_build_page_builds_the_first_pipeline_unless_its_form_names_one() {
        let (status, page) = get(
            Surveyed(Err("no warehouse".to_owned())),
            "/build?snapshot=2026-09-20",
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("rustfs"));
        assert!(page.contains("postgres"));
        assert!(page.contains("lithify build mysql --snapshot 2026-09-20"));
    }

    #[tokio::test]
    async fn the_build_page_says_what_its_form_could_not_read() {
        let (status, page) = get(Surveyed(Ok(vec![])), "/build?snapshot=yesterday").await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(page.contains("yesterday is not a date"));
    }
}
