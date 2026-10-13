use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;

use crate::command::BuildForm;
use crate::overview::SinkOverview;
use crate::page::Page;

pub trait Survey: Send + Sync + 'static {
    fn directory(&self) -> String;

    fn sinks(&self) -> Result<Vec<String>, String>;

    fn inspect(&self) -> Result<Vec<SinkOverview>, String>;
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
        let directory = survey.directory();

        match Self::blocking(survey, S::inspect).await {
            Ok(sinks) => (
                StatusCode::OK,
                Html(Page::Overview { directory, sinks }.render()),
            ),
            Err(cause) => Self::unsurveyed(cause),
        }
    }

    async fn build(
        State(survey): State<Arc<S>>,
        Query(form): Query<BuildForm>,
    ) -> (StatusCode, Html<String>) {
        let command = form.command(&survey.directory());
        let status = match command {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::BAD_REQUEST,
        };

        match Self::blocking(survey, S::sinks).await {
            Ok(sinks) => (
                status,
                Html(
                    Page::Build {
                        sinks,
                        form,
                        command,
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

    use super::*;
    use crate::overview::tests::overview;

    struct Surveyed(Result<Vec<SinkOverview>, String>);

    impl Survey for Surveyed {
        fn directory(&self) -> String {
            "mysql".to_owned()
        }

        fn sinks(&self) -> Result<Vec<String>, String> {
            Ok(vec!["rustfs".to_owned()])
        }

        fn inspect(&self) -> Result<Vec<SinkOverview>, String> {
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
    async fn the_overview_lists_each_table_and_discrepancy_of_each_sink() {
        let (status, page) = get(Surveyed(Ok(vec![overview()])), "/").await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("rustfs"));
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
    async fn the_overview_says_why_it_could_not_survey() {
        let (status, page) = get(Surveyed(Err("no warehouse".to_owned())), "/").await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(page.contains("no warehouse"));
    }

    #[tokio::test]
    async fn the_build_page_assembles_the_command_its_form_was_filled_with() {
        let (status, page) = get(
            Surveyed(Ok(vec![overview()])),
            "/build?sink=rustfs&snapshot=2026-09-20&tables=shop.pets&dry_run=plans",
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains(
            "lithify build mysql --snapshot 2026-09-20 --sink rustfs --table shop.pets --dry-run plans"
        ));
    }

    #[tokio::test]
    async fn the_build_page_is_assembled_from_the_configuration_alone() {
        let (status, page) = get(
            Surveyed(Err("no warehouse".to_owned())),
            "/build?snapshot=2026-09-20",
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert!(page.contains("rustfs"));
        assert!(page.contains("lithify build mysql --snapshot 2026-09-20"));
    }

    #[tokio::test]
    async fn the_build_page_says_what_its_form_could_not_read() {
        let (status, page) = get(Surveyed(Ok(vec![])), "/build?snapshot=yesterday").await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(page.contains("yesterday is not a date"));
    }
}
