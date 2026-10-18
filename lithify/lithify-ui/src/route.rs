use serde::{Deserialize, Serialize};

use crate::pipelines::BuildLogView;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    BuildLog,
    Checks,
}

impl Section {
    pub const ALL: [Self; 2] = [Self::BuildLog, Self::Checks];

    pub fn label(self) -> &'static str {
        match self {
            Self::BuildLog => "Build log",
            Self::Checks => "Checks",
        }
    }

    pub fn index(self) -> AppRoute {
        match self {
            Self::BuildLog => AppRoute::BuildLogs,
            Self::Checks => AppRoute::Checks,
        }
    }

    pub fn pipeline(self, pipeline: &str) -> AppRoute {
        match self {
            Self::BuildLog => AppRoute::BuildLog {
                pipeline: pipeline.to_owned(),
                query: QueryState::default(),
            },
            Self::Checks => AppRoute::Check {
                pipeline: pipeline.to_owned(),
                query: QueryState::default(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppRoute {
    BuildLogs,
    BuildLog {
        pipeline: String,
        query: QueryState<BuildLogQueryParam>,
    },
    Checks,
    Check {
        pipeline: String,
        query: QueryState<CheckQueryParam>,
    },
}

impl AppRoute {
    pub fn href(&self) -> String {
        match self {
            Self::BuildLogs => "/".to_owned(),
            Self::BuildLog { pipeline, query } => {
                format!("/pipelines/{pipeline}{}", query.written())
            }
            Self::Checks => "/checks".to_owned(),
            Self::Check { pipeline, query } => format!("/checks/{pipeline}{}", query.written()),
        }
    }

    pub fn pipeline(&self) -> Option<&str> {
        match self {
            Self::BuildLog { pipeline, .. } | Self::Check { pipeline, .. } => Some(pipeline),
            Self::BuildLogs | Self::Checks => None,
        }
    }

    pub fn section(&self) -> Section {
        match self {
            Self::BuildLogs | Self::BuildLog { .. } => Section::BuildLog,
            Self::Checks | Self::Check { .. } => Section::Checks,
        }
    }

    pub fn view(&self) -> BuildLogView {
        match self {
            Self::BuildLog { query, .. } => query.page().view(),
            Self::BuildLogs | Self::Checks | Self::Check { .. } => BuildLogView::default(),
        }
    }

    pub fn sink_name(&self) -> Option<&str> {
        match self {
            Self::BuildLog { query, .. } => query.page().sink_name(),
            Self::Check { query, .. } => query.page().sink_name(),
            Self::BuildLogs | Self::Checks => None,
        }
    }

    pub fn showing(&self, sink_name: &str) -> Self {
        match self {
            Self::BuildLog { pipeline, query } => Self::BuildLog {
                pipeline: pipeline.clone(),
                query: QueryState::new(BuildLogQueryParam::new(query.page().view, Some(sink_name))),
            },
            Self::Check { pipeline, .. } => Self::Check {
                pipeline: pipeline.clone(),
                query: QueryState::new(CheckQueryParam::new(Some(sink_name))),
            },
            Self::BuildLogs | Self::Checks => self.clone(),
        }
    }

    pub fn viewing(&self, view: BuildLogView) -> Self {
        match self {
            Self::BuildLog { pipeline, query } => Self::BuildLog {
                pipeline: pipeline.clone(),
                query: QueryState::new(BuildLogQueryParam::new(
                    Some(view),
                    query.page().sink_name(),
                )),
            },
            Self::BuildLogs | Self::Checks | Self::Check { .. } => self.clone(),
        }
    }

    pub fn at(&self, pipeline: &str) -> Self {
        match self {
            Self::BuildLog { query, .. } => Self::BuildLog {
                pipeline: pipeline.to_owned(),
                query: QueryState::new(BuildLogQueryParam::new(query.page().view, None)),
            },
            Self::BuildLogs | Self::Checks | Self::Check { .. } => {
                self.section().pipeline(pipeline)
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct QueryState<P> {
    #[serde(flatten)]
    page: P,
}

impl<P> QueryState<P> {
    pub fn new(page: P) -> Self {
        Self { page }
    }

    pub fn page(&self) -> &P {
        &self.page
    }
}

impl<P: Serialize> QueryState<P> {
    fn written(&self) -> String {
        match serde_urlencoded::to_string(self).unwrap_or_default() {
            written if written.is_empty() => written,
            written => format!("?{written}"),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct BuildLogQueryParam {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    view: Option<BuildLogView>,
    #[serde(default, rename = "sink", skip_serializing_if = "Option::is_none")]
    sink_name: Option<String>,
}

impl BuildLogQueryParam {
    pub fn new(view: Option<BuildLogView>, sink_name: Option<&str>) -> Self {
        Self {
            view,
            sink_name: sink_name.map(str::to_owned),
        }
    }

    pub fn view(&self) -> BuildLogView {
        self.view.unwrap_or_default()
    }

    pub fn sink_name(&self) -> Option<&str> {
        self.sink_name.as_deref()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct CheckQueryParam {
    #[serde(default, rename = "sink", skip_serializing_if = "Option::is_none")]
    sink_name: Option<String>,
}

impl CheckQueryParam {
    pub fn new(sink_name: Option<&str>) -> Self {
        Self {
            sink_name: sink_name.map(str::to_owned),
        }
    }

    pub fn sink_name(&self) -> Option<&str> {
        self.sink_name.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_log(view: Option<BuildLogView>, sink_name: Option<&str>) -> AppRoute {
        AppRoute::BuildLog {
            pipeline: "mysql".to_owned(),
            query: QueryState::new(BuildLogQueryParam::new(view, sink_name)),
        }
    }

    #[test]
    fn a_route_is_written_as_the_path_and_the_query_it_is_read_from() {
        let route = build_log(Some(BuildLogView::All), Some("warehouse"));

        assert_eq!(route.href(), "/pipelines/mysql?view=all&sink=warehouse");
        assert_eq!(
            AppRoute::BuildLog {
                pipeline: "mysql".to_owned(),
                query: serde_urlencoded::from_str("view=all&sink=warehouse").unwrap(),
            },
            route
        );
    }

    #[test]
    fn a_route_asking_nothing_in_particular_is_written_without_a_query() {
        assert_eq!(AppRoute::BuildLogs.href(), "/");
        assert_eq!(AppRoute::Checks.href(), "/checks");
        assert_eq!(build_log(None, None).href(), "/pipelines/mysql");
        assert_eq!(Section::Checks.pipeline("mysql").href(), "/checks/mysql");
        assert_eq!(
            serde_urlencoded::from_str::<QueryState<CheckQueryParam>>("").unwrap(),
            QueryState::default()
        );
    }

    #[test]
    fn showing_another_sink_keeps_the_view_and_viewing_another_way_keeps_the_sink() {
        let route = build_log(Some(BuildLogView::All), Some("warehouse"));

        assert_eq!(
            route.showing("rustfs").href(),
            "/pipelines/mysql?view=all&sink=rustfs"
        );
        assert_eq!(
            route.viewing(BuildLogView::Latest).href(),
            "/pipelines/mysql?view=latest&sink=warehouse"
        );
    }

    #[test]
    fn another_pipeline_is_reached_in_the_same_section_and_view_without_the_sink() {
        let route = build_log(Some(BuildLogView::All), Some("warehouse"));

        assert_eq!(route.at("postgres").href(), "/pipelines/postgres?view=all");
        assert_eq!(AppRoute::Checks.at("postgres").href(), "/checks/postgres");
    }

    #[test]
    fn a_sink_name_is_written_into_the_query_escaped() {
        assert_eq!(
            Section::Checks
                .pipeline("mysql")
                .showing("lake & sea")
                .href(),
            "/checks/mysql?sink=lake+%26+sea"
        );
    }

    #[test]
    fn a_route_names_the_pipeline_it_reaches_if_any() {
        assert_eq!(build_log(None, None).pipeline(), Some("mysql"));
        assert_eq!(Section::Checks.pipeline("mysql").pipeline(), Some("mysql"));
        assert_eq!(AppRoute::BuildLogs.pipeline(), None);
    }

    #[test]
    fn a_route_belongs_to_the_section_it_is_reached_from() {
        assert_eq!(AppRoute::BuildLogs.section(), Section::BuildLog);
        assert_eq!(build_log(None, None).section(), Section::BuildLog);
        assert_eq!(Section::Checks.pipeline("mysql").section(), Section::Checks);
        assert_eq!(build_log(None, None).view(), BuildLogView::Latest);
        assert_eq!(
            Section::Checks
                .pipeline("mysql")
                .showing("rustfs")
                .sink_name(),
            Some("rustfs")
        );
    }
}
