use lithify_core::artifact::ArtifactName;
use lithify_core::sink::Namespace;

use crate::build_log::Series;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Materialization {
    namespace: Namespace,
    series: Series,
    base: Option<Box<Materialization>>,
}

impl Materialization {
    pub fn new(namespace: Namespace, series: Series, base: Option<Materialization>) -> Self {
        Self {
            namespace,
            series,
            base: base.map(Box::new),
        }
    }

    pub fn artifact(&self) -> &ArtifactName {
        self.series.artifact()
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub fn series(&self) -> &Series {
        &self.series
    }

    pub fn base(&self) -> Option<&Materialization> {
        self.base.as_deref()
    }
}
