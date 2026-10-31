use lithify_core::artifact::ArtifactName;

use crate::artifact::Declaration;
use crate::infra::Sink;
use crate::stepping::SeriesLog;
use crate::warehouse::SinkWarehouse;

pub struct Lineup<D>(Vec<Member<D>>);

impl<D> Lineup<D> {
    pub fn of<W: SinkWarehouse, A: Declaration>(
        sink: &Sink<W, A>,
        declare: impl Fn(&ArtifactName, &A) -> D,
    ) -> Self {
        Self(
            sink.artifacts()
                .filter_map(|(name, declared)| {
                    Some(Member {
                        log: sink.log(name)?,
                        base: sink.base(name),
                        declared: declare(name, declared),
                    })
                })
                .collect(),
        )
    }

    pub fn members(&self) -> impl Iterator<Item = &Member<D>> {
        self.0.iter()
    }
}

pub struct Member<D> {
    declared: D,
    log: SeriesLog,
    base: Option<SeriesLog>,
}

impl<D> Member<D> {
    pub fn declared(&self) -> &D {
        &self.declared
    }

    pub fn log(&self) -> &SeriesLog {
        &self.log
    }

    pub fn base(&self) -> Option<&SeriesLog> {
        self.base.as_ref()
    }
}
