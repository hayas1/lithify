use lithify_core::artifact::ArtifactName;

use crate::artifact::Declaration;
use crate::infra::Sink;
use crate::stepping::SeriesLog;
use crate::warehouse::SinkWarehouse;

pub struct Lineup<D>(Vec<Member<D>>);

impl<D> Lineup<D> {
    pub fn of<W: SinkWarehouse, A: Declaration, E>(
        sink: &Sink<W, A>,
        declare: impl Fn(&ArtifactName, &A) -> Result<D, E>,
    ) -> Result<Self, E> {
        sink.artifacts()
            .filter_map(|(name, declared)| Some((name, declared, sink.log(name)?)))
            .map(|(name, declared, log)| {
                Ok(Member {
                    declared: declare(name, declared)?,
                    base: sink.base(name),
                    log,
                })
            })
            .collect::<Result<_, _>>()
            .map(Self)
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
