use lithify_core::artifact::UnbuildableArtifact;
use lithify_core::build::TableBuild;
use std::fmt;

use lithify_core::sink::{Namespace, WarehouseClient, WarehouseSql};
use lithify_core::ts::Ts;

use crate::build_log::{
    BuildLog, BuildLogQuery, BuildLogRow, Series, Status, TableBuilt, UnreadableBuildLog,
};

pub trait WarehouseControl: WarehouseClient {
    async fn build_log_rows(
        &self,
        namespace: &Namespace,
        query: &BuildLogQuery,
    ) -> Result<Vec<BuildLogRow>, Self::Error>;

    async fn build_logs<E>(
        &self,
        namespace: &Namespace,
        series: &Series,
        query: &BuildLogQuery,
    ) -> Result<Vec<BuildLog>, E>
    where
        E: From<Self::Error> + From<UnreadableBuildLog>,
    {
        if !self.exists(&BuildLog::path(namespace)).await? {
            return Ok(Vec::new());
        }

        self.build_log_rows(namespace, query)
            .await?
            .into_iter()
            .filter(|row| row.advances(series))
            .map(|row| Ok(BuildLog::try_from(row)?))
            .collect()
    }

    async fn create_build_log<E>(&self, namespace: &Namespace) -> Result<(), E>
    where
        E: From<Self::Error> + From<UnbuildableArtifact>,
    {
        let warehouse = self.warehouse();

        self.execute(&warehouse.create_namespace(namespace)).await?;
        self.execute(
            &BuildLog::create_table(warehouse, namespace).map_err(UnbuildableArtifact::from)?,
        )
        .await?;

        Ok(())
    }

    async fn append_build_logs(
        &self,
        namespace: &Namespace,
        logs: &[BuildLog],
    ) -> Result<(), Self::Error> {
        match BuildLog::insert(logs, self.warehouse(), namespace) {
            Some(insert) => self.execute(&insert).await,
            None => Ok(()),
        }
    }

    async fn build(&self, build: TableBuild) -> TableBuilt {
        let status = match build.refusal() {
            Some(refused) => Status::Failed(refused.to_string()),
            None => match self
                .execute_all(build.statements().unwrap_or_default())
                .await
            {
                Ok(()) => Status::Succeeded,
                Err(failed) => Status::Failed(failed.to_string()),
            },
        };

        TableBuilt::finished(build, status, Ts::now())
    }
}

pub trait WarehouseFn {
    type Output;

    async fn call<K: WarehouseControl>(self, client: &K) -> Self::Output;
}

pub trait SinkWarehouse {
    type Location: Clone + fmt::Debug + PartialEq + Eq + Into<Namespace>;
    type Credential;
    type Error: std::error::Error;

    fn reaches(&self, location: &Self::Location) -> bool;

    async fn with_client<F: WarehouseFn>(
        &self,
        change_log: &Self::Location,
        credential: &Self::Credential,
        f: F,
    ) -> Result<F::Output, Self::Error>;
}
