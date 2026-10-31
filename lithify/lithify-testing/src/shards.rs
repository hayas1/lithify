use chrono::DateTime;
use duckdb::Connection;
use duckdb::types::Value;
use ulid::Ulid;

use lithify_core::model::ColumnName;
use lithify_core::sink::{Namespace, WarehouseClient};
use lithify_core::ts::Ts;
use lithify_core::window::Window;
use lithify_pipeline::artifact::ArtifactControl;
use lithify_pipeline::build_log::{BuildLog, Run, Status};
use lithify_pipeline::runner::Runner;
use lithify_protocol::sink::duckdb::client::DuckDbClient;
use lithify_protocol::{Debezium, DuckDbWarehouse, MySql};

use crate::landing::Landing;
use crate::replica::{CellValue, Replica, Row};

pub struct Shards {
    client: DuckDbClient,
}

impl Shards {
    pub async fn over(replica: &Replica) -> Self {
        let warehouse = DuckDbWarehouse::new("cdc");
        let landing = Landing::of(replica).create(&warehouse);
        let client = DuckDbClient::new(Connection::open_in_memory().unwrap(), warehouse);
        client.execute(&landing).await.unwrap();

        Self { client }
    }

    pub async fn build<A: ArtifactControl>(
        &self,
        artifact: &A,
        namespace: &Namespace,
        window: &Window,
        previous: &[BuildLog],
    ) -> BuildLog {
        let mut built = self.settle(artifact, namespace, window, previous).await;
        assert_eq!(built.len(), 1);
        let built = built.remove(0);
        assert_eq!(built.status(), &Status::Succeeded);

        built
    }

    pub async fn settle<A: ArtifactControl>(
        &self,
        artifact: &A,
        namespace: &Namespace,
        window: &Window,
        previous: &[BuildLog],
    ) -> Vec<BuildLog> {
        let log = Debezium::under("value", Landing::TOPIC_PREFIX, MySql);
        let run = Run::new(
            Ulid::nil(),
            Ts::from(DateTime::UNIX_EPOCH),
            artifact.series().artifact(),
            artifact.series().schedule(),
            "primary",
            "{}",
            "0.1.0",
        );

        Runner::new(&self.client, &log)
            .built::<_, Box<dyn std::error::Error>>(artifact, namespace, window, previous, |_| true)
            .await
            .unwrap()
            .into_iter()
            .map(|built| built.logged(&run))
            .collect()
    }

    pub fn columns(&self, built: &BuildLog) -> Vec<String> {
        let target = built.target().unwrap();

        self.client
            .connection()
            .prepare(&format!(
                "SELECT * FROM \"{}\".\"{}\" LIMIT 0",
                target.namespace(),
                target.name()
            ))
            .unwrap()
            .query([])
            .unwrap()
            .as_ref()
            .unwrap()
            .column_names()
    }

    pub fn rows(&self, built: &BuildLog) -> Vec<Row> {
        let target = built.target().unwrap();

        self.read(&format!(
            "SELECT * FROM \"{}\".\"{}\"",
            target.namespace(),
            target.name()
        ))
    }

    pub fn read(&self, query: &str) -> Vec<Row> {
        let mut statement = self.client.connection().prepare(query).unwrap();
        let mut rows = statement.query([]).unwrap();
        let names = rows.as_ref().unwrap().column_names();
        let mut read = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            read.push(
                names
                    .iter()
                    .enumerate()
                    .map(|(index, name)| {
                        (
                            ColumnName::from(name.as_str()),
                            Self::cell(row.get(index).unwrap()),
                        )
                    })
                    .collect(),
            );
        }
        read.sort();

        read
    }

    fn cell(value: Value) -> CellValue {
        match value {
            Value::Null => CellValue::Null,
            Value::SmallInt(number) => CellValue::Int(number.into()),
            Value::Int(number) => CellValue::Int(number.into()),
            Value::BigInt(number) => CellValue::Int(number),
            Value::Text(text) => CellValue::Text(text),
            unread => panic!("a cell of {unread:?} is not read into a replica's"),
        }
    }
}
