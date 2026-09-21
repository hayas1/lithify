use duckdb::Connection;
use duckdb::types::FromSql;

use event_replay_config::infra::{Dialect as CapturedAs, Format, S3, Sink, Source, Store};
use event_replay_config::table::Replayed;
use event_replay_config::{Artifact, Host};
use event_replay_core::listener::{CatalogModel, TableEvent, TableListener};
use event_replay_core::position::{Position, RecordedAt};
use event_replay_core::sink::{TablePath, Warehouse};
use event_replay_core::source::{SchemaChangeStream, SchemaChanges};
use event_replay_core::window::Window;
use event_replay_protocol::DuckDb;
use event_replay_protocol::sink::duckdb::client::DuckDbClient;
use event_replay_protocol::source::debezium::{Debezium, DebeziumDdl, Dialect};
use event_replay_protocol::{MySql, Postgres};

struct ChangeLogFiles<'a> {
    store: &'a S3,
    sink: &'a Sink,
}

impl ChangeLogFiles<'_> {
    fn report(&self, host: &Host, window: &Window) {
        let mut sources = host.sources();
        let source = sources.next().expect("a captured source");

        if sources.next().is_some() {
            todo!("order the change log across the boundary between connection eras");
        }

        match source.capture().dialect() {
            CapturedAs::MySql => self.catalog(host, source, &Debezium::new(MySql), window),
            CapturedAs::Postgres => self.catalog(host, source, &Debezium::new(Postgres), window),
        }
    }

    fn catalog<D: Dialect>(&self, host: &Host, source: &Source, log: &Debezium<D>, window: &Window)
    where
        DebeziumDdl<D::Source>: FromSql,
    {
        let Some(table) = self.sink.schema_changes(source) else {
            println!("{} lands no schema changes", source.name());
            return;
        };

        let client = self.client(&table);
        let mut catalog = CatalogModel::default();
        let mut journal = Journal;
        let mut changes = client.schema_changes(log, &table, window).unwrap();

        for change in changes.stream().unwrap() {
            catalog
                .accept(&change.unwrap(), &mut journal)
                .expect("in order");
        }

        println!("--- {table}");
        for model in catalog.tables() {
            let configured = host.table(model.id());
            let nulled = model
                .columns()
                .filter(|column| configured.replayed(column.name()) == Replayed::Null)
                .count();

            println!(
                "{} ({} columns, {nulled} nulled, snapshot {}, history {})",
                model.id(),
                model.columns().count(),
                configured.builds(Artifact::Snapshot),
                configured.builds(Artifact::History),
            );
        }
    }

    fn client(&self, table: &TablePath) -> DuckDbClient {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(&self.secret()).unwrap();
        connection.execute_batch(&self.view(table)).unwrap();

        DuckDbClient::new(connection)
    }

    fn secret(&self) -> String {
        let (endpoint, over_tls) = self.endpoint();

        format!(
            "INSTALL httpfs;
             LOAD httpfs;
             CREATE SECRET (
               TYPE s3,
               PROVIDER config,
               KEY_ID '{}',
               SECRET '{}',
               REGION '{}',
               ENDPOINT '{endpoint}',
               USE_SSL {over_tls},
               URL_STYLE 'path'
             )",
            std::env::var("S3_ACCESS_KEY_ID").expect("S3_ACCESS_KEY_ID"),
            std::env::var("S3_SECRET_ACCESS_KEY").expect("S3_SECRET_ACCESS_KEY"),
            self.store.region(),
        )
    }

    fn endpoint(&self) -> (&str, bool) {
        let endpoint = self.store.endpoint();

        match endpoint.strip_prefix("https://") {
            Some(host) => (host, true),
            None => (endpoint.strip_prefix("http://").unwrap_or(endpoint), false),
        }
    }

    fn view(&self, table: &TablePath) -> String {
        format!(
            "CREATE SCHEMA IF NOT EXISTS {};
             CREATE VIEW {} AS SELECT * FROM {}",
            DuckDb.identifier(table.namespace()),
            DuckDb.table_ref(table),
            self.scan(table),
        )
    }

    fn scan(&self, table: &TablePath) -> String {
        match self.store.format() {
            Format::Parquet => format!("read_parquet('{}')", self.files(table)),
        }
    }

    fn files(&self, table: &TablePath) -> String {
        format!(
            "s3://{}/{}/{}/*.{}",
            self.store.bucket(),
            table.namespace(),
            table.name(),
            self.store.format().extension()
        )
    }
}

struct Journal;

impl<P: Position> TableListener<P> for Journal {
    fn accept(&mut self, event: &TableEvent<'_, P>) {
        let entry = match event {
            TableEvent::Created(created) => format!(
                "created {} ({} columns, key {:?})",
                created.table().id(),
                created.table().columns().count(),
                created.table().primary_key().collect::<Vec<_>>()
            ),
            TableEvent::Dropped(dropped) => format!("dropped {}", dropped.table().id()),
            TableEvent::ColumnAdded(added) => format!(
                "added {}.{} at {}",
                added.after().id(),
                added.column().name(),
                added.column().position()
            ),
            TableEvent::ColumnDropped(dropped) => format!(
                "dropped {}.{}",
                dropped.before().id(),
                dropped.column().name()
            ),
            TableEvent::ColumnRetyped(retyped) => format!(
                "retyped {}.{} {} -> {}",
                retyped.after().id(),
                retyped.column().name(),
                retyped.previous().type_name(),
                retyped.column().type_name()
            ),
            TableEvent::PrimaryKeyChanged(changed) => {
                format!("primary key {}", changed.after().id())
            }
        };
        println!("{entry}");
    }
}

struct Arguments {
    directory: String,
    window: Window,
}

impl Arguments {
    fn read() -> Self {
        let mut arguments = std::env::args().skip(1);
        let directory = arguments.next().expect("a host directory");
        let mut milliseconds = |fallback| {
            arguments
                .next()
                .map(|argument| argument.parse().expect("milliseconds since the epoch"))
                .unwrap_or(fallback)
        };
        let from = RecordedAt::from(milliseconds(0));
        let to = RecordedAt::from(milliseconds(4102444800000));

        Self {
            directory,
            window: Window::new(from, to),
        }
    }
}

fn main() {
    let arguments = Arguments::read();
    let host = Host::open(&arguments.directory).unwrap_or_else(|unreadable| {
        eprintln!("{unreadable}");
        std::process::exit(1);
    });
    let (store, sink) = host
        .sinks()
        .find_map(|sink| sink.warehouse().as_duck_db().map(|store| (store, sink)))
        .expect("a sink this example can read with duckdb");
    let Store::S3(store) = store;

    ChangeLogFiles { store, sink }.report(&host, &arguments.window);
}
