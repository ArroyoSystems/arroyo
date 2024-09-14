use cornucopia::{CodegenSettings, Error};
use postgres::{Client, NoTls};
use refinery_core::postgres::Client as MigrationClient;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("../arroyo-api/migrations");
}

fn main() -> Result<(), Error> {
    let queries_path = "queries";
    let destination = format!("{}/controller-sql.rs", std::env::var("OUT_DIR").unwrap());
    let settings = CodegenSettings {
        gen_async: true,
        derive_ser: false,
        gen_sync: false,
        gen_sqlite: true,
    };

    println!("cargo:rerun-if-changed={queries_path}");
    println!("cargo:rerun-if-changed=../arroyo-api/migrations");
    println!("cargo:rerun-if-changed=../arroyo-api/sqlite_migrations");

    let mut client = Client::configure()
        .dbname("arroyo")
        .host("localhost")
        .port(5432)
        .user("arroyo")
        .password("arroyo")
        .connect(NoTls)
        .unwrap_or_else(|_| {
            panic!("Could not connect to postgres: arroyo:arroyo@localhost:5432/arroyo")
        });

    let mut migration_client = MigrationClient::configure()
        .dbname("arroyo")
        .host("localhost")
        .port(5432)
        .user("arroyo")
        .password("arroyo")
        .connect(NoTls)
        .unwrap_or_else(|_| {
            panic!("Could not connect to postgres: arroyo:arroyo@localhost:5432/arroyo")
        });
    embedded::migrations::runner()
        .run(&mut migration_client)
        .unwrap();

    let mut sqlite =
        rusqlite::Connection::open_in_memory().expect("Couldn't open sqlite memory connection");
    let migrations = refinery::load_sql_migrations("../arroyo-api/sqlite_migrations").unwrap();
    refinery::Runner::new(&migrations)
        .run(&mut sqlite)
        .expect("Failed to run migrations");

    cornucopia::generate_live_with_sqlite(
        &mut client,
        queries_path,
        Some(&destination),
        &sqlite,
        settings,
    )?;

    Ok(())
}
