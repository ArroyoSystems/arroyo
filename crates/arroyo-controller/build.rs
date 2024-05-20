use arroyo_types::DatabaseConfig;
use cornucopia::{CodegenSettings, Error};
use postgres::{Client, NoTls};

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

    let config = DatabaseConfig::load();
    let mut client = Client::configure()
        .dbname(&config.name)
        .host(&config.host)
        .port(config.port)
        .user(&config.user)
        .password(&config.password)
        .connect(NoTls)
        .unwrap_or_else(|_| panic!("Could not connect to postgres: {:?}", config));

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
