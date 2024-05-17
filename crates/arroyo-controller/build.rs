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
    };

    println!("cargo:rerun-if-changed={queries_path}");
    println!("cargo:rerun-if-changed=../arroyo-api/migrations");

    let config = DatabaseConfig::load();
    let mut client = Client::configure()
        .dbname(&config.name)
        .host(&config.host)
        .port(config.port)
        .user(&config.user)
        .password(&config.password)
        .connect(NoTls)
        .unwrap_or_else(|_| panic!("Could not connect to postgres: {:?}", config));

    cornucopia::generate_live(&mut client, queries_path, Some(&destination), settings)?;

    Ok(())
}
