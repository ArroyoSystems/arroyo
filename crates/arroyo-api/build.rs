use cornucopia::{CodegenSettings, Error};
use postgres::{Client, NoTls};
use std::path::Path;
use std::process::Command;

fn ensure_webui_built() {
    let webui_dir = Path::new("../../webui");
    let dist_index = webui_dir.join("dist/index.html");

    // Only re-run this build script when dist/ changes (or is created).
    // We intentionally do NOT track webui/src/ here — the webui has its own
    // build toolchain. Developers who modify the frontend should run
    // `pnpm build` in webui/ themselves; this block only bootstraps
    // an initial build so that `cargo build` works out of the box.
    println!("cargo:rerun-if-changed=../../webui/dist");

    if dist_index.exists() {
        return;
    }

    eprintln!("webui/dist not found — building the web UI...");

    // Verify pnpm is available
    let pnpm = Command::new("pnpm").arg("--version").output();
    if pnpm.is_err() || !pnpm.unwrap().status.success() {
        panic!(
            "\n\n\
            ========================================================\n\
            pnpm is required to build the Arroyo web UI but was not\n\
            found on $PATH.\n\
            ========================================================\n"
        );
    }

    // Install dependencies (frozen so the lockfile is never mutated by a Rust build)
    let install = Command::new("pnpm")
        .args(["install", "--frozen-lockfile"])
        .current_dir(webui_dir)
        .status()
        .expect("failed to spawn `pnpm install`");

    if !install.success() {
        panic!("pnpm install failed in webui/ — see output above");
    }

    // Build the frontend
    let build = Command::new("pnpm")
        .arg("build")
        .current_dir(webui_dir)
        .status()
        .expect("failed to spawn `pnpm build`");

    if !build.success() {
        panic!("pnpm build failed in webui/ — see output above");
    }

    // Sanity check
    if !dist_index.exists() {
        panic!(
            "pnpm build succeeded but webui/dist/index.html was not created. \
             Check the vite config in webui/."
        );
    }

    eprintln!("webui built successfully.");
}

fn main() -> Result<(), Error> {
    ensure_webui_built();

    let queries_path = "queries";
    let destination = format!("{}/api-sql.rs", std::env::var("OUT_DIR").unwrap());
    let settings = CodegenSettings {
        gen_async: true,
        derive_ser: true,
        gen_sync: false,
        gen_sqlite: true,
    };

    println!("cargo:rerun-if-changed={queries_path}");
    println!("cargo:rerun-if-changed=migrations");
    println!("cargo:rerun-if-changed=sqlite_migrations");

    let mut client = match std::env::var("DATABASE_URL") {
        Ok(database_url) => Client::connect(&database_url, NoTls)
            .unwrap_or_else(|e| panic!("Failed to connect to database: {e}")),
        Err(_) => Client::configure()
            .dbname("arroyo")
            .host("localhost")
            .port(5432)
            .user("arroyo")
            .password("arroyo")
            .connect(NoTls)
            .unwrap_or_else(|e| panic!("Failed to connect to default database: {e}")),
    };

    let mut sqlite =
        rusqlite::Connection::open_in_memory().expect("Couldn't open sqlite memory connection");
    let migrations = refinery::load_sql_migrations("sqlite_migrations").unwrap();
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
