use axum::{
    extract::Query,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use flate2::write::GzEncoder;
use flate2::Compression;
use pprof::{protos::Message, ProfilerGuardBuilder};
use std::time::Duration;
use tokio::time::sleep;

pub async fn handle_get_profile(
    Query(params): Query<ProfileParams>,
) -> Result<Response, StatusCode> {
    let frequency = params.frequency.unwrap_or(3000);
    let duration = params.duration.unwrap_or(30);
    match generate_profile(frequency, duration).await {
        Ok(body) => Ok((
            StatusCode::OK,
            [("Content-Type", "application/octet-stream")],
            [(
                "Content-Disposition",
                "attachment; filename=\"profile.pb.gz\"",
            )],
            body,
        )
            .into_response()),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[derive(serde::Deserialize)]
pub struct ProfileParams {
    /// CPU profile collecting frequency, unit: Hz
    pub frequency: Option<i32>,
    /// CPU profile collecting duration, unit: second
    pub duration: Option<u64>,
}

async fn generate_profile(
    frequency: i32,
    duration: u64,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let guard = ProfilerGuardBuilder::default()
        .frequency(frequency)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()?;

    sleep(Duration::from_secs(duration)).await;

    let profile = guard.report().build()?.pprof()?;

    let mut body = Vec::new();
    let mut encoder = GzEncoder::new(&mut body, Compression::default());

    profile.write_to_writer(&mut encoder)?;
    encoder.finish()?;
    Ok(body)
}
