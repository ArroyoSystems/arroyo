use arroyo_controller::ControllerServer;
use arroyo_types::{grpc_port, ports};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = arroyo_server_common::init_logging("controller");

    let server = ControllerServer::new().await;
    let addr = format!(
        "0.0.0.0:{}",
        grpc_port("controller", ports::CONTROLLER_GRPC)
    )
    .parse()?;
    server.start(addr).await?;

    Ok(())
}
