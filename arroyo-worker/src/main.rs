use prost::Message;
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::grpc::api;
use arroyo_types::ARROYO_PROGRAM_ENV;
use arroyo_worker::WorkerServer;
use base64::{Engine as _, engine::general_purpose};


pub fn main() {
    let graph = std::env::var(ARROYO_PROGRAM_ENV).expect("ARROYO_PROGRAM not set");
    let graph = general_purpose::STANDARD_NO_PAD.decode(&graph)
        .expect("ARROYO_PROGRAM not valid base64");

    let graph = api::ArrowProgram::decode(&graph[..])
        .expect("ARROYO_PROGRAM is not a valid protobuf");

    let logical = LogicalProgram::try_from(graph)
        .expect("Failed to create LogicalProgram");

    WorkerServer::new("program", "blah",  logical.graph).start().unwrap();
}