use arroyo_worker::connectors;
use arroyo_worker::engine::{Program, SubtaskNode};
use arroyo_worker::operators::joins::*;
use arroyo_worker::operators::sinks;
use arroyo_worker::operators::sinks::*;
use arroyo_worker::operators::windows::*;
use arroyo_worker::operators::*;
use arroyo_worker::{LogicalEdge, LogicalNode};
use chrono;
use petgraph::graph::DiGraph;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::str::FromStr;
use std::time::SystemTime;
use tokio::time::{timeout, Duration};
use types::*;

pub fn make_graph() -> DiGraph<LogicalNode, LogicalEdge> {
    todo!();
}

pub fn main() {
    let graph = make_graph();
    arroyo_worker::WorkerServer::new("test", "dpw9h0g9sczp7b2b", graph)
        .start()
        .unwrap();
}
