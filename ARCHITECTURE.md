# Arroyo Architecture

This document describes the major parts of the Arroyo repository and how they fit together at runtime.

## System overview

Arroyo is a distributed stream processing engine built around a small control plane, a scalable worker runtime, and a SQL-first planning stack.

At a high level:

1. Users create pipelines through the Web UI, REST API, or CLI.
2. The API validates SQL and UDF definitions, persists metadata, and exposes cluster/job state.
3. The planner lowers SQL into Arroyo's logical dataflow representation.
4. The controller decides when jobs should run and asks a scheduler to provision workers.
5. Workers deserialize the logical program into an executable graph of operators and exchanges.
6. Checkpoints and artifacts are written to durable storage so jobs can recover and resume.

```text
Browser / CLI
    |
    v
Web UI + REST API  <---->  Metadata DB (Postgres or SQLite)
    |                             |
    | validate / create jobs      | pipelines, jobs, checkpoints, connections, UDFs
    v                             |
SQL planner + UDF services        |
    |                             |
    v                             |
Controller -----------------------+
    |
    | schedules workers / tracks job state
    v
Workers <---- shuffle / control ----> Workers
    |
    +----> Connectors (Kafka, files, Iceberg, MQTT, Redis, ...)
    |
    +----> Checkpoint + artifact storage (S3/R2/GCS/Azure/local FS)
```

## Major runtime pieces

| Component | Role | Primary implementation |
| --- | --- | --- |
| `crates/arroyo` | CLI entrypoint; starts API, controller, compiler, worker, node, or a full cluster | `crates/arroyo/src/main.rs` |
| Web UI | Browser experience for connections, pipelines, jobs, and UDFs | `webui/src` |
| `arroyo-api` | REST API, OpenAPI surface, static asset serving, SQL/UDF validation, metadata access | `crates/arroyo-api/src` |
| `arroyo-controller` | Job state machine, scheduling, worker coordination, checkpoint orchestration when the controller owns the job controller | `crates/arroyo-controller/src` |
| `arroyo-worker` | Distributed execution runtime, operator hosting, shuffle networking, leader-side worker job controller | `crates/arroyo-worker/src` |
| `arroyo-compiler-service` | Builds dynamic Rust UDF crates and stores shared library artifacts | `crates/arroyo-compiler-service/src/lib.rs` |
| `arroyo-node` | Node agent that spawns and manages worker subprocesses for the node scheduler | `crates/arroyo-node/src/lib.rs` |
| Metadata database | Persists pipelines, jobs, checkpoint metadata, connection profiles, and UDF metadata | `crates/arroyo-api/migrations`, `crates/arroyo-api/sqlite_migrations` |
| Object storage | Stores checkpoints and compiler artifacts | `crates/arroyo-state`, `crates/arroyo-storage` |

## Control plane

### Entry modes

The `arroyo` binary exposes several start modes:

- `arroyo api`
- `arroyo controller`
- `arroyo compiler`
- `arroyo worker`
- `arroyo node`
- `arroyo cluster`
- `arroyo run`
- `arroyo visualize`

`arroyo cluster` starts the admin server plus the API, compiler service, and controller in one process. `arroyo worker` and `arroyo node` can also run as separate processes when the selected scheduler requires them.

### API server

The API server is the user-facing control plane boundary.

It is responsible for:

- serving the SPA and embedded static assets
- exposing REST endpoints under `/api/v1`
- publishing OpenAPI docs and Swagger UI
- validating queries, connectors, schemas, and UDFs
- reading and writing metadata in the configured database
- talking to the compiler service when UDFs must be built

Notable details:

- The UI bundle is embedded directly into the Rust binary from `../../webui/dist`.
- The API serves both JSON APIs and the browser app from the same process.
- The generated OpenAPI document is also used to generate the Rust client in `arroyo-openapi`.

### Controller

The controller owns cluster orchestration.

Its responsibilities include:

- polling persisted jobs and reconciling desired state with actual state
- choosing a scheduler implementation
- registering workers and nodes
- tracking task start, finish, heartbeat, and failure events
- exposing job metrics and job-controller gRPC services
- transitioning jobs through scheduling, running, restarting, finishing, and stopping states

The controller uses a `Scheduler` trait with multiple implementations:

- `EmbeddedScheduler`: local/in-process execution
- `ProcessScheduler`: spawns worker subprocesses directly
- `NodeScheduler`: delegates worker lifecycle to `arroyo-node`
- `KubernetesScheduler`: creates worker pods in Kubernetes

### Compiler service

The compiler service builds dynamic Rust UDF crates on demand.

Its flow is:

1. receive a UDF crate definition over gRPC
2. materialize a temporary Cargo crate
3. ensure `cargo` and a C compiler are available
4. build a `cdylib`
5. hash the definition into a stable artifact path
6. upload the compiled library to the configured artifact store

### Node service

The node service exists for the node scheduler.

It:

- registers itself with the controller
- reports heartbeats and free slots
- starts worker subprocesses with the correct environment
- tracks worker PIDs and can stop them on demand

## Planning stack

Arroyo is SQL-first, but planning is split into several layers.

### Schema and function registry

`arroyo-planner` builds an `ArroyoSchemaProvider` that merges:

- source and sink schemas
- connector-provided functions
- built-in scalar, aggregate, and window functions
- user-defined Rust and Python UDF definitions

### SQL to logical graph

The planning path is:

1. parse SQL with Arroyo's dialect support
2. build a DataFusion logical plan
3. rewrite it with Arroyo-specific extensions for windows, joins, sinks, key calculation, and other streaming semantics
4. lower the plan into `arroyo-datastream::logical::LogicalProgram`
5. optimize chaining before execution

`arroyo-datastream` is the bridge between SQL planning and distributed execution. It contains the logical program representation and optimizers that are later consumed by workers.

### Physical operator representation

The planner serializes execution details into protobuf structures in `arroyo-rpc`. Workers later reconstruct executable operators from those serialized descriptions.

## Execution runtime

### Worker startup

A worker receives `StartExecutionReq` from the controller. The request contains:

- the logical program
- task assignments
- restore information for checkpoints
- checkpoint cadence
- optional leader/job-controller information

Workers first enter an initialization phase, report readiness back to the controller, and then transition into a running phase.

### From logical graph to executable graph

`Engine::from_logical` transforms a `LogicalGraph` into a per-worker physical graph.

That step:

- restores checkpoint metadata when needed
- expands each logical node into per-subtask execution nodes
- creates in-memory forward and shuffle edges
- assigns local tasks to local executors
- uses the network manager for remote edges between workers

### Operators

`arroyo-operator` provides the runtime contracts used by workers to instantiate sources, transforms, timers, keyed state, and sinks.

Important execution behaviors include:

- forward vs shuffle exchanges
- timer handling
- checkpoint barriers across multiple inputs
- graceful, immediate, or final source shutdown semantics

### Connectors

`arroyo-connectors` packages the source and sink integrations. The connector registry currently includes systems such as:

- Kafka
- Confluent schema registry-backed sources
- Kinesis
- MQTT
- NATS
- RabbitMQ
- Redis lookups/sinks
- filesystem, Delta Lake, Iceberg, and single-file outputs
- SSE, webhook, websocket, polling HTTP
- preview, stdout, blackhole, impulse, and Nexmark test connectors

Connectors serve two roles:

- planning-time: schemas, config schemas, validation, connector-specific UDF registration
- runtime: actual source and sink operator implementations

## State, checkpoints, and recovery

Arroyo's fault tolerance centers on checkpointing.

### State backend

`arroyo-state` defines the state model and currently aliases `StateBackend` to the parquet-backed implementation. It stores:

- checkpoint metadata
- operator checkpoint metadata
- table state for global and keyed tables
- expiring keyed time tables for windowed/stateful processing

### Checkpoint flow

A checkpoint typically looks like this:

1. the active job controller starts a checkpoint
2. workers receive checkpoint barriers
3. operators flush or persist state
4. operator metadata is written through the state backend
5. checkpoint metadata is finalized
6. optional commit messages are sent for two-phase-commit sinks
7. old checkpoints may be compacted and cleaned up

Job-control ownership can live in two places:

- the controller process
- a designated leader worker when `job_controller` mode is set to `worker`

### Durable storage

`arroyo-storage` abstracts object stores and local filesystems. It parses and normalizes storage URLs for:

- S3-compatible backends
- Cloudflare R2
- GCS
- Azure Blob/Data Lake
- local filesystem paths

That abstraction is reused by:

- `arroyo-state` for checkpoints
- `arroyo-compiler-service` for UDF artifacts

## Shared infrastructure

### RPC and configuration

`arroyo-rpc` is the shared contract crate. It contains:

- gRPC protobuf-generated APIs
- API DTOs used by the REST layer
- shared config loading and defaults
- client helpers for controller and job-controller communication
- shared serialization helpers for plans and operator configs

### Server utilities

`arroyo-server-common` centralizes:

- logging setup
- TLS helpers
- admin/status/metrics/profile endpoints
- telemetry hooks
- shutdown coordination

### Metrics

`arroyo-metrics` provides task and connection level Prometheus metrics used by workers and surfaced through admin endpoints and the UI.

## Repository crate map

### User-facing and service crates

| Crate | Responsibility |
| --- | --- |
| `arroyo` | CLI and multi-service launcher |
| `arroyo-api` | REST API, OpenAPI, static UI serving, metadata access |
| `arroyo-controller` | Job orchestration and schedulers |
| `arroyo-worker` | Distributed execution runtime |
| `arroyo-compiler-service` | Build and store Rust UDF artifacts |
| `arroyo-node` | Node-side worker process manager |
| `arroyo-openapi` | Generated Rust client from the API spec |

### Planning and execution model crates

| Crate | Responsibility |
| --- | --- |
| `arroyo-planner` | SQL parsing, DataFusion integration, streaming rewrites |
| `arroyo-datastream` | Logical program model and chaining optimizer |
| `arroyo-operator` | Runtime operator traits and construction |
| `arroyo-connectors` | Connector catalog and connector implementations |
| `arroyo-formats` | Format handling for Avro, JSON, Protobuf, Parquet, Iceberg, and related encodings |

### State, storage, and shared contracts

| Crate | Responsibility |
| --- | --- |
| `arroyo-state` | Checkpoint/state backend and table abstractions |
| `arroyo-storage` | Object-store and filesystem abstraction |
| `arroyo-rpc` | gRPC contracts, config, shared API types |
| `arroyo-types` | IDs, timestamps, watermarks, metric names, shared low-level types |
| `arroyo-server-common` | Logging, admin server, TLS, shutdown, telemetry |
| `arroyo-metrics` | Prometheus metric registration helpers |

### UDF crates

| Crate | Responsibility |
| --- | --- |
| `arroyo-udf-common` | Shared UDF ABI/contracts |
| `arroyo-udf-host` | Runtime loading/parsing of compiled UDFs |
| `arroyo-udf-plugin` | User-facing proc-macro support |
| `arroyo-udf-macros` | Macro internals |
| `arroyo-udf-python` | Python UDF support |

### Test/support crates

| Crate | Responsibility |
| --- | --- |
| `arroyo-sql-testing` | SQL planning/runtime test helpers |
| `integ` | end-to-end integration tests |

## Deployment shapes

### Local development

Common local modes are:

- `arroyo cluster` for a single-process control plane
- process-scheduled workers launched by the controller
- SQLite for lightweight local metadata
- local filesystem paths for checkpoints and artifacts

### Docker image

The Docker build:

- builds the Web UI first
- embeds the generated `webui/dist` into the API binary
- runs DB migrations during image build for codegen/build correctness
- packages the `arroyo` binary as the runtime entrypoint

### Kubernetes

The Helm chart under `k8s/arroyo` deploys the control plane and configures the controller to use the Kubernetes scheduler.

In this shape:

- the main deployment runs `arroyo cluster`
- the controller uses the Kubernetes scheduler
- worker pods are created dynamically per job
- Postgres may be deployed in-cluster or referenced externally
- Prometheus integration is optional but expected for UI metrics
- checkpoint and artifact URLs must point at shared durable storage for real clusters

## Web UI

The UI is a Vite/React application that focuses on:

- home/cluster overview
- connection creation and schema definition
- pipeline creation, editing, graph visualization, and outputs
- job checkpoint/error/metric inspection
- UDF authoring and management

The browser app talks to the API under `/api/v1` and is served by the same Rust process in normal deployments.

## Recommended companion specs

See the detailed specs in `docs/`:

- `docs/README.md`
- `docs/control-plane-spec.md`
- `docs/planning-and-execution-spec.md`
- `docs/deployment-spec.md`
- `docs/crate-map.md`
