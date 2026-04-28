# Control plane spec

## Scope

This spec covers the services that accept user intent, persist metadata, manage job lifecycle, and provision execution capacity.

## Services

### CLI (`crates/arroyo`)

The CLI is the top-level launcher. It can start isolated services (`api`, `controller`, `compiler`, `worker`, `node`) or a combined control plane (`cluster`). It also provides local-only workflows such as `run` and `visualize`.

## API service (`arroyo-api`)

### Responsibilities

- expose REST endpoints under `/api/v1`
- serve the embedded Web UI
- publish Swagger UI and OpenAPI docs
- validate SQL, UDFs, connection profiles, connection tables, and schemas
- read and write pipeline/job/UDF/connection metadata in the database
- contact the compiler service to build UDF artifacts

### Main resource areas

- connectors
- connection profiles
- connection tables
- pipelines
- jobs
- checkpoints
- UDFs
- metrics

### Important properties

- The API process is also the static frontend server.
- `index.html` is rewritten at serve time with API endpoint, cluster ID, telemetry, and basename values.
- The API can enforce auth at the HTTP layer through configured auth mode.

## Controller service (`arroyo-controller`)

### Responsibilities

- reconcile desired pipeline state from the metadata store
- track active jobs and runs
- choose a scheduler implementation
- assign work to workers
- collect worker/node heartbeats and task events
- expose gRPC surfaces for controller and job-controller interactions
- own job state transitions and restart logic

### Internal model

The controller keeps an in-memory job-state map and a scheduler instance. A background updater refreshes desired job configs from the database and feeds the per-job state machines.

### Scheduling modes

#### Embedded scheduler

Used for local/in-process execution.

#### Process scheduler

The controller directly spawns worker subprocesses and injects environment/config overrides into each child process.

#### Node scheduler

The controller tracks registered node agents and asks them to start workers with a requested slot count.

#### Kubernetes scheduler

The controller creates worker pods with labels, resource requests, volumes, and environment derived from config and the job being started.

## Compiler service (`arroyo-compiler-service`)

### Responsibilities

- materialize a Rust crate from a UDF definition
- verify toolchain availability
- build a dynamic library
- store the built artifact in durable storage
- return artifact locations to the API/control plane

### Operational notes

- compilation is serialized behind a mutex
- artifact names are derived from a hash of the definition
- build output goes to the configured artifact URL and build directory

## Node service (`arroyo-node`)

### Responsibilities

- register node identity and available slots with the controller
- spawn worker subprocesses for jobs assigned to the node
- stop workers on demand
- report completion when child processes exit

The node service exists only when the controller uses the node scheduler.

## Persistence boundaries

### Metadata database

The metadata database holds cluster and control-plane state, including:

- pipelines and pipeline versions
- jobs and job runs
- connection profiles/tables
- UDF metadata
- checkpoint metadata references
- cluster identity

Arroyo supports both Postgres and SQLite for metadata.

### Object storage

The control plane depends on shared durable storage for:

- compiler artifacts
- state/checkpoint payloads

## Admin and observability surfaces

Each long-running service can expose an admin server with endpoints such as:

- `/status`
- `/metrics`
- `/metrics.pb`
- `/debug/pprof/profile`

These endpoints support health checks, Prometheus scraping, and profiling.
