# Deployment spec

## Scope

This spec describes the supported deployment shapes and the runtime boundaries between control plane, workers, storage, and configuration.

## Configuration model

The shared config model in `arroyo-rpc` defines service settings for:

- API
- controller
- compiler
- node
- worker
- admin server
- database
- scheduler mode
- checkpoint URL
- artifact URL
- TLS and auth

The same config model is consumed across local, Docker, and Kubernetes deployments.

## Local deployment

### Single-machine cluster

`arroyo cluster` is the simplest local shape. It starts:

- admin server
- API service
- compiler service
- controller service

Workers may then be launched by the configured scheduler.

### Local metadata and state

Typical local setups use:

- SQLite for metadata
- filesystem-backed checkpoint storage
- filesystem-backed artifact storage

This makes `arroyo cluster` and `arroyo run` convenient for local development and testing.

## Process-scheduled deployment

With the process scheduler, the controller directly forks worker subprocesses on the same machine.

This is useful for:

- local development
- lightweight multi-worker testing
- environments where separate node agents are unnecessary

## Node-scheduled deployment

With the node scheduler:

- node agents register with the controller
- the controller chooses nodes with available slots
- each node starts worker subprocesses locally

This introduces a cleaner separation between centralized orchestration and machine-local worker lifecycle.

## Kubernetes deployment

The Helm chart under `k8s/arroyo` packages a production-oriented deployment.

### Control plane

The main deployment runs `arroyo cluster` and configures the controller scheduler as `kubernetes`.

### Workers

The Kubernetes scheduler creates worker pods dynamically. Pod templates include:

- task slots
- labels for cluster/job/run identity
- resource requests and limits
- volumes and volume mounts
- environment overrides
- optional TLS hostname overrides

### Dependencies

The chart can:

- deploy Postgres in-cluster
- reference an external Postgres instance
- annotate resources for Prometheus scraping

### Shared storage requirement

For real clusters, checkpoints and compiler artifacts must live on shared durable storage such as S3, R2, GCS, Azure, or another shared filesystem.

## Docker image

The Docker build is opinionated around the API's embedded frontend.

The image build:

1. installs Rust, Node, pnpm, and build dependencies
2. builds the web console
3. builds the `arroyo` binary
4. publishes an image that defaults to `arroyo cluster`

The runtime image exposes the HTTP/UI entrypoint and can be reused by both control-plane and worker-oriented deployment flows.

## Storage boundaries

### Metadata database

Stores control-plane metadata and should be considered the source of truth for job intent and cluster metadata.

### Checkpoint storage

Stores execution state and recovery data.

### Artifact storage

Stores compiled UDF shared libraries.

## Operational expectations

- API/UI availability depends on the control plane being up.
- Metrics-driven UI views work best when Prometheus is present and scraping Arroyo services.
- Worker provisioning behavior depends entirely on the configured scheduler mode.
- The API binary expects `webui/dist` to exist at build time because the frontend is embedded into the binary.
