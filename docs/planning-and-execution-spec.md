# Planning and execution spec

## Scope

This spec covers how Arroyo turns SQL and connector metadata into a distributed execution graph, and how that graph is executed and recovered.

## Planning pipeline

### Inputs

Planning consumes:

- SQL text
- registered connection profiles and tables
- connector-specific schemas and validation rules
- built-in functions
- user-defined Rust UDFs

### Stages

1. Parse SQL using Arroyo's SQL dialect.
2. Build a DataFusion logical plan.
3. Apply Arroyo-specific rewrites for streaming semantics.
4. Lower the plan into `arroyo-datastream::logical::LogicalProgram`.
5. Apply logical optimizations such as chaining.
6. Serialize operator configuration and graph edges into RPC/protobuf payloads.

## Logical program model

`arroyo-datastream` is the shared logical graph representation between the planner and workers.

It carries:

- nodes/operators
- edge semantics
- per-node parallelism
- operator-chain structure
- window definitions and execution metadata

## Worker execution model

### Startup

When the controller starts a job, each worker receives a `StartExecutionReq` containing the program, task assignments, checkpoint restore info, and leader/job-controller settings.

### Graph materialization

Workers transform the logical graph into a physical graph of per-subtask execution nodes.

That process expands:

- one logical operator node into `parallelism` execution subtasks
- forward edges into point-to-point links
- shuffle and join edges into fan-out/fan-in exchange links

### Local and remote execution

- Local edges use bounded in-memory queues.
- Remote edges are delegated to the worker network manager.
- The engine only runs subtasks assigned to the current worker; all other subtasks are represented as remote endpoints.

## Operators

`arroyo-operator` provides the core execution abstractions.

Key runtime concepts include:

- source lifecycle and finish behavior
- control messages for checkpoints and stop signals
- checkpoint counters across multi-input operators
- timers and event-time processing
- connector-backed sources and sinks

## Connectors

`arroyo-connectors` is both a planning-time registry and a runtime integration layer.

### Planning-time role

Connectors provide:

- declarative config schemas
- connector discovery for the API/UI
- schema handling
- validation and autocomplete helpers
- optional UDF registration

### Runtime role

Connectors provide actual source/sink behavior for systems such as Kafka, filesystems, MQTT, NATS, RabbitMQ, Redis, SSE, webhook, websocket, preview, and others.

## UDF subsystem

### Components

- `arroyo-udf-plugin` and `arroyo-udf-macros`: authoring support
- `arroyo-udf-common`: shared ABI/contracts
- `arroyo-udf-host`: runtime parsing/loading of compiled artifacts
- `arroyo-compiler-service`: artifact build service

### Lifecycle

1. A UDF is authored and validated through the API/UI.
2. The compiler service builds a shared library artifact.
3. Artifact metadata is persisted by the control plane.
4. The planner includes UDF metadata in the compiled logical program.
5. Workers load and invoke the UDF at runtime.

## State and checkpoints

### State backend

`arroyo-state` owns checkpoint payloads and operator/table state. It currently uses a parquet-backed backend and stores metadata plus table payloads through the configured storage provider.

### Checkpoint lifecycle

1. The active job controller starts a checkpoint.
2. Workers inject checkpoint barriers into the graph.
3. Stateful operators flush table data and checkpoint metadata.
4. Operator checkpoint metadata is persisted.
5. Global checkpoint metadata is finalized.
6. Two-phase-commit sinks may enter a commit stage.
7. Old checkpoints can be compacted and cleaned up.

### Job-controller placement

Checkpoint coordination can happen in two ways:

- controller-owned job controller
- worker-owned leader job controller

The worker-leader mode lets one worker host the job controller while the controller process manages the higher-level job state.

## Recovery

Recovery reuses persisted checkpoint metadata.

At startup a worker can restore a specific epoch, reconstruct operator state from the backing store, and resume execution with the restored minimum epoch and checkpoint metadata.
