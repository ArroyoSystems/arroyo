# Arroyo - Distributed Stream Processing Engine

## Project Overview

Arroyo is a distributed stream processing engine written in Rust that enables real-time data pipeline processing with a SQL-first approach. It processes millions of events per second with subsecond query results, featuring stateful computations with fault-tolerance.

**Version**: 0.16.0-dev
**License**: MIT OR Apache-2.0
**Language**: Rust (edition 2024, Rust 1.92+)

## Architecture

### Control Plane Components
- **API Server** (`crates/arroyo-api/`) - REST API with OpenAPI spec for pipeline management
- **Controller** (`crates/arroyo-controller/`) - Job orchestration, scheduling, and lifecycle management
- **Compiler Service** (`crates/arroyo-compiler-service/`) - Dynamic compilation of Rust/Python UDFs
- **Node Server** (`crates/arroyo-node/`) - Node management and coordination

### Data Plane Components
- **Worker** (`crates/arroyo-worker/`) - Task execution, network management, stream processing engine
- **Operator** (`crates/arroyo-operator/`) - Stream operators (windows, joins, aggregations)
- **State Management** (`crates/arroyo-state/`) - State backends, checkpointing, recovery
- **Storage** (`crates/arroyo-storage/`) - Object storage abstraction (S3, GCS, Azure)

### Query Processing
- **Planner** (`crates/arroyo-planner/`) - SQL parsing, logical/physical planning using DataFusion
- **Datastream** (`crates/arroyo-datastream/`) - Dataflow graph representation and abstractions

### Connectors & Formats
- **Connectors** (`crates/arroyo-connectors/`) - 19 source/sink implementations
- **Formats** (`crates/arroyo-formats/`) - Data serialization (JSON, Avro, Protobuf, Debezium)

### Supporting Components
- **RPC** (`crates/arroyo-rpc/`) - gRPC protocol definitions, configuration system
- **Types** (`crates/arroyo-types/`) - Core type definitions (WorkerId, timestamps)
- **Metrics** (`crates/arroyo-metrics/`) - Prometheus metrics collection
- **UDF System** (`crates/arroyo-udf/`) - User-defined functions in Rust and Python

## Technology Stack

### Core Dependencies
- **Apache Arrow** (v55.2.0) - Columnar data format
- **DataFusion** (v48.0.1) - Query planning and execution engine
- **Tokio** - Async runtime for concurrent operations
- **Tonic** (v0.13) - gRPC framework for inter-service communication
- **Axum** (v0.7) - HTTP web framework for API server
- **Petgraph** - Graph data structures for dataflow graphs
- **Prometheus** - Metrics collection and monitoring

### Frontend
- **React 18** with **TypeScript**
- **Vite** - Build tool and dev server
- **Chakra UI** - Component library
- **Monaco Editor** - SQL editor with syntax highlighting
- **ReactFlow** - Visual pipeline graph rendering
- **AG Grid** - Data tables and result display

### Infrastructure
- **Database**: PostgreSQL (production) or SQLite (development)
- **Deployment**: Docker, Kubernetes with Helm charts
- **Storage**: Object storage backends (S3, GCS, Azure Blob)

## Key Directories

```
arroyo/
├── crates/              # Rust workspace with 29 member crates
│   ├── arroyo/          # Main binary & CLI entry point
│   ├── arroyo-api/      # REST API server
│   ├── arroyo-controller/   # Job orchestration
│   ├── arroyo-worker/   # Stream processing workers
│   ├── arroyo-planner/  # SQL query planning
│   ├── arroyo-compiler-service/  # UDF compilation
│   ├── arroyo-connectors/    # Data source/sink connectors
│   ├── arroyo-operator/ # Stream operators
│   ├── arroyo-state/    # State management
│   ├── arroyo-storage/  # Object storage
│   ├── arroyo-formats/  # Data format handling
│   ├── arroyo-udf/      # User-defined functions
│   └── integ/           # Integration tests
├── webui/               # React TypeScript web interface
│   ├── src/
│   │   ├── components/  # Reusable UI components
│   │   ├── routes/      # Page routes and layouts
│   │   └── gen/         # Generated API client types
├── docker/              # Docker build configurations
├── k8s/                 # Kubernetes Helm chart
└── .github/workflows/   # CI/CD pipelines
```

## Main Entry Points

### CLI Binary
**File**: `crates/arroyo/src/main.rs`

**Available Commands**:
- `cluster` - Start complete Arroyo cluster (all-in-one)
- `api` - Start API server
- `controller` - Start controller service
- `worker` - Start worker node
- `compiler` - Start compiler service
- `node` - Start node server
- `run` - Run local pipeline
- `migrate` - Database migrations
- `visualize` - Query plan visualization

### Key Implementation Files
- `crates/arroyo-planner/src/lib.rs` (1,172 lines) - SQL query planning logic
- `crates/arroyo-operator/src/operator.rs` (1,503 lines) - Stream operator implementations
- `crates/arroyo-worker/src/engine.rs` (912 lines) - Worker execution engine
- `crates/arroyo-controller/src/lib.rs` (739 lines) - Job orchestration
- `crates/arroyo-api/src/pipelines.rs` (1,003 lines) - Pipeline management API
- `crates/arroyo-storage/src/lib.rs` (1,258 lines) - Storage backend implementations

## Stream Processing Features

### Window Operations
- **Tumbling Windows** - Fixed-size, non-overlapping time windows
- **Sliding Windows** - Overlapping time windows with configurable slide interval
- **Session Windows** - Dynamic windows based on activity gaps

### Join Operations
- Inner, left, right, and full outer joins
- Temporal join semantics for streaming data
- State-backed join implementations

### State Management
- Parquet-based state checkpointing
- Exactly-once processing semantics
- State persistence to object stores (S3, GCS, Azure)
- Fault-tolerant recovery from checkpoints

### Aggregations
- Standard SQL aggregations (SUM, COUNT, AVG, MIN, MAX)
- Custom user-defined aggregate functions (UDAFs)
- Windowed aggregations

## Supported Connectors (19 total)

### Messaging Systems
- Kafka, Confluent Cloud, Amazon Kinesis
- RabbitMQ, Redis, MQTT, NATS, Fluvio

### File Systems & Lakes
- Amazon S3, Google Cloud Storage, Azure Blob Storage
- Local File System
- Delta Lake, Apache Iceberg

### Streaming Protocols
- Server-Sent Events (SSE)
- WebSocket
- Webhook
- Polling HTTP

### Testing & Development
- Impulse (synthetic data generation)
- Nexmark (standard streaming benchmark)
- Preview (UI preview)
- Blackhole (null sink)
- stdout (console output)

## Data Formats

### Supported Serialization
- **JSON** - Standard JSON with schema inference
- **Avro** - Apache Avro with schema registry support
- **Protobuf** - Protocol Buffers
- **Debezium JSON** - Change Data Capture (CDC) format

### Format Handling
- `crates/arroyo-formats/src/de.rs` (1,048 lines) - Deserialization logic
- `crates/arroyo-formats/src/ser.rs` (623 lines) - Serialization logic

## User-Defined Functions (UDFs)

### Rust UDFs
- Compiled dynamically via compiler service
- Macro-based definition (`arroyo-udf-macros`)
- Full async/await support
- Type-safe integration with Arrow

### Python UDFs (Optional Feature)
- Enabled with `python-udf` feature flag
- Python 3 runtime integration
- PyO3 bindings for Rust interop

### UDF Components
- `arroyo-udf-common` - Shared UDF types and traits
- `arroyo-udf-host` - UDF hosting and execution
- `arroyo-udf-macros` - Proc macros for UDF definitions
- `arroyo-udf-plugin` - Plugin system for dynamic loading
- `arroyo-udf-python` - Python UDF runtime

## Development Workflow

### Building
```bash
cargo build --release
```

### Running Locally
```bash
# Start full cluster
cargo run -- cluster

# Run specific component
cargo run -- api
cargo run -- controller
cargo run -- worker
```

### Testing
```bash
# Run all tests
cargo test

# SQL integration tests
cargo test -p arroyo-sql-testing

# Integration tests
cargo test -p integ
```

### Frontend Development
```bash
cd webui
pnpm install
pnpm dev
```

## Configuration

### Database Setup
- **PostgreSQL**: Production deployments
- **SQLite**: Local development (default)
- Migrations managed via `migrate` command

### Environment Variables
Configuration managed through `arroyo-rpc/src/config.rs` (1,199 lines)

### Kubernetes Deployment
Helm chart in `k8s/arroyo/` with configurable:
- Worker replicas and resources
- PostgreSQL backend
- Storage backend configuration
- Metrics and monitoring

## Coding Patterns

### Error Handling
- Result types with custom error enums
- `anyhow` for error context propagation
- Structured error types per crate

### Async Runtime
- Tokio-based async/await throughout
- Tonic for async gRPC services
- Axum for async HTTP handlers

### Data Processing
- Arrow RecordBatch for columnar data
- DataFusion for query execution
- Zero-copy data transfer where possible

### State Management
- Trait-based state backends
- Parquet serialization for checkpoints
- Object store abstraction for portability

### Testing
- Unit tests alongside implementation
- Integration tests in dedicated crates
- SQL golden file testing in `arroyo-sql-testing`

## API Structure

### REST API (`arroyo-api`)
- OpenAPI specification generation
- Pipeline CRUD operations
- Job management and monitoring
- Connection profile management
- UDF management

### gRPC Services (`arroyo-rpc`)
- Controller-Worker communication
- Node registration and heartbeats
- Task assignment and status
- Metrics collection

## Workspace Structure

**Total Crates**: 29
**Total Rust Files**: ~238
**Total TypeScript Files**: ~63

### Build Configuration
- Custom patches for arrow-rs, datafusion, sqlparser-rs (forked repos)
- Release profile with debug symbols enabled
- Optional features: python-udf, profiling with jemalloc

### CI/CD
- **ci.yml** - Build, test, lint for Rust and frontend
- **docker.yaml** - Multi-platform Docker image builds
- **binaries.yml** - Release binary artifacts
- **semgrep.yml** - Security scanning

## Use Cases

### Real-Time Analytics
- Fraud detection and security incident detection
- Real-time product and business analytics
- Live dashboards and monitoring

### Data Pipeline
- Real-time data warehouse/lake ingestion
- ETL/ELT stream processing
- Data transformation and enrichment

### Machine Learning
- Real-time ML feature generation
- Online model inference
- Anomaly detection

## Deployment Options

1. **Single Binary** - `arroyo cluster` runs all components
2. **Docker** - Official images on ghcr.io/arroyo-systems/arroyo
3. **Kubernetes** - Helm chart with PostgreSQL dependency
4. **Managed Service** - Cloudflare Pipelines (beta)

## Observability

### Metrics
- Prometheus metrics collection
- Per-operator metrics (throughput, latency)
- System metrics (CPU, memory, network)

### Logging
- Structured logging with JSON output
- Configurable log levels per component
- Distributed tracing support

### Monitoring
- Web UI with real-time job monitoring
- Admin endpoints on workers/services
- GraphQL API for metrics queries

## Community

- **GitHub**: https://github.com/ArroyoSystems/arroyo
- **Discord**: Active community for support and discussion
- **Documentation**: Comprehensive docs and guides
- **Contributing**: Open to community contributions (see CONTRIBUTING.md)

---

*This file was generated to help AI assistants understand the Arroyo codebase structure and should be kept up to date as the project evolves.*
