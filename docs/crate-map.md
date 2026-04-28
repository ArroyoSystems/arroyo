# Workspace crate map

## Top-level runtime crates

| Crate | Role |
| --- | --- |
| `arroyo` | Main CLI binary and service launcher |
| `arroyo-api` | REST API, OpenAPI docs, UI serving, metadata access |
| `arroyo-controller` | Job orchestration and scheduler integration |
| `arroyo-worker` | Distributed execution runtime |
| `arroyo-compiler-service` | UDF compilation service |
| `arroyo-node` | Node agent for worker process management |
| `arroyo-openapi` | Generated Rust API client |

## Planning and execution crates

| Crate | Role |
| --- | --- |
| `arroyo-planner` | SQL parsing, DataFusion planning, streaming rewrites |
| `arroyo-datastream` | Logical program model and optimizer support |
| `arroyo-operator` | Runtime operator traits and utilities |
| `arroyo-connectors` | Connector registry and implementations |
| `arroyo-formats` | Payload and file format support |

## Shared systems crates

| Crate | Role |
| --- | --- |
| `arroyo-rpc` | gRPC contracts, API types, configuration, client helpers |
| `arroyo-types` | Shared IDs, timestamps, watermarks, metric names |
| `arroyo-server-common` | Logging, TLS, admin server, shutdown, telemetry |
| `arroyo-storage` | Object-store and local filesystem abstraction |
| `arroyo-state` | Checkpoint/state backend and state-table abstractions |
| `arroyo-metrics` | Prometheus metric registration helpers |

## UDF crates

| Crate | Role |
| --- | --- |
| `arroyo-udf-common` | Shared UDF contracts |
| `arroyo-udf-plugin` | User-facing proc-macro support |
| `arroyo-udf-macros` | Internal macro implementation |
| `arroyo-udf-host` | Runtime UDF parsing/loading |
| `arroyo-udf-python` | Python UDF support |

## Test/support crates

| Crate | Role |
| --- | --- |
| `arroyo-sql-testing` | SQL-focused testing helpers |
| `integ` | Integration tests |

## Frontend and deployment assets

| Path | Role |
| --- | --- |
| `webui/` | React/Vite frontend served by `arroyo-api` |
| `docker/` | Container build and runtime defaults |
| `k8s/arroyo/` | Helm chart for Kubernetes deployment |
