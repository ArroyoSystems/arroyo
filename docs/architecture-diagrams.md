# Arroyo Architecture Diagrams

A comprehensive visual guide to how the Arroyo distributed stream processing engine works.

---

## 1. System Architecture

The overall topology of Arroyo services, their communication protocols, and external dependencies.

```mermaid
graph TB
    subgraph Clients
        WebUI["Web UI<br/>(React SPA)"]
        REST["REST / CLI Client"]
    end

    subgraph ControlPlane["Control Plane"]
        API["API Server<br/><i>arroyo-api</i><br/>HTTP :5115"]
        Controller["Controller<br/><i>arroyo-controller</i><br/>gRPC :5116"]
        Compiler["Compiler Service<br/><i>arroyo-compiler-service</i><br/>gRPC :5117"]
    end

    subgraph DataPlane["Data Plane"]
        Node1["Node Server<br/><i>arroyo-node</i><br/>gRPC :5118"]
        Node2["Node Server<br/><i>arroyo-node</i><br/>gRPC :5118"]
        W1["Worker 1<br/><i>arroyo-worker</i><br/>gRPC + TCP"]
        W2["Worker 2<br/><i>arroyo-worker</i><br/>gRPC + TCP"]
        W3["Worker 3<br/><i>arroyo-worker</i><br/>gRPC + TCP"]
    end

    subgraph ExternalSystems["External Systems"]
        Kafka["Kafka / Kinesis /<br/>MQTT / NATS / ..."]
        Sinks["S3 / Delta Lake /<br/>Iceberg / Redis / ..."]
    end

    DB[("PostgreSQL<br/>or SQLite")]
    ObjStore[("Object Storage<br/>S3 / GCS / Azure / Local<br/><i>arroyo-storage</i>")]

    %% Client connections
    WebUI -->|"HTTP/REST"| API
    REST -->|"HTTP/REST"| API

    %% API connections
    API -->|"gRPC: BuildUdf"| Compiler
    API -->|"gRPC: SubscribeToOutput,<br/>JobMetrics"| Controller
    API -->|"SQL read/write"| DB

    %% Controller connections
    Controller -->|"SQL read/write<br/>(polls every 500ms)"| DB
    Controller -->|"gRPC: StartExecution,<br/>Checkpoint, Commit,<br/>StopExecution, GetMetrics"| W1
    Controller -->|"gRPC: StartExecution,<br/>Checkpoint, Commit"| W2
    Controller -->|"gRPC: StartExecution,<br/>Checkpoint, Commit"| W3
    Controller -->|"gRPC: StartWorker,<br/>StopWorker"| Node1
    Controller -->|"gRPC: StartWorker,<br/>StopWorker"| Node2

    %% Node connections
    Node1 -->|"spawn process"| W1
    Node1 -->|"spawn process"| W2
    Node2 -->|"spawn process"| W3
    Node1 -->|"gRPC: RegisterNode,<br/>HeartbeatNode,<br/>WorkerFinished"| Controller
    Node2 -->|"gRPC: RegisterNode,<br/>HeartbeatNode"| Controller

    %% Worker connections
    W1 -->|"gRPC: RegisterWorker,<br/>Heartbeat, TaskStarted,<br/>TaskCheckpoint*,<br/>TaskFailed"| Controller
    W2 -->|"gRPC: RegisterWorker,<br/>Heartbeat, ..."| Controller
    W3 -->|"gRPC: RegisterWorker,<br/>Heartbeat, ..."| Controller
    W1 <-->|"TCP: Arrow IPC<br/>(shuffle data)"| W2
    W2 <-->|"TCP: Arrow IPC<br/>(shuffle data)"| W3
    W1 <-->|"TCP: Arrow IPC"| W3

    %% Storage connections
    W1 -->|"Checkpoint state<br/>(Parquet)"| ObjStore
    W2 -->|"Checkpoint state"| ObjStore
    W3 -->|"Checkpoint state"| ObjStore
    Compiler -->|"Store compiled<br/>UDF dylibs"| ObjStore
    Controller -->|"Read/write<br/>checkpoint metadata"| ObjStore

    %% Data connections
    Kafka -->|"Source data"| W1
    Kafka -->|"Source data"| W2
    W2 -->|"Sink data"| Sinks
    W3 -->|"Sink data"| Sinks

    %% Styling
    classDef control fill:#4A90D9,stroke:#2C5F8A,color:#fff
    classDef data fill:#50B86C,stroke:#2D7A3E,color:#fff
    classDef external fill:#E8A838,stroke:#B07D1E,color:#fff
    classDef storage fill:#9B6DC6,stroke:#6B3F96,color:#fff
    classDef client fill:#E06666,stroke:#A33D3D,color:#fff

    class API,Controller,Compiler control
    class Node1,Node2,W1,W2,W3 data
    class Kafka,Sinks external
    class DB,ObjStore storage
    class WebUI,REST client
```

### Key Communication Patterns

| Path | Protocol | Purpose |
|------|----------|---------|
| Client -> API | HTTP/REST | Pipeline CRUD, job management, UI serving |
| API -> Controller | gRPC | Output subscription, metrics queries |
| API -> Compiler | gRPC | UDF compilation (Rust -> dylib) |
| Controller -> Worker | gRPC | Start execution, checkpoint, commit, stop |
| Worker -> Controller | gRPC | Register, heartbeat, task events, sink data |
| Controller -> Node | gRPC | Start/stop worker processes |
| Node -> Controller | gRPC | Register, heartbeat, worker exit notification |
| Worker <-> Worker | TCP | Arrow IPC data for shuffle/repartition |
| API/Controller -> DB | SQL | Job configs (desired state) and job status (current state) |
| Worker -> Object Storage | Parquet | Checkpoint state persistence |
| Compiler -> Object Storage | Binary | Compiled UDF dylib storage |

### Deployment Modes

The `arroyo` binary supports four scheduler modes:

| Mode | Command | How Workers Run |
|------|---------|-----------------|
| **Embedded** | `arroyo cluster` | In-process as Tokio tasks (single binary) |
| **Process** | API + Controller separate | Forked as child processes on same machine |
| **Node** | + Node servers | Distributed across registered Node servers |
| **Kubernetes** | K8s deployment | As Kubernetes Job resources (pods) |

---

## 2. Job Lifecycle State Machine

The Controller manages each job through a compile-time-enforced state machine defined in `crates/arroyo-controller/src/states/`.

```mermaid
stateDiagram-v2
    [*] --> Created : Job inserted into DB

    Created --> Compiling : Immediate

    Compiling --> Scheduling : Program ready

    Scheduling --> Running : All workers connected,<br/>tasks started,<br/>JobController created

    Running --> CheckpointStopping : stop_mode = checkpoint
    Running --> Stopping : stop_mode = graceful/immediate
    Running --> Rescaling : parallelism changed
    Running --> Recovering : Task failure (retryable)<br/>or worker heartbeat timeout
    Running --> Restarting : restart_nonce changed
    Running --> Finishing : All sources exhausted
    Running --> Failing : Fatal error (non-retryable)

    CheckpointStopping --> Stopped : Final checkpoint complete,<br/>all tasks finished

    Stopping --> Stopped : Workers stopped<br/>(or force-killed on timeout)

    Rescaling --> Scheduling : Checkpoint complete,<br/>workers torn down,<br/>new parallelism set

    Recovering --> Compiling : Cluster torn down,<br/>exponential backoff elapsed

    Restarting --> Scheduling : Safe: checkpoint then restart<br/>Force: kill then restart

    Finishing --> Finished : All remaining tasks complete

    Failing --> Failed : Cleanup complete

    Stopped --> Compiling : stop_mode set to none<br/>(user restarts pipeline)

    Failed --> Compiling : restart_nonce changed<br/>(user requests restart)

    Finished --> [*]
    Stopped --> [*]
    Failed --> [*]

    note right of Scheduling
        1. scheduler.start_workers()
        2. Wait for RegisterWorker RPCs
        3. Connect gRPC clients TO workers
        4. Load checkpoint metadata
        5. Compute task assignments
        6. Send StartExecutionReq to all workers
        7. Wait for TaskStarted from all tasks
    end note

    note right of Running
        Event loop (200ms tick):
        - Process config updates
        - Check worker heartbeats
        - Manage checkpoint cycle
        - Collect metrics
    end note

    note right of Recovering
        Retry with exponential backoff:
        base * 2^attempts
        (500ms to 60s default)
        Max 20 restarts
    end note
```

### State Descriptions

| State | File | What Happens |
|-------|------|-------------|
| **Created** | `states/mod.rs:84` | Immediate transition to Compiling |
| **Compiling** | `states/compiling.rs` | Pass-through (compilation happens at API time) |
| **Scheduling** | `states/scheduling.rs` | Start workers, wait for connections, deploy program |
| **Running** | `states/running.rs` | Main event loop: config changes, checkpoints, metrics |
| **CheckpointStopping** | `states/checkpoint_stopping.rs` | Final checkpoint with `then_stop=true` |
| **Stopping** | `states/stopping.rs` | Send StopExecution or force-kill workers |
| **Rescaling** | `states/rescaling.rs` | Checkpoint, tear down, re-schedule with new parallelism |
| **Recovering** | `states/recovering.rs` | Backoff, tear down cluster, restart from last checkpoint |
| **Restarting** | `states/restarting.rs` | User-initiated restart (safe or force) |
| **Finishing** | `states/finishing.rs` | Sources exhausted, wait for remaining tasks |
| **Failing** | `states/failing.rs` | Graceful shutdown before entering Failed |
| **Stopped** | `states/mod.rs:153` | Terminal. Awaits restart command |
| **Failed** | `states/mod.rs:112` | Terminal. Awaits restart command |
| **Finished** | `states/mod.rs:135` | Terminal. Job completed successfully |

### Error Handling

The state machine uses two error types:
- **`FatalError`** -- Immediately transitions to `Failing -> Failed`
- **`RetryableError`** -- Transitions to `Recovering`, which applies exponential backoff and retries up to `allowed_restarts` (default: 20) times

---

## 3. SQL-to-Execution Pipeline

How a SQL query is transformed into a running distributed dataflow.

```mermaid
flowchart TB
    subgraph Stage1["Stage 1: SQL Parsing"]
        SQL["SQL Text<br/><code>CREATE TABLE ... INSERT INTO ... SELECT ...</code>"]
        Parser["sqlparser-rs<br/>with ArroyoDialect"]
        AST["SQL AST<br/>(Vec&lt;Statement&gt;)"]
        SQL --> Parser --> AST
    end

    subgraph Stage2["Stage 2: Statement Classification"]
        AST --> Classify{Statement Type?}
        Classify -->|"CREATE TABLE"| DDL["Register ConnectorTable<br/>in SchemaProvider"]
        Classify -->|"CREATE VIEW"| View["Register as<br/>TableFromQuery"]
        Classify -->|"INSERT INTO"| Insert["InsertQuery<br/>(explicit sink)"]
        Classify -->|"SELECT"| Anon["Anonymous<br/>(preview sink)"]
        Classify -->|"SET"| Config["Update planner<br/>config"]
    end

    subgraph Stage3["Stage 3: DataFusion Logical Planning"]
        Insert --> SqlToRel["DataFusion<br/>SqlToRel"]
        Anon --> SqlToRel
        DDL -.->|"tables registered in<br/>ArroyoSchemaProvider"| SqlToRel
        View -.->|"views registered"| SqlToRel
        SqlToRel --> LogPlan["DataFusion<br/>LogicalPlan"]
        LogPlan --> Optimizer["DataFusion Optimizer<br/>(20+ rules: push filters,<br/>simplify exprs, eliminate<br/>joins/unions, etc.)"]
        Optimizer --> OptPlan["Optimized<br/>LogicalPlan"]
    end

    subgraph Stage4["Stage 4: Streaming Rewrite"]
        OptPlan --> Rewriter["ArroyoRewriter<br/>(TreeNodeRewriter)"]

        Rewriter --> |"TableScan"| SrcExt["TableSourceExtension<br/>+ WatermarkNode<br/>+ RemoteTableExtension"]
        Rewriter --> |"Aggregate + window fn"| AggExt["AggregateExtension<br/>(Tumbling/Sliding/Session)"]
        Rewriter --> |"Join"| JoinExt["JoinExtension<br/>+ KeyCalculationExtension"]
        Rewriter --> |"Projection"| ProjExt["ProjectionExtension<br/>(+ async UDF detection)"]

        SrcExt --> StreamPlan["Streaming<br/>LogicalPlan<br/>(with Extension nodes)"]
        AggExt --> StreamPlan
        JoinExt --> StreamPlan
        ProjExt --> StreamPlan

        StreamPlan --> SinkWrap["Wrap in SinkExtension<br/>(+ ToDebezium if CDC)"]
    end

    subgraph Stage5["Stage 5: Graph Construction"]
        SinkWrap --> Visitor["PlanToGraphVisitor<br/>(walks extension tree)"]
        Visitor --> Graph["LogicalGraph<br/>(petgraph DiGraph)"]
        Graph --> Chain["ChainingOptimizer<br/>(merge adjacent Forward<br/>nodes into OperatorChains)"]
        Chain --> OptGraph["Optimized<br/>LogicalGraph"]
    end

    subgraph Stage6["Stage 6: Serialization & Deployment"]
        OptGraph --> Proto["Serialize to<br/>ArrowProgram (protobuf)"]
        Proto --> DBStore[("Store in DB<br/>(pipeline row)")]
        DBStore --> CtrlPoll["Controller polls DB<br/>(every 500ms)"]
        CtrlPoll --> Deploy["Send StartExecutionReq<br/>to each Worker via gRPC"]
        Deploy --> Deser["Worker deserializes<br/>ArrowProgram"]
        Deser --> PhysGraph["Expand to physical graph<br/>(parallelism instances)"]
        PhysGraph --> Exec["Schedule operator tasks<br/>on Tokio runtime"]
    end

    classDef stage fill:#f0f0f0,stroke:#999
    classDef action fill:#4A90D9,stroke:#2C5F8A,color:#fff
    classDef data fill:#50B86C,stroke:#2D7A3E,color:#fff
    classDef storage fill:#9B6DC6,stroke:#6B3F96,color:#fff

    class Parser,SqlToRel,Optimizer,Rewriter,Visitor,Chain action
    class AST,LogPlan,OptPlan,StreamPlan,Graph,OptGraph,Proto data
    class DBStore,CtrlPoll storage
```

### Concrete Example

Given this SQL:
```sql
CREATE TABLE clicks (user_id TEXT, url TEXT, ts TIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL '5 seconds')
  WITH (connector = 'kafka', topic = 'clicks', format = 'json');

CREATE TABLE click_counts (user_id TEXT, window_start TIMESTAMP, cnt BIGINT)
  WITH (connector = 'kafka', topic = 'click_counts', format = 'json');

INSERT INTO click_counts
  SELECT user_id, window_start, count(*) as cnt
  FROM tumble(clicks, interval '1 minute')
  GROUP BY user_id, window_start;
```

The resulting dataflow graph after all transformations:

```mermaid
graph LR
    S["ConnectorSource<br/><b>Kafka: clicks</b><br/>parallelism=4"]
    V["ArrowValue<br/><b>deserialize + project</b>"]
    WM["ExpressionWatermark<br/><b>ts - 5s</b>"]
    K["ArrowKey<br/><b>hash(user_id)</b>"]
    AGG["TumblingWindowAggregate<br/><b>tumble(1 min), count(*)</b><br/>parallelism=4"]
    P["Projection<br/><b>select output cols</b>"]
    SINK["ConnectorSink<br/><b>Kafka: click_counts</b><br/>parallelism=4"]

    S -->|"Forward"| V
    V -->|"Forward"| WM
    WM -->|"Forward"| K
    K -->|"Shuffle<br/>(hash partition<br/>by user_id)"| AGG
    AGG -->|"Forward"| P
    P -->|"Forward"| SINK

    classDef source fill:#50B86C,stroke:#2D7A3E,color:#fff
    classDef op fill:#4A90D9,stroke:#2C5F8A,color:#fff
    classDef sink fill:#E06666,stroke:#A33D3D,color:#fff

    class S source
    class V,WM,K,AGG,P op
    class SINK sink
```

After **chaining optimization**, adjacent Forward nodes with the same parallelism are merged:

```mermaid
graph LR
    C1["<b>Chain 1</b><br/>ConnectorSource(Kafka)<br/>+ ArrowValue<br/>+ ExpressionWatermark<br/>+ ArrowKey"]
    C2["<b>Chain 2</b><br/>TumblingWindowAggregate<br/>+ Projection<br/>+ ConnectorSink(Kafka)"]

    C1 -->|"Shuffle<br/>(hash by user_id)"| C2

    classDef chain fill:#4A90D9,stroke:#2C5F8A,color:#fff
    class C1,C2 chain
```

---

## 4. Data Flow Through Operators

How Arrow RecordBatches move through the system at runtime.

```mermaid
flowchart TB
    subgraph SourceTask["Source Task (Tokio task)"]
        Connector["Source Connector<br/>(e.g. KafkaSource)"]
        Deser["ArrowDeserializer<br/>(JSON/Avro/Protobuf<br/>-> RecordBatch)"]
        SrcCollector["SourceCollector"]
        Connector -->|"raw bytes"| Deser
        Deser -->|"RecordBatch"| SrcCollector
    end

    subgraph Channels["Inter-Operator Channels"]
        direction TB
        LocalQ["<b>Local: BatchSender/Receiver</b><br/>bounded by row count<br/>(config: queue_size = 8192)<br/>in-process async channel"]
        RemoteQ["<b>Remote: NetworkManager</b><br/>TCP with Arrow IPC wire format<br/>Header(24B) + Payload<br/>100ms flush interval"]
    end

    subgraph OperatorTask["Chained Operator Task (Tokio task)"]
        InQ["InQReader<br/>(FuturesUnordered:<br/>merges N input streams)"]
        Select["tokio::select!<br/>- Data/Signal from InQ<br/>- ControlMessage from ctrl_rx<br/>- Operator future<br/>- Timer tick"]

        subgraph OpChain["Operator Chain (fused)"]
            Op1["Operator 1<br/>(e.g. WatermarkGen)"]
            Op2["Operator 2<br/>(e.g. Filter)"]
            Op3["Operator 3<br/>(e.g. Projection)"]
            Op1 -->|"ChainedCollector"| Op2
            Op2 -->|"ChainedCollector"| Op3
        end

        Collect["ArrowCollector"]

        InQ --> Select
        Select -->|"ArrowMessage::Data"| Op1
        Op3 --> Collect
    end

    subgraph Repartition["Output Routing (ArrowCollector)"]
        HasKey{"Has routing<br/>keys?"}
        Hash["Hash key columns<br/>(create_hashes)"]
        Partition["Map hash -> partition<br/>(server_for_hash)"]
        Sort["Sort + slice into<br/>per-partition sub-batches"]
        RoundRobin["Round-robin<br/>distribution"]

        HasKey -->|"Yes (Shuffle)"| Hash --> Partition --> Sort
        HasKey -->|"No (Forward)"| RoundRobin
    end

    subgraph SinkTask["Sink Task"]
        SinkOp["Sink Connector<br/>(e.g. KafkaSink)"]
        Ser["ArrowSerializer<br/>(RecordBatch -><br/>JSON/Avro/Protobuf)"]
        SinkOp --> Ser
    end

    SrcCollector --> LocalQ
    SrcCollector --> RemoteQ
    LocalQ --> InQ
    RemoteQ -->|"deserialize<br/>Arrow IPC"| InQ
    Collect --> HasKey
    Sort --> LocalQ
    Sort --> RemoteQ
    RoundRobin --> LocalQ

    classDef source fill:#50B86C,stroke:#2D7A3E,color:#fff
    classDef channel fill:#E8A838,stroke:#B07D1E,color:#fff
    classDef operator fill:#4A90D9,stroke:#2C5F8A,color:#fff
    classDef sink fill:#E06666,stroke:#A33D3D,color:#fff
    classDef routing fill:#9B6DC6,stroke:#6B3F96,color:#fff

    class Connector,Deser,SrcCollector source
    class LocalQ,RemoteQ channel
    class InQ,Select,Op1,Op2,Op3,Collect operator
    class HasKey,Hash,Partition,Sort,RoundRobin routing
    class SinkOp,Ser sink
```

### Signal Messages (in-band with data)

Signals flow through the same channels as data, using `ArrowMessage::Signal`:

```mermaid
flowchart LR
    subgraph Signals["Signal Types"]
        B["Barrier<br/>(epoch, min_epoch,<br/>timestamp, then_stop)"]
        W["Watermark<br/>(EventTime | Idle)"]
        S["Stop"]
        E["EndOfData"]
    end

    subgraph Processing["How Operators Handle Signals"]
        BA["<b>Barrier Alignment</b><br/>CheckpointCounter tracks<br/>which inputs sent barrier.<br/>Block early inputs until<br/>all inputs report."]
        WH["<b>Watermark Holder</b><br/>Track min watermark<br/>across all inputs.<br/>Advance only when<br/>all inputs report."]
        ST["<b>Stop Tracking</b><br/>Count per-input.<br/>Act only when all<br/>inputs are closed."]
    end

    B --> BA
    W --> WH
    S --> ST
    E --> ST
```

### Physical Graph Expansion

A logical node with parallelism=3 becomes 3 physical subtask instances:

```mermaid
graph TB
    subgraph Logical["Logical Graph"]
        LN1["Node A<br/>parallelism=3"]
        LN2["Node B<br/>parallelism=3"]
        LN1 -->|"Shuffle"| LN2
    end

    subgraph Physical["Physical Graph (per-worker)"]
        A0["A[0]"]
        A1["A[1]"]
        A2["A[2]"]
        B0["B[0]"]
        B1["B[1]"]
        B2["B[2]"]
        A0 --> B0
        A0 --> B1
        A0 --> B2
        A1 --> B0
        A1 --> B1
        A1 --> B2
        A2 --> B0
        A2 --> B1
        A2 --> B2
    end

    Logical -.->|"expand"| Physical

    classDef logical fill:#E8A838,stroke:#B07D1E,color:#fff
    classDef physical fill:#4A90D9,stroke:#2C5F8A,color:#fff
    class LN1,LN2 logical
    class A0,A1,A2,B0,B1,B2 physical
```

For **Forward** edges, the mapping is 1:1 (A[0]->B[0], A[1]->B[1], etc.) and requires equal parallelism. For **Shuffle** edges, every source subtask connects to every destination subtask (N x M full connectivity).

---

## 5. Checkpointing Flow

Arroyo implements Chandy-Lamport distributed snapshots with barrier alignment and optional two-phase commit for exactly-once sinks.

```mermaid
sequenceDiagram
    participant Ctrl as Controller<br/>(JobController)
    participant Src as Source<br/>Worker 1
    participant Op as Operator<br/>Worker 1
    participant Op2 as Operator<br/>Worker 2
    participant Sink as Sink<br/>Worker 2
    participant Store as Object Storage
    participant DB as Database

    Note over Ctrl: checkpoint_interval elapsed

    rect rgb(230, 240, 255)
    Note right of Ctrl: Phase 1: Initiate
    Ctrl->>DB: INSERT checkpoint (epoch=N, state=inprogress)
    Ctrl->>Src: CheckpointReq(epoch=N, min_epoch, then_stop?)
    end

    rect rgb(230, 255, 230)
    Note right of Src: Phase 2: Barriers flow through dataflow
    Src->>Ctrl: TaskCheckpointEvent(AlignmentStarted)
    Src->>Src: table_manager.checkpoint()
    Src->>Ctrl: TaskCheckpointEvent(StartedCheckpointing)
    Src-->>Store: Write state (Parquet files)
    Src->>Ctrl: TaskCheckpointEvent(FinishedSync)
    Src->>Op: Barrier(epoch=N) [in-band with data]

    Note over Op: Barrier Alignment:<br/>block inputs that sent barrier,<br/>wait for all inputs
    Op->>Ctrl: TaskCheckpointEvent(AlignmentStarted)
    Op->>Op: handle_checkpoint() + table_manager.checkpoint()
    Op->>Ctrl: TaskCheckpointEvent(StartedCheckpointing)
    Op-->>Store: Write state (Parquet files)
    Op->>Ctrl: TaskCheckpointEvent(FinishedSync)
    Op->>Op2: Barrier(epoch=N) [via NetworkManager TCP]

    Op2->>Ctrl: TaskCheckpointEvent(AlignmentStarted)
    Op2->>Op2: handle_checkpoint() + table_manager.checkpoint()
    Op2-->>Store: Write state (Parquet files)
    Op2->>Ctrl: TaskCheckpointEvent(FinishedSync)
    Op2->>Sink: Barrier(epoch=N)

    Sink->>Sink: handle_checkpoint() + table_manager.checkpoint()
    Sink-->>Store: Write state (Parquet files)
    Sink->>Ctrl: TaskCheckpointCompleted(metadata, bytes, watermark)
    end

    rect rgb(255, 245, 230)
    Note right of Ctrl: Phase 3: Metadata Aggregation
    Note over Ctrl: All subtasks for operator complete
    Ctrl-->>Store: Write OperatorCheckpointMetadata
    Note over Ctrl: All operators complete
    Ctrl-->>Store: Write CheckpointMetadata
    end

    rect rgb(255, 230, 230)
    Note right of Ctrl: Phase 4: Two-Phase Commit (if needed)
    Ctrl->>Sink: CommitReq(epoch=N, commit_data)
    Sink->>Sink: Commit to external system<br/>(e.g. Iceberg, Delta Lake)
    Sink->>Ctrl: TaskCheckpointEvent(FinishedCommit)
    end

    Ctrl->>DB: UPDATE checkpoint SET state=ready
    Note over Ctrl: Compact old state (optional)
```

### Checkpoint Storage Layout

```
{checkpoint_url}/{job_id}/checkpoints/
    checkpoint-0000001/
        metadata                              # CheckpointMetadata (protobuf)
        operator-{id}/
            metadata                          # OperatorCheckpointMetadata
            table-{name}-000                  # Parquet state file (subtask 0)
            table-{name}-001                  # Parquet state file (subtask 1)
            table-{name}-000-compacted        # Compacted state file
    checkpoint-0000002/
        ...
```

### State Table Types

| Table Type | Use Case | State Structure |
|-----------|----------|-----------------|
| **GlobalKeyedTable** | Connector offsets, simple KV | `HashMap<K, V>` serialized via bincode |
| **ExpiringTimeKeyTable** | Window aggregations, joins | `RecordBatch` keyed by `(key, timestamp)` with TTL |
| **KeyTimeView** | Join state | Temporal key lookup |
| **UncachedKeyValueView** | On-demand lookups | Read from Parquet on demand |

### Recovery from Checkpoint

```mermaid
flowchart LR
    Fail["Task Failure<br/>or Worker Crash"]
    Recover["Recovering State<br/>(exponential backoff)"]
    Compile["Compiling State"]
    Schedule["Scheduling State<br/>(start new workers)"]
    Load["Workers load<br/>CheckpointMetadata<br/>from Object Storage"]
    Restore["TableManager restores<br/>each table from Parquet"]
    Resume["Resume processing<br/>from checkpoint epoch"]

    Fail --> Recover --> Compile --> Schedule --> Load --> Restore --> Resume

    classDef fail fill:#E06666,stroke:#A33D3D,color:#fff
    classDef recover fill:#E8A838,stroke:#B07D1E,color:#fff
    classDef ok fill:#50B86C,stroke:#2D7A3E,color:#fff

    class Fail fail
    class Recover,Compile,Schedule recover
    class Load,Restore,Resume ok
```

---

## 6. Worker Internal Architecture

The internal structure of a single Arroyo worker process.

```mermaid
graph TB
    subgraph Worker["Worker Process"]
        WS["WorkerServer<br/>(gRPC server)<br/><i>arroyo-worker</i>"]

        subgraph Engine["Engine"]
            Program["Program<br/>(Physical DiGraph)"]

            subgraph Tasks["Operator Tasks (Tokio tasks)"]
                T1["Source Task<br/>(node 0, subtask 0)"]
                T2["Source Task<br/>(node 0, subtask 1)"]
                T3["Operator Task<br/>(node 1, subtask 0)"]
                T4["Operator Task<br/>(node 1, subtask 1)"]
                T5["Sink Task<br/>(node 2, subtask 0)"]
            end

            subgraph StateLayer["State Layer (per-operator)"]
                TM1["TableManager<br/>(tables + caches)"]
                TM2["TableManager"]
                TM3["TableManager"]
                BW1["BackendWriter<br/>(channel sender)"]
                BW2["BackendWriter"]
                BW3["BackendWriter"]
                BF["BackendFlusher<br/>(background Tokio task)<br/>Accumulates writes,<br/>flushes on checkpoint"]
            end
        end

        NM["NetworkManager<br/>(TCP listener + connections)<br/>Arrow IPC wire protocol"]

        subgraph Channels["Internal Channels"]
            DataCh["BatchSender/Receiver<br/>(row-count bounded)"]
            CtrlIn["ctrl_rx: ControlMessage<br/>(Checkpoint, Stop,<br/>Commit, LoadCompacted)"]
            CtrlOut["ctrl_tx: ControlResp<br/>(CheckpointEvent,<br/>TaskStarted/Failed/Finished)"]
        end
    end

    subgraph External["External Connections"]
        CtrlGRPC["Controller<br/>(gRPC)"]
        OtherW["Other Workers<br/>(TCP)"]
        Storage["Object Storage"]
        Sources["External Sources<br/>(Kafka, Kinesis, ...)"]
        SinkDest["External Sinks<br/>(Kafka, S3, ...)"]
    end

    %% gRPC connections
    CtrlGRPC -->|"StartExecution,<br/>Checkpoint, Commit"| WS
    WS -->|"RegisterWorker,<br/>TaskCheckpoint*,<br/>Heartbeat"| CtrlGRPC

    %% Worker server to engine
    WS -->|"deploy program"| Program
    Program -->|"instantiate"| Tasks

    %% Tasks to state
    T3 --> TM1
    T4 --> TM2
    T5 --> TM3
    TM1 --> BW1
    TM2 --> BW2
    TM3 --> BW3
    BW1 --> BF
    BW2 --> BF
    BW3 --> BF
    BF -->|"Parquet files"| Storage

    %% Data flow
    Sources --> T1
    Sources --> T2
    T1 --> DataCh
    T2 --> DataCh
    DataCh --> T3
    DataCh --> T4
    T3 --> DataCh
    T4 --> DataCh
    DataCh --> T5
    T5 --> SinkDest

    %% Network
    NM <-->|"Arrow IPC<br/>(shuffle)"| OtherW
    NM --> DataCh
    DataCh --> NM

    %% Control channels
    WS --> CtrlIn
    CtrlIn --> Tasks
    Tasks --> CtrlOut
    CtrlOut --> WS

    classDef server fill:#4A90D9,stroke:#2C5F8A,color:#fff
    classDef task fill:#50B86C,stroke:#2D7A3E,color:#fff
    classDef state fill:#9B6DC6,stroke:#6B3F96,color:#fff
    classDef channel fill:#E8A838,stroke:#B07D1E,color:#fff
    classDef external fill:#ccc,stroke:#999

    class WS,NM server
    class T1,T2,T3,T4,T5 task
    class TM1,TM2,TM3,BW1,BW2,BW3,BF state
    class DataCh,CtrlIn,CtrlOut channel
    class CtrlGRPC,OtherW,Storage,Sources,SinkDest external
```

### Key Internal Components

| Component | Responsibility |
|-----------|---------------|
| **WorkerServer** | gRPC server handling controller commands; manages Engine lifecycle |
| **Engine** | Converts `LogicalProgram` -> physical graph, spawns operator tasks |
| **Program** | Holds the `DiGraph<SubtaskOrQueueNode, PhysicalGraphEdge>` |
| **NetworkManager** | TCP connections to other workers for shuffle data transfer |
| **TableManager** | Per-operator state API: typed access to GlobalKeyed, ExpiringTimeKey tables |
| **BackendWriter** | Channel sender for async state writes |
| **BackendFlusher** | Background task: accumulates state writes, flushes Parquet to storage on checkpoint |
| **InQReader** | `FuturesUnordered`-based stream combiner for multiple input queues |
| **ArrowCollector** | Output interface: handles repartitioning/shuffle for downstream operators |
| **CheckpointCounter** | Tracks barrier alignment across multiple input partitions |

### Task Execution Loop

Each non-source operator task runs this core loop:

```mermaid
flowchart TB
    Start["tokio::select!"]
    Start --> A{"ctrl_rx.recv()"}
    Start --> B{"inq_reader.next()"}
    Start --> C{"operator_future"}
    Start --> D{"interval.tick()"}

    A -->|"Checkpoint"| A1["Forward to<br/>state flusher"]
    A -->|"Commit"| A2["Call operator<br/>handle_commit()"]
    A -->|"Stop"| A3["Graceful shutdown"]
    A -->|"LoadCompacted"| A4["Load compacted<br/>state files"]

    B -->|"Data(RecordBatch)"| B1["process_batch_index()<br/>on chained operator"]
    B -->|"Signal::Barrier"| B2["Barrier alignment<br/>+ checkpoint"]
    B -->|"Signal::Watermark"| B3["Update watermark holder<br/>+ handle_watermark()"]
    B -->|"Signal::Stop"| B4["Track per-input<br/>stop signals"]

    C --> C1["Handle async<br/>operator result"]
    D --> D1["handle_tick()"]

    classDef select fill:#4A90D9,stroke:#2C5F8A,color:#fff
    classDef ctrl fill:#E06666,stroke:#A33D3D,color:#fff
    classDef data fill:#50B86C,stroke:#2D7A3E,color:#fff
    classDef async fill:#9B6DC6,stroke:#6B3F96,color:#fff
    classDef tick fill:#E8A838,stroke:#B07D1E,color:#fff

    class Start select
    class A,A1,A2,A3,A4 ctrl
    class B,B1,B2,B3,B4 data
    class C,C1 async
    class D,D1 tick
```

---

## Appendix: Available Operators

All operator types that can appear in a dataflow graph:

| Operator | Name in Graph | Description | State |
|----------|--------------|-------------|-------|
| **ConnectorSource** | `ConnectorSource` | 19 source connectors (Kafka, Kinesis, S3, etc.) | GlobalKeyed (offsets) |
| **ConnectorSink** | `ConnectorSink` | 19 sink connectors | GlobalKeyed (commit data) |
| **ArrowValue** | `ArrowValue` | Stateless DataFusion plan (filter, map, project) | None |
| **ArrowKey** | `ArrowKey` | Key extraction for shuffle | None |
| **Projection** | `Projection` | Column projection with physical expressions | None |
| **ExpressionWatermark** | `ExpressionWatermark` | Generate watermarks from event data | None |
| **TumblingWindowAggregate** | `TumblingWindowAggregate` | Fixed-size non-overlapping window aggregation | ExpiringTimeKey |
| **SlidingWindowAggregate** | `SlidingWindowAggregate` | Overlapping window aggregation | ExpiringTimeKey |
| **SessionWindowAggregate** | `SessionWindowAggregate` | Gap-based session window aggregation | ExpiringTimeKey |
| **UpdatingAggregate** | `UpdatingAggregate` | Incrementally updating aggregation (CDC output) | ExpiringTimeKey |
| **Join** | `Join` | Stateful streaming join with TTL expiration | 2x ExpiringTimeKey |
| **InstantJoin** | `InstantJoin` | Stateless instant join | None |
| **LookupJoin** | `LookupJoin` | External lookup join | None |
| **WindowFunction** | `WindowFunction` | SQL window functions (ROW_NUMBER, etc.) | ExpiringTimeKey |
| **AsyncUdf** | `AsyncUdf` | Asynchronous user-defined functions | None |

## Appendix: Supported Connectors

| Connector | Source | Sink | Lookup |
|-----------|--------|------|--------|
| Kafka | Yes | Yes | No |
| Confluent Cloud | Yes | Yes | No |
| Amazon Kinesis | Yes | Yes | No |
| RabbitMQ | Yes | No | No |
| Redis | Yes | Yes | Yes |
| MQTT | Yes | Yes | No |
| NATS | Yes | Yes | No |
| Fluvio | Yes | Yes | No |
| Server-Sent Events | Yes | No | No |
| WebSocket | Yes | Yes | No |
| Webhook | No | Yes | No |
| Polling HTTP | Yes | No | No |
| Filesystem (S3/GCS/Azure/Local) | Yes | Yes | No |
| Delta Lake | Yes | Yes | No |
| Apache Iceberg | No | Yes | No |
| Impulse (synthetic) | Yes | No | No |
| Nexmark (benchmark) | Yes | No | No |
| Preview (UI) | No | Yes | No |
| Blackhole (null) | No | Yes | No |
| stdout (console) | No | Yes | No |
