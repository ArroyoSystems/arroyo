# FileSystem Sink Redesign - Design Document

## 1. Motivation

### 1.1 Current Problems

The existing `FileSystem Sink` implementation has several issues:

1. **Complex Background Task Pattern:** The sink spawns a separate Tokio task (`AsyncMultipartFileSystemWriter`) that communicates via channels. This creates:
* Difficulty propagating detailed errors back to the operator
* Multiple patterns for async operations (channel sends, channel receives, internal future polling)
* Complex synchronization during checkpoint/commit


2. **Error Handling Limitations:** Errors in the background task often result in panics (`.unwrap()` on channel sends) rather than proper error propagation. This makes it hard to implement sophisticated error categorization (User vs External, Retryable vs NoRetry).
3. **Debugging Difficulty:** The indirection through channels and the background task makes the system hard to reason about and debug.
4. **Future Memory Concerns:** No infrastructure for controlling memory usage when users select high-cardinality partition keys.

### 1.2 Goals

1. **Eliminate background task:** Use `future_to_poll` infrastructure instead
2. **Proper error propagation:** Errors from I/O operations surface through operator methods
3. **Simpler state machine:** Direct method calls instead of message passing
4. **Extensibility for memory management:** Build infrastructure that can support memory pressure handling in the future
5. **Single-part optimization:** Use simple PUT for small files that don't need multipart

### 1.3 Non-Goals

* Memory pressure handling (deferred to future work)
* Changes to `LocalFileSystemWriter` (keeps using `TwoPhaseCommitter`)
* Changes to table format commit logic (Delta/Iceberg)
* Backwards compatibility with old checkpoint format

---

## 2. Architecture Overview

### 2.1 Current Architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│ TwoPhaseCommitterOperator<FileSystemSink>                       │
│    implements ArrowOperator                                     │
└─────────────────────────────────────────────────────────────────┘
           │
           │ delegates via TwoPhaseCommitter trait
           ▼
┌─────────────────────────────────────────────────────────────────┐
│ FileSystemSink                                                  │
│    sender: Sender<FileSystemMessages>                           │
│    checkpoint_receiver: Receiver<CheckpointData>                │
└─────────────────────────────────────────────────────────────────┘
           │
           │ channel communication
           ▼
┌─────────────────────────────────────────────────────────────────┐
│ AsyncMultipartFileSystemWriter (spawned task)                   │
│    - Own event loop with tokio::select!                         │
│    - Manages writers, uploads, rolling policies                 │
│    - FuturesUnordered for upload progress                       │
└─────────────────────────────────────────────────────────────────┘

```

### 2.2 New Architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│ FileSystemSinkOperator                                          │
│    implements ArrowOperator directly                            │
│                                                                 │
│    - partition_writers: HashMap<partition, file_path>           │
│    - open_files: HashMap<file_path, OpenFile>                   │
│    - pending_uploads: Arc<Mutex<FuturesUnordered<...>>>         │
│    - files_to_commit: Vec<FileToCommit>                         │
│    - commit_state: CommitState                                  │
│                                                                 │
│    Uses future_to_poll for upload progress                      │
│    All I/O driven by operator lifecycle methods                 │
└─────────────────────────────────────────────────────────────────┘

```

**Key changes:**

* **No background task:** All state lives in the operator
* **No channels:** Direct method calls and state mutations
* **future_to_poll:** Upload futures polled by operator infrastructure
* **Direct ArrowOperator implementation:** No `TwoPhaseCommitter` indirection for object store sinks

---

## 3. Data Structures

### 3.1 Main Operator

```rust
// sink/operator.rs
pub struct FileSystemSinkOperator<BBW: BatchBufferingWriter> {
    // === Configuration ===
    config: config::FileSystemSink,
    storage_provider: Arc<StorageProvider>,
    format: Format,

    // === Partition Management ===
    /// Maps partition key to current file path for that partition
    active_partitions: HashMap<Option<OwnedRow>, String>,

    /// Maps file path to file state
    open_files: HashMap<String, OpenFile<BBW>>,

    // === Upload Management ===
    /// Pending upload futures, polled via future_to_poll
    /// Arc<Mutex<...>> needed because future_to_poll returns a Send future
    pending_uploads: Arc<Mutex<FuturesUnordered<UploadFuture>>>,

    /// Maximum concurrent uploads (hardcoded to 50 for now)
    max_concurrent_uploads: usize,

    // === Commit State ===
    /// Files that are closed and ready for commit (all parts uploaded)
    files_to_commit: Vec<FileToCommit>,

    /// Table format state (Delta/Iceberg/Vanilla)
    commit_state: CommitState,

    /// Commit strategy for two-phase commit
    commit_strategy: CommitStrategy,

    /// Pre-commits for PerSubtask strategy
    pre_commits: Vec<FileToCommit>,

    // === File Naming ===
    file_index: usize,
    file_naming: NamingConfig,

    // === Schema & Partitioning ===
    schema: ArroyoSchemaRef,
    iceberg_schema: Option<iceberg::spec::SchemaRef>,
    partitioner: Arc<Partitioner>,
    partitioner_mode: PartitionerMode,

    // === Rolling Policies ===
    rolling_policies: Vec<RollingPolicy>,
    watermark: Option<SystemTime>,

    // === Observability ===
    event_logger: FsEventLogger,
}

type UploadFuture = Pin<Box<dyn Future<Output = Result<UploadResult, DataflowError>> + Send>>;

```

### 3.2 Open File State

```rust
// sink/open_file.rs
pub struct OpenFile<BBW: BatchBufferingWriter> {
    /// Object store path for this file
    path: Path,

    /// Buffered batch writer (Parquet or JSON)
    batch_writer: BBW,

    /// Multipart upload ID (None until first part is created)
    /// If None when file closes, we use single PUT optimization
    multipart_id: Option<MultipartId>,

    /// Parts that have completed upload, in order
    completed_parts: Vec<CompletedPart>,

    /// Number of parts currently being uploaded
    in_flight_parts: usize,

    /// Total bytes that will be in the final file
    total_bytes: usize,

    /// File statistics for rolling policies
    stats: FileStats,

    /// Whether close() has been called on this file
    closing: bool,

    /// Iceberg metadata (populated when file closes)
    metadata: Option<IcebergFileMetadata>,
}

pub struct CompletedPart {
    pub part_index: usize,
    pub content_id: String,
}

pub struct FileStats {
    pub bytes_written: usize,
    pub parts_written: usize,
    pub first_write_at: Instant,
    pub last_write_at: Instant,
    pub representative_timestamp: SystemTime,
}

impl<BBW: BatchBufferingWriter> OpenFile<BBW> {
    pub fn new(path: Path, batch_writer: BBW) -> Self { ... }

    /// Add a batch to the buffer
    pub fn add_batch(&mut self, batch: &RecordBatch) { ... }

    /// Check if buffer has enough data for a part upload
    pub fn should_flush_part(&self, target_size: usize) -> bool { ... }

    /// Extract bytes for a part upload
    pub fn take_part_bytes(&mut self, target_size: usize) -> Bytes { ... }

    /// Flush all remaining bytes (for checkpoint or close)
    pub fn flush_all(&mut self) -> (Option<Bytes>, Option<IcebergFileMetadata>) { ... }

    /// Record that a part upload started
    pub fn part_upload_started(&mut self) { ... }

    /// Record that a part upload completed
    pub fn part_upload_completed(&mut self, part: CompletedPart) { ... }

    /// Check if file is ready to finalize (closing and no in-flight parts)
    pub fn ready_to_finalize(&self) -> bool { ... }
}

```

### 3.3 Upload Results

```rust
// sink/uploads.rs

/// Result of an upload operation
pub enum UploadResult {
    /// Multipart upload was initialized
    MultipartInitialized {
        file_path: String,
        multipart_id: MultipartId,
    },

    /// A part was successfully uploaded
    PartUploaded {
        file_path: String,
        part_index: usize,
        part_id: PartId,
    },

    /// Single-file upload completed (no multipart needed)
    SingleFileUploaded {
        file_path: String,
        size: usize,
    },
}

/// Create a future that initializes a multipart upload
pub fn create_multipart_init_future(
    storage: Arc<StorageProvider>,
    path: Path,
) -> UploadFuture { ... }

/// Create a future that uploads a part with retry logic
pub fn create_part_upload_future(
    storage: Arc<StorageProvider>,
    path: Path,
    multipart_id: MultipartId,
    part_index: usize,
    data: Bytes,
) -> UploadFuture { ... }

/// Create a future that uploads a single file (PUT, no multipart)
pub fn create_single_file_upload_future(
    storage: Arc<StorageProvider>,
    path: Path,
    data: Bytes,
) -> UploadFuture { ... }

```

### 3.4 Checkpoint State

```rust
// sink/checkpoint.rs

/// Checkpoint data for the filesystem sink
#[derive(Debug, Clone, Encode, Decode)]
pub struct FileSystemCheckpoint {
    /// Files that are still being written to
    pub open_files: Vec<OpenFileCheckpoint>,

    /// Files that are closed and ready for commit
    pub files_to_commit: Vec<FileToCommit>,

    /// Next file index for naming
    pub file_index: usize,

    /// Delta table version (for Delta Lake)
    pub delta_version: i64,
}

/// Checkpoint data for an open file
#[derive(Debug, Clone, Encode, Decode)]
pub struct OpenFileCheckpoint {
    /// Object store path
    pub path: String,

    /// Multipart upload ID (None if no parts uploaded yet)
    pub multipart_id: Option<String>,

    /// Content IDs of completed parts, in order
    pub completed_parts: Vec<String>,

    /// Bytes buffered but not yet uploaded
    pub trailing_bytes: Vec<u8>,

    /// Total bytes written to this file
    pub size: usize,

    /// Iceberg metadata
    pub metadata: Option<IcebergFileMetadata>,
}

/// A file that is ready to be committed (finalized)
#[derive(Debug, Clone, Encode, Decode)]
pub struct FileToCommit {
    pub path: String,
    pub multipart_id: Option<String>,  // None for single-file uploads
    pub parts: Vec<String>,            // Empty for single-file uploads
    pub size: usize,
    pub metadata: Option<IcebergFileMetadata>,
}

```

---

## 4. ArrowOperator Implementation

### 4.1 Trait Methods Overview

```rust
impl<BBW: BatchBufferingWriter> ArrowOperator for FileSystemSinkOperator<BBW> {
    fn name(&self) -> String;
    fn tables(&self) -> HashMap<String, TableConfig>;
    fn is_committing(&self) -> bool;

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()>;
    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut OperatorContext, collector: &mut dyn Collector) -> DataflowResult<()>;
    async fn handle_tick(&mut self, tick: u64, ctx: &mut OperatorContext, collector: &mut dyn Collector) -> DataflowResult<()>;
    async fn handle_watermark(&mut self, watermark: Watermark, ctx: &mut OperatorContext, collector: &mut dyn Collector) -> DataflowResult<Option<Watermark>>;
    async fn handle_checkpoint(&mut self, barrier: CheckpointBarrier, ctx: &mut OperatorContext, collector: &mut dyn Collector) -> DataflowResult<()>;
    async fn handle_commit(&mut self, epoch: u32, commit_data: &HashMap<String, HashMap<u32, Vec<u8>>>, ctx: &mut OperatorContext) -> DataflowResult<()>;

    fn future_to_poll(&mut self) -> Option<Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>>;
    async fn handle_future_result(&mut self, result: Box<dyn Any + Send>, ctx: &mut OperatorContext, collector: &mut dyn Collector) -> DataflowResult<()>;
}

```

### 4.2 on_start - Initialization and Recovery

```
on_start(ctx):
    1. Initialize storage provider from config

    2. Initialize partitioner from schema

    3. Initialize commit_state:
        - Delta: load_or_create_table()
        - Iceberg: table.load_or_create()
        - Vanilla: no-op

    4. Get recovery data from state table "r":
       recovery_state = ctx.table_manager.get_global_keyed_state("r")
       all_checkpoints = recovery_state.get_all()

    5. Determine max file_index across all recovered data

    6. If task_index == 0:
       // Task 0 recovers all in-progress files
       For each checkpoint in all_checkpoints:
           For each open_file in checkpoint.open_files:
               finish_recovered_file(open_file)

           // Also recover any files_to_commit
           For each file in checkpoint.files_to_commit:
               self.files_to_commit.push(file)

       // Check for interrupted commit
       pre_commit_state = ctx.table_manager.get_global_keyed_state("p")
       self.pre_commits = pre_commit_state.get_all().values()

    7. Initialize rolling_policies from config

    8. Return Ok(())

```

**Helper: finish_recovered_file**

```
finish_recovered_file(checkpoint: OpenFileCheckpoint):
    // Upload trailing bytes if present
    If checkpoint.trailing_bytes is not empty:
        If checkpoint.multipart_id is Some:
            // Add as final part
            part_index = checkpoint.completed_parts.len()
            part_id = storage.add_multipart(..., checkpoint.trailing_bytes).await?
            checkpoint.completed_parts.push(part_id)
        Else:
            // Single file upload
            storage.put(path, checkpoint.trailing_bytes).await?
            Add to files_to_commit as single-file
            Return

    // Create FileToCommit
    files_to_commit.push(FileToCommit {
        path: checkpoint.path,
        multipart_id: checkpoint.multipart_id,
        parts: checkpoint.completed_parts,
        size: checkpoint.size,
        metadata: checkpoint.metadata,
    })

```

### 4.3 process_batch - Data Ingestion

```text
process_batch(batch, ctx, collector):
    1. Partition the batch:
       If partitioner.is_partitioned():
           partitions = partitioner.partition(&batch)?
       Else:
           partitions = [(None, batch)]

    2. For each (partition_key, sub_batch) in partitions:
       a. Get or create file for partition:
           If partition_key not in active_partitions:
               file_path = generate_file_path(partition_key)
               open_file = OpenFile::new(path, BBW::new(...))
               active_partitions[partition_key] = file_path
               open_files[file_path] = open_file

           file_path = active_partitions[partition_key]
           file = open_files[file_path]

       b. Add batch to file buffer:
           file.add_batch(&sub_batch)

       c. Flush parts if buffer is large enough:
           While file.should_flush_part(target_part_size):
               bytes = file.take_part_bytes(target_part_size)
               schedule_part_upload(file_path, bytes)

       d. Check size-based rolling policies:
           If should_roll_file(file, size_policies):
               close_file(partition_key)

    3. Return Ok(())

```

**Helper: schedule_part_upload**

```text
schedule_part_upload(file_path, bytes):
    file = open_files[file_path]

    // Check concurrent upload limit
    uploads = pending_uploads.lock()
    If uploads.len() >= max_concurrent_uploads:
        // Wait for some uploads to complete
        // (handled by future_to_poll/handle_future_result cycle)
        // For now, just add anyway - limit is soft

    If file.multipart_id is None:
        // Need to initialize multipart first
        // Store bytes for after init
        file.pending_first_part = Some(bytes)
        file.part_upload_started()
        future = create_multipart_init_future(storage, path)
    Else:
        part_index = file.completed_parts.len() + file.in_flight_parts
        file.part_upload_started()
        future = create_part_upload_future(storage, path, multipart_id, part_index, bytes)

    uploads.push(future)

```

### 4.4 handle_tick - Time-Based Rolling

```text
handle_tick(tick, ctx, collector):
    1. Collect partitions to roll:
       partitions_to_close = []

       For each (partition, file_path) in active_partitions:
           file = open_files[file_path]

           For each policy in rolling_policies:
               If policy.is_time_based() && policy.should_roll(file.stats, watermark):
                   partitions_to_close.push(partition)
                   Break

    2. Close files:
       For each partition in partitions_to_close:
           close_file(partition)

    3. Return Ok(())

```

### 4.5 handle_watermark - Watermark Processing

```text
handle_watermark(watermark, ctx, collector):
    1. Update self.watermark if EventTime:
       If watermark is EventTime(ts):
           self.watermark = Some(ts)

    2. Check watermark-based rolling:
       partitions_to_close = []

       For each (partition, file_path) in active_partitions:
           file = open_files[file_path]

           If watermark_policy.should_roll(file.stats, self.watermark):
               partitions_to_close.push(partition)

       For each partition in partitions_to_close:
           close_file(partition)

    3. Return Ok(Some(watermark))  // Pass through

```

### 4.6 future_to_poll - Upload Progress

```rust
fn future_to_poll(&mut self) -> Option<Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>> {
    let futures = self.pending_uploads.clone();

    Some(Box::pin(async move {
        let mut guard = futures.lock().await;

        if guard.is_empty() {
            // No pending uploads - wait indefinitely
            // Will be cancelled when checkpoint/new batch arrives
            drop(guard);
            futures::future::pending::<()>().await;
            unreachable!()
        }

        // Poll the next upload to completion
        let result: Option<Result<UploadResult, DataflowError>> = guard.next().await;
        Box::new(result) as Box<dyn Any + Send>
    }))
}

```

### 4.7 handle_future_result - Upload Completion

```text
handle_future_result(result, ctx, collector):
    1. Downcast result:
       result: Option<Result<UploadResult, DataflowError>> = result.downcast()?

    2. Handle None (shouldn't happen):
       If result is None:
           Return Ok(())  // FuturesUnordered was empty

    3. Handle error:
       If result is Err(e):
           Return Err(e)  // Propagate error up

    4. Handle success:
       Match result.unwrap():
           MultipartInitialized { file_path, multipart_id }:
               file = open_files.get_mut(file_path)?
               file.multipart_id = Some(multipart_id)

               // Upload the pending first part
               If file.pending_first_part is Some(bytes):
                   part_index = 0
                   future = create_part_upload_future(...)
                   pending_uploads.lock().push(future)
                   file.pending_first_part = None

           PartUploaded { file_path, part_index, part_id }:
               file = open_files.get_mut(file_path)?
               file.part_upload_completed(CompletedPart { part_index, content_id: part_id.content_id })

               // Check if file is ready to finalize
               If file.ready_to_finalize():
                   finalize_file(file_path)

           SingleFileUploaded { file_path, size }:
               // File already added to files_to_commit in close_file
               // Just remove from open_files
               open_files.remove(file_path)

    5. Return Ok(())

```

### 4.8 handle_checkpoint - Checkpoint Processing

```text
handle_checkpoint(barrier, ctx, collector):
    1. If stopping, close all open files:
       If barrier.then_stop:
           For each partition in active_partitions.keys():
               close_file(partition)

    2. Flush all open files (get trailing bytes):
       For each (file_path, file) in open_files:
           If not file.closing:
               // Flush to get trailing bytes, but don't close
               file.flush_row_group()  // Creates trailing bytes

    3. Wait for all pending uploads to complete:
       loop:
           uploads = pending_uploads.lock()
           If uploads.is_empty():
               Break

           // Poll next upload
           result = uploads.next().await
           drop(uploads)  // Release lock

           // Process result (same logic as handle_future_result)
           process_upload_result(result)?

    4. Build checkpoint state:
       checkpoint = FileSystemCheckpoint {
           open_files: [],
           files_to_commit: self.files_to_commit.clone(),
           file_index: self.file_index,
           delta_version: get_delta_version(),
       }

       For each (file_path, file) in open_files:
           If not file.closing:
               (trailing_bytes, metadata) = file.get_trailing_bytes()

               checkpoint.open_files.push(OpenFileCheckpoint {
                   path: file_path,
                   multipart_id: file.multipart_id,
                   completed_parts: file.completed_parts.iter().map(|p| p.content_id).collect(),
                   trailing_bytes: trailing_bytes.unwrap_or_default(),
                   size: file.total_bytes,
                   metadata,
               })

    5. Save recovery state:
       recovery_state = ctx.table_manager.get_global_keyed_state("r")
       recovery_state.insert(ctx.task_info.task_index, checkpoint)

    6. Handle pre-commit data:
       pre_commits = files_to_commit.iter().map(|f| (f.path.clone(), f.clone())).collect()

       Match commit_strategy:
           PerSubtask:
               pre_commit_state = ctx.table_manager.get_global_keyed_state("p")
               For (key, value) in pre_commits:
                   self.pre_commits.push(value.clone())
                   pre_commit_state.insert(key, value)
               ctx.table_manager.insert_committing_data("p", vec![])

           PerOperator:
               serialized = bincode::encode(&pre_commits)
               ctx.table_manager.insert_committing_data("p", serialized)

    7. Return Ok(())

```

### 4.9 handle_commit - Two-Phase Commit Finalization

```text
handle_commit(epoch, commit_data, ctx):
    1. Collect pre-commits:
       Match commit_strategy:
           PerSubtask:
               pre_commits = std::mem::take(&mut self.pre_commits)

           PerOperator:
               If ctx.task_info.task_index == 0:
                   pre_commits = commit_data.get("p")
                       .flat_map(deserialize_pre_commits)
                       .collect()
               Else:
                   pre_commits = vec![]

    2. Finalize files:
       finished_files = []
       For file in pre_commits:
           If file.multipart_id is Some:
               // Finalize multipart upload
               parts = file.parts.iter().map(|id| PartId { content_id: id }).collect()
               storage.close_multipart(file.path, file.multipart_id, parts).await?
               // Single-file uploads were already PUT, nothing to do

           finished_files.push(FinishedFile {
               filename: file.path,
               size: file.size,
               metadata: file.metadata,
           })

    3. Commit to table format:
       Match commit_state:
           DeltaLake { table, last_version }:
               If new_version = commit_files_to_delta(&finished_files, table, *last_version)?:
                   *last_version = new_version

           Iceberg(table):
               table.commit(epoch, &finished_files).await?

           VanillaParquet:
               // Nothing to do

    4. Clear committed files:
       self.files_to_commit.clear()

    5. Send completion event:
       ctx.control_tx.send(ControlResp::CheckpointEvent(CheckpointEvent {
           checkpoint_epoch: epoch,
           event_type: FinishedCommit,
           ...
       }))

    6. Return Ok(())

```

### 4.10 Helper: close_file

```text
close_file(partition):
    1. Get file:
       file_path = active_partitions.remove(partition)?
       file = open_files.get_mut(file_path)?

    2. Mark as closing:
       file.closing = true

    3. Flush remaining data:
       (final_bytes, metadata) = file.flush_all()
       file.metadata = metadata

    4. Handle final bytes:
       If final_bytes is None or empty:
           // No more data to upload
           If file.in_flight_parts == 0:
               finalize_file(file_path)
           // Else: will finalize when in-flight parts complete
           Return

       bytes = final_bytes.unwrap()

       If file.multipart_id is None:
           // No multipart started - use single-file PUT
           future = create_single_file_upload_future(storage, path, bytes)
           file.total_bytes = bytes.len()

           // Pre-add to files_to_commit (will be finalized when upload completes)
           files_to_commit.push(FileToCommit {
               path: file_path,
               multipart_id: None,
               parts: vec![],
               size: bytes.len(),
               metadata: file.metadata,
           })
       Else:
           // Add as final part
           part_index = file.completed_parts.len() + file.in_flight_parts

           // Handle minimum part size constraint
           // Final part can be any size, so this is always valid
           file.part_upload_started()
           future = create_part_upload_future(storage, path, multipart_id, part_index, bytes)

           pending_uploads.lock().push(future)

```

**Helper: finalize_file**

```text
finalize_file(file_path):
    file = open_files.remove(file_path)?

    // For single-file uploads, already added to files_to_commit in close_file
    If file.multipart_id is None:
        Return

    // Add to files_to_commit
    files_to_commit.push(FileToCommit {
        path: file_path,
        multipart_id: file.multipart_id,
        parts: file.completed_parts.iter().map(|p| p.content_id).collect(),
        size: file.total_bytes,
        metadata: file.metadata,
    })

```

---

## 5. Upload Futures with Retry

```rust
// sink/uploads.rs
use arroyo_rpc::retry;

pub fn create_part_upload_future(
    storage: Arc<StorageProvider>,
    path: Path,
    multipart_id: MultipartId,
    part_index: usize,
    data: Bytes,
) -> UploadFuture {
    let path_string = path.to_string();

    Box::pin(async move {
        let part_id = retry!(
            storage.add_multipart(&path, &multipart_id, part_index, data.clone()).await,
            max_retries = 10,
            base_delay = Duration::from_millis(100),
            map_err = |e| map_storage_error(e)
        )?;

        Ok(UploadResult::PartUploaded {
            file_path: path_string,
            part_index,
            part_id,
        })
    })
}

pub fn create_multipart_init_future(
    storage: Arc<StorageProvider>,
    path: Path,
) -> UploadFuture {
    let path_string = path.to_string();

    Box::pin(async move {
        let multipart_id = retry!(
            storage.start_multipart(&path).await,
            max_retries = 10,
            base_delay = Duration::from_millis(100),
            map_err = |e| map_storage_error(e)
        )?;

        Ok(UploadResult::MultipartInitialized {
            file_path: path_string,
            multipart_id,
        })
    })
}

pub fn create_single_file_upload_future(
    storage: Arc<StorageProvider>,
    path: Path,
    data: Bytes,
) -> UploadFuture {
    let path_string = path.to_string();
    let size = data.len();

    Box::pin(async move {
        retry!(
            storage.put(&path, data.clone()).await,
            max_retries = 10,
            base_delay = Duration::from_millis(100),
            map_err = |e| map_storage_error(e)
        )?;

        Ok(UploadResult::SingleFileUploaded {
            file_path: path_string,
            size,
        })
    })
}

```

---

## 6. File Organization

### 6.1 New File Structure

```text
arroyo-connectors/src/filesystem/
├── mod.rs                      # Connector definition, make_sink()
├── config.rs                   # Configuration structs (unchanged)
├── source.rs                   # Source implementation (unchanged)
└── sink/
    ├── mod.rs                  # Re-exports, shared types (FileToCommit, etc.)
    ├── operator.rs             # NEW: FileSystemSinkOperator
    ├── open_file.rs            # NEW: OpenFile struct and impl
    ├── uploads.rs              # NEW: Upload future creation with retry
    ├── checkpoint.rs           # NEW: Checkpoint types
    ├── two_phase_committer.rs  # KEEP: For LocalFileSystemWriter
    ├── local.rs                # LocalFileSystemWriter (unchanged)
    ├── parquet.rs              # ParquetBatchBufferingWriter (minor updates)
    ├── json.rs                 # JsonWriter (minor updates)
    ├── partitioning.rs         # Partitioner (unchanged)
    ├── delta.rs                # Delta Lake commit logic (unchanged)
    └── iceberg/
        ├── mod.rs              # IcebergTable (unchanged)
        └── metadata.rs         # IcebergFileMetadata (unchanged)

```

### 6.2 Module Dependencies

```text
operator.rs
├── open_file.rs
├── uploads.rs
├── checkpoint.rs
├── parquet.rs / json.rs (BatchBufferingWriter)
├── partitioning.rs
├── delta.rs
└── iceberg/mod.rs

two_phase_committer.rs (unchanged)
└── local.rs
    └── parquet.rs / json.rs (LocalWriter trait)

```

---

## 7. State Tables

The operator uses two state tables (same as current design):

### 7.1 Recovery Table "r"

* **Type:** `GlobalKeyedState<usize, FileSystemCheckpoint>`
* **Key:** Task index
* **Value:** Checkpoint state for that subtask
* **Purpose:** Recover in-progress files after failure

### 7.2 Pre-Commit Table "p"

* **Type:** `GlobalKeyedState<String, FileToCommit>`
* **Key:** File path
* **Value:** File ready to commit
* **Purpose:** Two-phase commit coordination
* **Config:** `uses_two_phase_commit: true`

---

## 8. Error Handling

### 8.1 Error Sources

1. **Storage operations:** `start_multipart`, `add_multipart`, `close_multipart`, `put`
2. **Table format operations:** Delta/Iceberg commit
3. **Serialization:** Checkpoint encoding/decoding
4. **Internal errors:** Invalid state, missing files

### 8.2 Error Flow

```text
Upload Future
│
├── Retry loop (up to 10 attempts)
│
└── Return Result<UploadResult, DataflowError>
    │
    ▼
handle_future_result
│
├── Ok(result) → Update state
│
└── Err(e) → Return Err(e) → Operator fails → Pipeline restarts

```

### 8.3 Error Mapping

Reuse existing `map_storage_error` and `map_object_store_error` functions from current implementation:

* **User errors (NoRetry):** Invalid paths, auth failures, permission denied
* **External errors (WithBackoff):** Transient network issues, rate limiting
* **Internal errors (NoRetry):** Invalid state, programming errors

---

## 9. Configuration

### 9.1 Existing Config (Unchanged)

```rust
pub struct FileSystemSink {
    pub output_path: String,
    pub file_naming: NamingConfig,
    pub rolling_policy: RollingPolicy,
    pub multipart: MultipartConfig,
    pub partitioning: PartitioningConfig,
}

pub struct MultipartConfig {
    pub target_part_size_bytes: Option<u64>,
    pub max_parts: Option<NonZeroU32>,
}

```

### 9.2 Hardcoded Values (For Now)

```rust
const MAX_CONCURRENT_UPLOADS: usize = 50;
const DEFAULT_TARGET_PART_SIZE: usize = 32 * 1024 * 1024;  // 32 MB
const UPLOAD_MAX_RETRIES: usize = 10;
const UPLOAD_BASE_DELAY_MS: u64 = 100;

```

---

## 10. Migration Path

### 10.1 Implementation Order

1. Create new files: `operator.rs`, `open_file.rs`, `uploads.rs`, `checkpoint.rs`
2. Implement `FileSystemSinkOperator` with all `ArrowOperator` methods
3. Update `mod.rs` to export new types alongside old ones
4. Update connector `make_sink()` to use new operator for object store sinks
5. Test thoroughly
6. Remove old `FileSystemSink` and channel-based code

### 10.2 Coexistence Strategy

During development, both implementations can coexist:

* **Old:** `FileSystemSink<BBW>` + `TwoPhaseCommitterOperator`
* **New:** `FileSystemSinkOperator<BBW>`

The connector's `make_sink()` function selects which to use.

---

## 11. Future Enhancements

### 11.1 Memory Pressure Handling

The infrastructure supports adding memory pressure handling:

```rust
struct FileSystemSinkOperator<BBW> {
    // ... existing fields ...

    // Future: memory tracking
    memory_tracker: MemoryTracker,
}

struct MemoryTracker {
    current_buffered_bytes: usize,
    max_buffered_bytes: usize,
    partition_lru: VecDeque<Option<OwnedRow>>,
}

```

When `current_buffered_bytes > threshold`:

1. Find oldest partition via LRU
2. Force flush (close file if necessary due to min part size)
3. Continue until under threshold

### 11.2 Configurable Concurrency

```rust
pub struct MultipartConfig {
    // ... existing ...
    pub max_concurrent_uploads: Option<usize>,
}

```

### 11.3 Adaptive Part Sizing

Adjust part size based on upload throughput to optimize for different network conditions.

---

## 12. Testing Strategy

### 12.1 Unit Tests

* **OpenFile:** Buffer management, state transitions
* **uploads.rs:** Future creation (mock storage)
* **checkpoint.rs:** Serialization round-trip

### 12.2 Integration Tests

* Single partition write → checkpoint → recover → commit
* Multi-partition write with rolling policies
* Error injection (storage failures)
* Delta/Iceberg commit flows

### 12.3 E2E Tests

* Existing filesystem sink tests should pass
* Add tests for single-file optimization
* Add tests for concurrent upload limiting