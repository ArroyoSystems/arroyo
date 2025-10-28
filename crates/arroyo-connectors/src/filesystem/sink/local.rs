use std::{collections::HashMap, fs::create_dir_all, path::Path, sync::Arc, time::SystemTime};

use arrow::record_batch::RecordBatch;
use arroyo_operator::context::OperatorContext;
use arroyo_rpc::{df::ArroyoSchemaRef, formats::Format};
use arroyo_storage::StorageProvider;
use arroyo_types::TaskInfo;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tracing::debug;
use ulid::Ulid;
use uuid::Uuid;

use super::{
    add_suffix_prefix, delta, two_phase_committer::TwoPhaseCommitterOperator, CommitState,
    FilenameStrategy, FinishedFile, MultiPartWriterStats, RollingPolicy,
};
use crate::filesystem::config::NamingConfig;
use crate::filesystem::sink::delta::load_or_create_table;
use crate::filesystem::sink::iceberg::metadata::IcebergFileMetadata;
use crate::filesystem::sink::partitioning::{Partitioner, PartitionerMode};
use crate::filesystem::{config, sink::two_phase_committer::TwoPhaseCommitter, TableFormat};
use anyhow::{bail, Result};
use arrow::row::OwnedRow;

pub struct LocalFileSystemWriter<V: LocalWriter> {
    // writer to a local tmp file
    writers: HashMap<Option<OwnedRow>, V>,
    tmp_dir: String,
    final_dir: String,
    next_file_index: usize,
    subtask_id: usize,
    partitioner_mode: PartitionerMode,
    partitioner: Option<Arc<Partitioner>>,
    finished_files: Vec<FilePreCommit>,
    rolling_policies: Vec<RollingPolicy>,
    table_properties: config::FileSystemSink,
    format: Format,
    schema: Option<ArroyoSchemaRef>,
    commit_state: Option<CommitState>,
    file_naming: NamingConfig,
    table_format: TableFormat,
}

impl<V: LocalWriter> LocalFileSystemWriter<V> {
    pub fn new(
        table_properties: config::FileSystemSink,
        table_format: TableFormat,
        format: Format,
        partitioner_mode: PartitionerMode,
    ) -> TwoPhaseCommitterOperator<Self> {
        let mut final_dir = table_properties.path.clone();
        if final_dir.starts_with("file://") {
            final_dir = final_dir.trim_start_matches("file://").to_string();
        }
        // TODO: explore configuration options here
        let tmp_dir = format!("{final_dir}/__in_progress");
        // make sure final_dir and tmp_dir exists
        create_dir_all(&tmp_dir).unwrap();

        let mut file_naming = table_properties.file_naming.clone();
        if file_naming.suffix.is_none() {
            file_naming.suffix = Some(V::file_suffix().to_string());
        }

        let writer = Self {
            writers: HashMap::new(),
            tmp_dir,
            final_dir,
            next_file_index: 0,
            subtask_id: 0,
            partitioner: None,
            finished_files: Vec::new(),
            format,
            rolling_policies: RollingPolicy::from_file_settings(&table_properties),
            table_properties,
            schema: None,
            commit_state: None,
            file_naming,
            table_format,
            partitioner_mode,
        };
        TwoPhaseCommitterOperator::new(writer)
    }

    fn get_or_insert_writer(&mut self, partition: &Option<OwnedRow>) -> &mut V {
        let filename_strategy = self.file_naming.strategy.unwrap_or_default();

        if !self.writers.contains_key(partition) {
            // This forms the base for naming files depending on strategy
            let filename_base = match filename_strategy {
                FilenameStrategy::Serial => {
                    format!("{:>05}-{:>03}", self.next_file_index, self.subtask_id)
                }
                FilenameStrategy::Ulid => Ulid::new().to_string(),
                FilenameStrategy::Uuid => Uuid::new_v4().to_string(),
                FilenameStrategy::UuidV7 => Uuid::now_v7().to_string(),
            };

            let mut filename = add_suffix_prefix(
                filename_base,
                self.file_naming.prefix.as_ref(),
                self.file_naming.suffix.as_ref().unwrap(),
            );

            if let Some(partition) = partition {
                if let Some(hive) = self.partitioner.as_ref().unwrap().hive_path(partition) {
                    filename = format!("{hive}/{filename}");
                }
            }

            self.writers.insert(
                partition.clone(),
                V::new(
                    format!("{}/{}", self.tmp_dir, filename),
                    format!("{}/{}", self.final_dir, filename),
                    &self.table_properties,
                    self.format.clone(),
                    self.schema.as_ref().unwrap().clone(),
                ),
            );
            self.next_file_index += 1;
        }
        self.writers.get_mut(partition).unwrap()
    }
}

pub trait LocalWriter: Send + 'static {
    fn new(
        tmp_path: String,
        final_path: String,
        table_properties: &config::FileSystemSink,
        format: Format,
        schema: ArroyoSchemaRef,
    ) -> Self;
    fn file_suffix() -> &'static str;
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<usize>;
    // returns the total size of the file
    fn sync(&mut self) -> Result<usize>;
    fn close(&mut self) -> Result<FilePreCommit>;
    fn checkpoint(&mut self) -> Result<Option<CurrentFileRecovery>>;
    fn stats(&self) -> MultiPartWriterStats;
}

#[derive(Debug, Clone, Decode, Encode, PartialEq)]
pub struct LocalFileDataRecovery {
    next_file_index: usize,
    current_files: Vec<CurrentFileRecovery>,
}

#[derive(Debug, Clone, Decode, Encode, PartialEq)]
pub struct CurrentFileRecovery {
    pub tmp_file: String,
    pub bytes_written: usize,
    pub suffix: Option<Vec<u8>>,
    pub destination: String,
    pub metadata: Option<IcebergFileMetadata>,
}

#[derive(Debug, Clone, Decode, Encode, PartialEq)]
pub struct FilePreCommit {
    pub tmp_file: String,
    pub destination: String,
}

#[async_trait]
impl<V: LocalWriter + Send + 'static> TwoPhaseCommitter for LocalFileSystemWriter<V> {
    type DataRecovery = LocalFileDataRecovery;
    type PreCommit = FilePreCommit;

    fn name(&self) -> String {
        "local_file".to_string()
    }

    async fn init(
        &mut self,
        ctx: &mut OperatorContext,
        data_recovery: Vec<Self::DataRecovery>,
    ) -> Result<()> {
        let mut max_file_index = 0;
        let mut recovered_files = Vec::new();
        for LocalFileDataRecovery {
            next_file_index,
            current_files,
        } in data_recovery
        {
            max_file_index = max_file_index.max(next_file_index);
            // task 0 is responsible for recovering all files.
            // This is because the number of subtasks may have changed.
            // Recovering should be reasonably fast since it is just finishing the files.
            if ctx.task_info.task_index > 0 {
                continue;
            }
            for CurrentFileRecovery {
                tmp_file,
                bytes_written,
                suffix,
                destination,
                ..
            } in current_files
            {
                let mut file = OpenOptions::new()
                    .write(true)
                    .open(tmp_file.clone())
                    .await?;
                file.set_len(bytes_written as u64).await?;
                if let Some(suffix) = suffix {
                    file.write_all(&suffix).await?;
                }
                file.flush().await?;
                file.sync_all().await?;
                recovered_files.push(FilePreCommit {
                    tmp_file,
                    destination,
                })
            }
        }
        self.subtask_id = ctx.task_info.task_index as usize;
        self.finished_files = recovered_files;
        self.next_file_index = max_file_index;

        let storage_provider = StorageProvider::for_url(&self.final_dir).await?;

        let schema = ctx.in_schemas[0].clone();

        self.commit_state = Some(match self.table_format {
            TableFormat::Delta { .. } => CommitState::DeltaLake {
                last_version: -1,
                table: Box::new(
                    load_or_create_table(&storage_provider, &schema.schema_without_timestamp())
                        .await?,
                ),
            },
            TableFormat::None { .. } => CommitState::VanillaParquet,
            TableFormat::Iceberg { .. } => todo!(),
        });

        self.partitioner = Some(Arc::new(Partitioner::new(
            self.partitioner_mode.clone(),
            &schema.schema,
        )?));

        self.schema = Some(schema);

        Ok(())
    }

    async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let partitioner = self.partitioner.as_ref().unwrap();
        if partitioner.is_partitioned() {
            for (partition, batch) in partitioner.partition(&batch)? {
                let writer = self.get_or_insert_writer(&Some(partition));
                writer.write_batch(&batch)?;
            }
        } else {
            let writer = self.get_or_insert_writer(&None);
            writer.write_batch(&batch)?;
        }

        Ok(())
    }

    async fn commit(
        &mut self,
        _epoch: u32,
        _task_info: &TaskInfo,
        pre_commit: Vec<Self::PreCommit>,
    ) -> Result<()> {
        if pre_commit.is_empty() {
            return Ok(());
        }

        let mut finished_files = vec![];

        for FilePreCommit {
            tmp_file,
            destination,
        } in pre_commit
        {
            let (tmp_file, destination) = (Path::new(&tmp_file), Path::new(&destination));

            if destination.exists() {
                return Ok(());
            }

            if !tmp_file.exists() {
                bail!("tmp file {} does not exist", tmp_file.to_string_lossy());
            }

            debug!(
                "committing file {} to {}",
                tmp_file.to_string_lossy(),
                destination.to_string_lossy()
            );

            tokio::fs::rename(tmp_file, destination).await?;

            let filename = destination
                .to_string_lossy()
                .strip_prefix(&self.final_dir)
                .unwrap_or_else(|| {
                    panic!(
                        "invalid file in commit: {:?}; must be in directory {}",
                        destination, self.final_dir
                    )
                })
                .to_string();

            finished_files.push(FinishedFile {
                filename,
                partition: None,
                size: destination.metadata()?.len() as usize,
                // TODO: this breaks iceberg, but we don't support iceberg on local filesystem anyways
                metadata: None,
            });
        }

        match self.commit_state.as_mut().unwrap() {
            CommitState::DeltaLake {
                last_version,
                table,
            } => {
                if let Some(version) =
                    delta::commit_files_to_delta(&finished_files, table, *last_version).await?
                {
                    *last_version = version;
                }
            }
            CommitState::Iceberg(_) => {
                unreachable!("Iceberg is not supported for local filesystems");
            }
            CommitState::VanillaParquet => {
                // nothing to do
            }
        }

        Ok(())
    }

    async fn checkpoint(
        &mut self,
        _task_info: &TaskInfo,
        watermark: Option<SystemTime>,
        stopping: bool,
    ) -> Result<(Self::DataRecovery, HashMap<String, Self::PreCommit>)> {
        let mut partitions_to_roll = vec![];
        for (partition, writer) in self.writers.iter_mut() {
            writer.sync()?;
            let stats = writer.stats();
            if self
                .rolling_policies
                .iter()
                .any(|p| p.should_roll(&stats, watermark))
                || stopping
            {
                partitions_to_roll.push(partition.clone());
            }
        }
        for partition in partitions_to_roll {
            let mut writer = self.writers.remove(&partition).unwrap();
            let pre_commit = writer.close()?;
            self.finished_files.push(pre_commit);
        }
        let mut pre_commits = HashMap::new();
        for pre_commit in self.finished_files.drain(..) {
            pre_commits.insert(pre_commit.destination.to_string(), pre_commit);
        }
        let data_recovery = LocalFileDataRecovery {
            next_file_index: self.next_file_index,
            current_files: self
                .writers
                .iter_mut()
                .filter_map(|(_partition, writer)| writer.checkpoint().transpose())
                .collect::<Result<_>>()?,
        };
        Ok((data_recovery, pre_commits))
    }
}
