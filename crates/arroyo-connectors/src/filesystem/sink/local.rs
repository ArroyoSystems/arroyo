use std::{collections::HashMap, fs::create_dir_all, path::Path, sync::Arc, time::SystemTime};

use arrow::record_batch::RecordBatch;
use arroyo_operator::context::OperatorContext;
use arroyo_rpc::{df::ArroyoSchemaRef, formats::Format, OperatorConfig};
use arroyo_storage::StorageProvider;
use arroyo_types::TaskInfo;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use datafusion::physical_plan::PhysicalExpr;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tracing::debug;
use uuid::Uuid;

use super::{
    add_suffix_prefix, delta, get_partitioner_from_file_settings, parquet::batches_by_partition,
    two_phase_committer::TwoPhaseCommitterOperator, CommitState, CommitStyle, FileNaming,
    FileSystemTable, FilenameStrategy, FinishedFile, MultiPartWriterStats, RollingPolicy,
    TableType,
};
use crate::filesystem::sink::delta::load_or_create_table;
use crate::filesystem::{sink::two_phase_committer::TwoPhaseCommitter, FileSettings};
use anyhow::{bail, Result};

pub struct LocalFileSystemWriter<V: LocalWriter> {
    // writer to a local tmp file
    writers: HashMap<Option<String>, V>,
    tmp_dir: String,
    final_dir: String,
    next_file_index: usize,
    subtask_id: usize,
    partitioner: Option<Arc<dyn PhysicalExpr>>,
    finished_files: Vec<FilePreCommit>,
    rolling_policies: Vec<RollingPolicy>,
    table_properties: FileSystemTable,
    file_settings: FileSettings,
    format: Option<Format>,
    schema: Option<ArroyoSchemaRef>,
    commit_state: Option<CommitState>,
    filenaming: FileNaming,
}

impl<V: LocalWriter> LocalFileSystemWriter<V> {
    pub fn new(
        mut final_dir: String,
        table_properties: FileSystemTable,
        config: OperatorConfig,
    ) -> TwoPhaseCommitterOperator<Self> {
        if final_dir.starts_with("file://") {
            final_dir = final_dir.trim_start_matches("file://").to_string();
        }
        // TODO: explore configuration options here
        let tmp_dir = format!("{}/__in_progress", final_dir);
        // make sure final_dir and tmp_dir exists
        create_dir_all(&tmp_dir).unwrap();

        let TableType::Sink {
            ref file_settings, ..
        } = table_properties.table_type
        else {
            unreachable!("LocalFileSystemWriter can only be used as a sink")
        };

        let mut filenaming = file_settings
            .clone()
            .unwrap()
            .file_naming
            .unwrap_or(FileNaming {
                strategy: Some(FilenameStrategy::Serial),
                prefix: None,
                suffix: None,
            });

        if filenaming.suffix.is_none() {
            filenaming.suffix = Some(V::file_suffix().to_string());
        }

        let writer = Self {
            writers: HashMap::new(),
            tmp_dir,
            final_dir,
            next_file_index: 0,
            subtask_id: 0,
            partitioner: None,
            finished_files: Vec::new(),
            file_settings: file_settings.clone().unwrap(),
            format: config.format,
            rolling_policies: RollingPolicy::from_file_settings(file_settings.as_ref().unwrap()),
            table_properties,
            schema: None,
            commit_state: None,
            filenaming,
        };
        TwoPhaseCommitterOperator::new(writer)
    }

    fn get_or_insert_writer(&mut self, partition: &Option<String>) -> &mut V {
        let filename_strategy = match self.filenaming.strategy {
            Some(FilenameStrategy::Uuid) => FilenameStrategy::Uuid,
            Some(FilenameStrategy::Serial) => FilenameStrategy::Serial,
            None => FilenameStrategy::Serial,
        };

        if !self.writers.contains_key(partition) {
            // This forms the base for naming files depending on strategy
            let filename_base = if filename_strategy == FilenameStrategy::Uuid {
                Uuid::new_v4().to_string()
            } else {
                format!("{:>05}-{:>03}", self.next_file_index, self.subtask_id)
            };
            let filename = add_suffix_prefix(
                filename_base,
                self.filenaming.prefix.as_ref(),
                self.filenaming.suffix.as_ref().unwrap(),
            );

            let filename = match partition {
                Some(partition) => {
                    // make sure the partition directory exists in tmp and final
                    create_dir_all(format!("{}/{}", self.tmp_dir, partition)).unwrap();
                    create_dir_all(format!("{}/{}", self.final_dir, partition)).unwrap();
                    format!("{}/{}", partition, filename)
                }
                None => filename,
            };
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
        table_properties: &FileSystemTable,
        format: Option<Format>,
        schema: ArroyoSchemaRef,
    ) -> Self;
    fn file_suffix() -> &'static str;
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;
    // returns the total size of the file
    fn sync(&mut self) -> Result<usize>;
    fn close(&mut self) -> Result<FilePreCommit>;
    fn checkpoint(&mut self) -> Result<Option<CurrentFileRecovery>>;
    fn stats(&self) -> MultiPartWriterStats;
}

#[derive(Debug, Clone, Decode, Encode, PartialEq, PartialOrd)]
pub struct LocalFileDataRecovery {
    next_file_index: usize,
    current_files: Vec<CurrentFileRecovery>,
}

#[derive(Debug, Clone, Decode, Encode, PartialEq, PartialOrd)]
pub struct CurrentFileRecovery {
    pub tmp_file: String,
    pub bytes_written: usize,
    pub suffix: Option<Vec<u8>>,
    pub destination: String,
}

#[derive(Debug, Clone, Decode, Encode, PartialEq, PartialOrd)]
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

        self.commit_state = Some(match self.file_settings.commit_style.unwrap() {
            CommitStyle::DeltaLake => CommitState::DeltaLake {
                last_version: -1,
                table: Box::new(
                    load_or_create_table(&storage_provider, &schema.schema_without_timestamp())
                        .await?,
                ),
            },
            CommitStyle::Direct => CommitState::VanillaParquet,
        });

        self.partitioner =
            get_partitioner_from_file_settings(self.file_settings.clone(), schema.clone());

        self.schema = Some(schema);

        Ok(())
    }

    async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if let Some(partitioner) = self.partitioner.as_ref() {
            for (batch, partition) in batches_by_partition(batch, partitioner.clone())? {
                let writer = self.get_or_insert_writer(&partition);
                writer.write_batch(batch)?;
            }
        } else {
            let writer = self.get_or_insert_writer(&None);
            writer.write_batch(batch)?;
        }
        Ok(())
    }

    async fn commit(
        &mut self,
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
            });
        }

        if let CommitState::DeltaLake {
            last_version,
            table,
        } = self.commit_state.as_mut().unwrap()
        {
            if let Some(version) =
                delta::commit_files_to_delta(&finished_files, table, *last_version).await?
            {
                *last_version = version;
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
