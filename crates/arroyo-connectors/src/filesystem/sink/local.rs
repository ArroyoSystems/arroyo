use std::{collections::HashMap, fs::create_dir_all, path::Path, sync::Arc, time::SystemTime};

use arrow::record_batch::RecordBatch;
use arroyo_operator::context::ArrowContext;
use arroyo_rpc::{
    df::{ArroyoSchema, ArroyoSchemaRef},
    formats::Format,
    OperatorConfig,
};
use arroyo_storage::StorageProvider;
use arroyo_types::TaskInfo;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use datafusion::physical_plan::PhysicalExpr;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tracing::info;
use uuid::Uuid;

use crate::filesystem::{sink::two_phase_committer::TwoPhaseCommitter, FileSettings};
use anyhow::{bail, Result};

use super::{
    add_suffix_prefix, delta, get_partitioner_from_file_settings, parquet::batches_by_partition,
    two_phase_committer::TwoPhaseCommitterOperator, CommitState, CommitStyle, FileNaming,
    FileSystemTable, FilenameStrategy, FinishedFile, MultiPartWriterStats, RollingPolicy,
    TableType,
};

pub struct LocalFileSystemWriter<V: LocalWriter> {
    // writer to a local tmp file
    writers: HashMap<Option<String>, V>,
    tmp_dir: String,
    final_dir: String,
    next_file_index: usize,
    subtask_id: usize,
    partitioner: Option<Arc<dyn PhysicalExpr>>,
    finished_files: Vec<FilePreCommit>,
    rolling_policy: RollingPolicy,
    table_properties: FileSystemTable,
    file_settings: FileSettings,
    format: Option<Format>,
    schema: Option<ArroyoSchemaRef>,
    commit_state: CommitState,
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
        let commit_state = match file_settings.as_ref().unwrap().commit_style.unwrap() {
            CommitStyle::DeltaLake => CommitState::DeltaLake { last_version: -1 },
            CommitStyle::Direct => CommitState::VanillaParquet,
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
            schema: None,
            format: config.format,
            rolling_policy: RollingPolicy::from_file_settings(file_settings.as_ref().unwrap()),
            table_properties,
            commit_state,
            filenaming,
        };
        TwoPhaseCommitterOperator::new(writer)
    }

    fn init_schema_and_partitioner(&mut self, record_batch: &RecordBatch) -> Result<()> {
        if self.schema.is_none() {
            self.schema = Some(Arc::new(ArroyoSchema::from_fields(
                record_batch
                    .schema()
                    .fields()
                    .into_iter()
                    .map(|field| field.as_ref().clone())
                    .collect(),
            )));
        }
        if self.partitioner.is_none() {
            self.partitioner = get_partitioner_from_file_settings(
                self.file_settings.clone(),
                self.schema.as_ref().unwrap().clone(),
            );
        }
        Ok(())
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
        ctx: &mut ArrowContext,
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
        self.subtask_id = ctx.task_info.task_index;
        self.finished_files = recovered_files;
        self.next_file_index = max_file_index;
        Ok(())
    }

    async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.schema.is_none() {
            self.init_schema_and_partitioner(&batch)?;
        }
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
            info!(
                "committing file {} to {}",
                tmp_file.to_string_lossy(),
                destination.to_string_lossy()
            );
            tokio::fs::rename(tmp_file, destination).await?;
            finished_files.push(FinishedFile {
                filename: object_store::path::Path::parse(destination.to_string_lossy())?
                    .to_string(),
                partition: None,
                size: destination.metadata()?.len() as usize,
            });
        }
        if let CommitState::DeltaLake { last_version } = self.commit_state {
            let storage_provider = Arc::new(StorageProvider::for_url("/").await?);
            if let Some(version) = delta::commit_files_to_delta(
                &finished_files,
                &object_store::path::Path::parse(&self.final_dir)?,
                &storage_provider,
                last_version,
                Arc::new(self.schema.as_ref().unwrap().schema_without_timestamp()),
            )
            .await?
            {
                self.commit_state = CommitState::DeltaLake {
                    last_version: version,
                };
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
            if self.rolling_policy.should_roll(&stats, watermark) || stopping {
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
