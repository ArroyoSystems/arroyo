use std::{
    collections::HashMap, fs::create_dir_all, marker::PhantomData, path::Path, sync::Arc,
    time::SystemTime,
};

use arroyo_storage::StorageProvider;
use arroyo_types::{Data, Key, Record, TaskInfo};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use serde::Serialize;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tracing::info;

use crate::{
    connectors::{filesystem::FinishedFile, two_phase_committer::TwoPhaseCommitter},
    SchemaData,
};

use anyhow::{bail, Result};

use super::{
    delta, get_partitioner_from_table, CommitState, CommitStyle, FileSystemTable,
    MultiPartWriterStats, RollingPolicy,
};

pub struct LocalFileSystemWriter<K: Key, D: Data + Sync, V: LocalWriter<D>> {
    // writer to a local tmp file
    writers: HashMap<Option<String>, V>,
    tmp_dir: String,
    final_dir: String,
    next_file_index: usize,
    subtask_id: usize,
    partitioner: Option<Box<dyn Fn(&Record<K, D>) -> String + Send>>,
    finished_files: Vec<FilePreCommit>,
    rolling_policy: RollingPolicy,
    table_properties: FileSystemTable,
    commit_state: CommitState,
    phantom: PhantomData<(K, D)>,
}

impl<K: Key, D: Data + Sync + Serialize, V: LocalWriter<D>> LocalFileSystemWriter<K, D, V> {
    pub fn new(final_dir: String, table_properties: FileSystemTable) -> Self {
        // TODO: explore configuration options here
        let tmp_dir = format!("{}/__in_progress", final_dir);
        // make sure final_dir and tmp_dir exists
        create_dir_all(&tmp_dir).unwrap();

        let commit_state = match table_properties
            .file_settings
            .as_ref()
            .unwrap()
            .commit_style
            .unwrap()
        {
            CommitStyle::DeltaLake => CommitState::DeltaLake { last_version: -1 },
            CommitStyle::Direct => CommitState::VanillaParquet,
        };

        Self {
            writers: HashMap::new(),
            tmp_dir,
            final_dir,
            next_file_index: 0,
            subtask_id: 0,
            partitioner: get_partitioner_from_table(&table_properties),
            finished_files: Vec::new(),
            rolling_policy: RollingPolicy::from_file_settings(
                table_properties.file_settings.as_ref().unwrap(),
            ),
            table_properties,
            commit_state,
            phantom: PhantomData,
        }
    }

    fn get_or_insert_writer(&mut self, partition: &Option<String>) -> &mut V {
        if !self.writers.contains_key(partition) {
            let file_name = match partition {
                Some(partition) => {
                    // make sure the partition directory exists in tmp and final
                    create_dir_all(&format!("{}/{}", self.tmp_dir, partition)).unwrap();
                    create_dir_all(&format!("{}/{}", self.final_dir, partition)).unwrap();
                    format!(
                        "{}/{:>05}-{:>03}.{}",
                        partition,
                        self.next_file_index,
                        self.subtask_id,
                        V::file_suffix()
                    )
                }
                None => format!(
                    "{:>05}-{:>03}.{}",
                    self.next_file_index,
                    self.subtask_id,
                    V::file_suffix()
                ),
            };
            self.writers.insert(
                partition.clone(),
                V::new(
                    format!("{}/{}", self.tmp_dir, file_name),
                    format!("{}/{}", self.final_dir, file_name),
                    &self.table_properties,
                ),
            );
            self.next_file_index += 1;
        }
        self.writers.get_mut(&partition).unwrap()
    }
}

pub trait LocalWriter<T: Data>: Send + 'static {
    fn new(tmp_path: String, final_path: String, table_properties: &FileSystemTable) -> Self;
    fn file_suffix() -> &'static str;
    fn write(&mut self, value: T, timestamp: SystemTime) -> Result<()>;
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
impl<K: Key, D: Data + Sync + SchemaData + Serialize, V: LocalWriter<D> + Send + 'static>
    TwoPhaseCommitter<K, D> for LocalFileSystemWriter<K, D, V>
{
    type DataRecovery = LocalFileDataRecovery;
    type PreCommit = FilePreCommit;

    fn name(&self) -> String {
        "local_file".to_string()
    }

    async fn init(
        &mut self,
        task_info: &TaskInfo,
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
            if task_info.task_index > 0 {
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
        self.subtask_id = task_info.task_index;
        self.finished_files = recovered_files;
        self.next_file_index = max_file_index;
        Ok(())
    }

    async fn insert_record(&mut self, record: &Record<K, D>) -> Result<()> {
        let partition = self.partitioner.as_ref().map(|f| f(record));
        let writer = self.get_or_insert_writer(&partition);
        writer
            .write(record.value.clone(), record.timestamp)
            .unwrap();
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
                filename: object_store::path::Path::parse(
                    destination.to_string_lossy().to_string(),
                )?
                .to_string(),
                partition: None,
                size: destination.metadata()?.len() as usize,
            });
        }
        if let CommitState::DeltaLake { last_version } = self.commit_state {
            let schema = D::schema();
            let storage_provider = Arc::new(StorageProvider::for_url("/").await?);
            if let Some(version) = delta::commit_files_to_delta(
                finished_files,
                object_store::path::Path::parse(&self.final_dir)?,
                storage_provider,
                last_version,
                schema,
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
