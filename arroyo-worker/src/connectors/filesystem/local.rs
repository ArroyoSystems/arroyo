use std::{
    collections::HashMap, fs::create_dir_all, marker::PhantomData, path::Path, time::Instant,
};

use arroyo_types::{Data, Key, Record, TaskInfo};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tracing::info;

use crate::connectors::two_phase_committer::TwoPhaseCommitter;

use anyhow::{bail, Result};

use super::{FileSystemTable, MultiPartWriterStats, RollingPolicy, TableType};

pub struct LocalFileSystemWriter<K: Key, D: Data + Sync, V: LocalWriter<D>> {
    // writer to a local tmp file
    writer: Option<V>,
    tmp_dir: String,
    final_dir: String,
    next_file_index: usize,
    subtask_id: usize,
    finished_files: Vec<FilePreCommit>,
    first_write: Option<Instant>,
    last_write: Option<Instant>,
    rolling_policy: RollingPolicy,
    table_properties: FileSystemTable,
    phantom: PhantomData<(K, D)>,
}

impl<K: Key, D: Data + Sync, V: LocalWriter<D>> LocalFileSystemWriter<K, D, V> {
    pub fn new(final_dir: String, table_properties: FileSystemTable) -> Self {
        // TODO: explore configuration options here
        let tmp_dir = format!("{}/__in_progress", final_dir);
        // make sure final_dir and tmp_dir exists
        create_dir_all(&tmp_dir).unwrap();

        let file_settings = if let TableType::Sink {
            ref file_settings, ..
        } = table_properties.type_
        {
            file_settings.as_ref().unwrap()
        } else {
            unreachable!("LocalFileSystemWriter can only be used as a sink")
        };

        Self {
            writer: None,
            tmp_dir,
            final_dir,
            next_file_index: 0,
            subtask_id: 0,
            finished_files: Vec::new(),
            first_write: None,
            last_write: None,
            rolling_policy: RollingPolicy::from_file_settings(file_settings),
            table_properties,
            phantom: PhantomData,
        }
    }

    fn should_roll(&mut self) -> bool {
        if !self.first_write.is_some() {
            return false;
        }
        if let Some(writer) = self.writer.as_mut() {
            let bytes_written = writer.sync().unwrap();
            let stats = MultiPartWriterStats {
                bytes_written,
                parts_written: 0,
                last_write_at: self.last_write.unwrap(),
                first_write_at: self.first_write.unwrap(),
            };
            self.rolling_policy.should_roll(&stats)
        } else {
            false
        }
    }

    fn init_writer(&mut self) -> Result<()> {
        let file_name = format!(
            "{:>05}-{:>03}.{}",
            self.next_file_index,
            self.subtask_id,
            V::file_suffix()
        );
        self.writer = Some(V::new(
            format!("{}/{}", self.tmp_dir, file_name),
            format!("{}/{}", self.final_dir, file_name),
            &self.table_properties,
        ));
        self.next_file_index += 1;
        self.first_write = Some(Instant::now());
        Ok(())
    }
}

pub trait LocalWriter<T: Data>: Send + 'static {
    fn new(tmp_path: String, final_path: String, table_properties: &FileSystemTable) -> Self;
    fn file_suffix() -> &'static str;
    fn write(&mut self, value: T) -> Result<()>;
    // returns the total size of the file
    fn sync(&mut self) -> Result<usize>;
    fn close(&mut self) -> Result<FilePreCommit>;
    fn checkpoint(&mut self) -> Result<Option<CurrentFileRecovery>>;
}

#[derive(Debug, Clone, Decode, Encode, PartialEq, PartialOrd)]
pub struct LocalFileDataRecovery {
    next_file_index: usize,
    current_file: Option<CurrentFileRecovery>,
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
impl<K: Key, D: Data + Sync, V: LocalWriter<D> + Send + 'static> TwoPhaseCommitter<K, D>
    for LocalFileSystemWriter<K, D, V>
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
            current_file,
        } in data_recovery
        {
            max_file_index = max_file_index.max(next_file_index);
            // task 0 is responsible for recovering all files.
            // This is because the number of subtasks may have changed.
            // Recovering should be reasonably fast since it is just finishing the files.
            if task_info.task_index > 0 {
                continue;
            }
            let Some(CurrentFileRecovery {
                tmp_file,
                bytes_written,
                suffix,
                destination,
            }) = current_file
            else {
                continue;
            };
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
        self.subtask_id = task_info.task_index;
        self.finished_files = recovered_files;
        self.next_file_index = max_file_index;
        Ok(())
    }

    async fn insert_record(&mut self, record: &Record<K, D>) -> Result<()> {
        if self.first_write.is_none() {
            self.init_writer()?;
        };
        self.writer.as_mut().unwrap().write(record.value.clone())?;
        self.last_write = Some(Instant::now());
        Ok(())
    }

    async fn commit(
        &mut self,
        _task_info: &TaskInfo,
        pre_commit: Vec<Self::PreCommit>,
    ) -> Result<()> {
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
        }
        Ok(())
    }

    async fn checkpoint(
        &mut self,
        _task_info: &TaskInfo,
        stopping: bool,
    ) -> Result<(Self::DataRecovery, HashMap<String, Self::PreCommit>)> {
        if self.should_roll() || stopping {
            let pre_commit = self.writer.take().unwrap().close()?;
            self.first_write = None;
            self.last_write = None;
            self.finished_files.push(pre_commit);
        }
        let mut pre_commits = HashMap::new();
        for pre_commit in self.finished_files.drain(..) {
            pre_commits.insert(pre_commit.destination.to_string(), pre_commit);
        }
        let data_recovery = LocalFileDataRecovery {
            next_file_index: self.next_file_index,
            current_file: self
                .writer
                .as_mut()
                .map(|writer| writer.checkpoint())
                .transpose()?
                .flatten(),
        };
        Ok((data_recovery, pre_commits))
    }
}
