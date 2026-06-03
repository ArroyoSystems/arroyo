use crate::ProtocolPaths;
use crate::store::{ProtocolStore, StoreError, read_protobuf};
use crate::types::{CheckpointRef, Epoch, Generation, ProtocolError};
use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, GlobalKeyedTableTaskCheckpointMetadata, TableCheckpointMetadata, TableEnum,
};
use futures::future::join_all;
use prost::Message;
use std::collections::HashSet;
use tracing::{debug, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct CheckpointOwner {
    pub generation: Generation,
    pub epoch: Epoch,
}

pub async fn cleanup_leader_checkpoints<S>(
    store: &S,
    paths: &ProtocolPaths,
    head: CheckpointRef,
    new_min_epoch: Epoch,
) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let history =
        collect_history_and_clean_checkpoint_files(store, paths, head, new_min_epoch).await?;

    for c in history {
        debug!(
            generation = c.generation.0,
            epoch = c.epoch.0,
            "cleaning checkpoint"
        );

        store
            .delete_object(&paths.checkpoint_manifest(c.generation, c.epoch))
            .await?;
    }

    Ok(())
}

/// Traverses backwards through the checkpoint history, doing two things:
/// 1. Finding and cleaning data files for GC'able checkpoints
/// 2. Collecting a record of ownership for each checkpoint
///
/// We do it in this order because we must delete metadata files by going forward, to prevent
/// gaps from forming in case of failure which would orphan older files. However, we do not
/// want to re-read or retain the metadata in order to do file deletion as that could exhaust
/// memory, so we opportunistically delete data files.
async fn collect_history_and_clean_checkpoint_files<S>(
    store: &S,
    paths: &ProtocolPaths,
    current: CheckpointRef,
    new_min_epoch: Epoch,
) -> Result<Vec<CheckpointOwner>, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let mut history = Vec::new();
    let mut seen = HashSet::new();
    let mut next = Some(current);

    while let Some(checkpoint_ref) = next {
        // TODO: use a metadata cache here so we're not re-reading checkpoints we've just written
        let Some(manifest): Option<CheckpointManifest> =
            read_protobuf(store, &checkpoint_ref).await?
        else {
            if history.is_empty() {
                return Err(StoreError::ExistingObjectMissing {
                    path: checkpoint_ref,
                });
            }
            break;
        };

        let owner = CheckpointOwner {
            generation: Generation(manifest.generation),
            epoch: Epoch(manifest.epoch),
        };

        if !seen.insert(owner) {
            return Err(ProtocolError::CheckpointCycle {
                generation: owner.generation,
                epoch: owner.epoch,
            }
            .into());
        }
        history.push(owner);

        if manifest.epoch < new_min_epoch.0 {
            if let Err(e) = clean_checkpoint(store, paths, &checkpoint_ref, &manifest).await {
                warn!(
                    checkpoint = checkpoint_ref.as_str(),
                    error =? e,
                    "failed to clean checkpoint"
                );
            }
        }

        next = manifest
            .parent_checkpoint_ref
            .map(|r| CheckpointRef::new(r))
            .transpose()?;
    }

    Ok(history)
}

/// cleans a checkpoint but leaves the metadata file in place
async fn clean_checkpoint<S>(
    store: &S,
    paths: &ProtocolPaths,
    manifest_path: &CheckpointRef,
    checkpoint: &CheckpointManifest,
) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let mut to_delete = vec![];
    for operator in &checkpoint.operators {
        for (table_name, metadata) in &operator.table_checkpoint_metadata {
            let op_metadata =
                operator
                    .operator_metadata
                    .as_ref()
                    .ok_or_else(|| StoreError::InvalidProtobuf {
                        path: manifest_path.clone(),
                        msg: "missing OperatorMetadata field".to_string(),
                    })?;

            table_checkpoint_data_files(
                &op_metadata.operator_id,
                table_name,
                manifest_path,
                metadata,
                &mut to_delete,
            )?;
        }
    }

    to_delete
        .push(paths.committed_marker(Generation(checkpoint.generation), Epoch(checkpoint.epoch)));
    to_delete.push(paths.epoch_record(Epoch(checkpoint.epoch)));

    for r in join_all(to_delete.iter().map(|f| store.delete_object(f))).await {
        r?;
    }

    Ok(())
}

fn table_checkpoint_data_files(
    operator_id: &str,
    table_name: &str,
    metadata_path: &CheckpointRef,
    metadata: &TableCheckpointMetadata,
    files: &mut Vec<CheckpointRef>,
) -> Result<(), StoreError> {
    match metadata.table_type() {
        TableEnum::MissingTableType => {
            return Err(StoreError::InvalidProtobuf {
                path: metadata_path.clone(),
                msg: format!(
                    "table metadata for operator '{}' table '{}' is missing table type",
                    operator_id, table_name
                ),
            });
        }
        TableEnum::GlobalKeyValue => {
            let metadata = GlobalKeyedTableTaskCheckpointMetadata::decode(metadata.data.as_slice())
                .map_err(|e| StoreError::DecodeProtobuf {
                    path: metadata_path.clone(),
                    source: e,
                })?;

            for file in metadata.files {
                files.push(CheckpointRef::new(file.clone())?);
            }
        }
        TableEnum::ExpiringKeyedTimeTable => {
            return Err(StoreError::InvalidProtobuf {
                path: metadata_path.clone(),
                msg: format!(
                    "table metadata for operator '{}' table '{}' has table type \
                ExpiringKeyedTimeTable, which is not yet supported in leader mode",
                    operator_id, table_name
                ),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::MemoryProtocolStore;
    use crate::store::{put_json, put_protobuf, read_protobuf};
    use crate::types::{CommittedMarker, EpochRecord, PROTOCOL_VERSION};
    use arroyo_rpc::grpc::rpc::{
        ExpiringKeyedTimeTableCheckpointMetadata, OperatorCheckpointMetadata, OperatorMetadata,
    };
    use arroyo_types::{JobId, PipelineId};

    fn paths() -> ProtocolPaths {
        ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"))
    }

    fn checkpoint_ref(paths: &ProtocolPaths, epoch: u64) -> CheckpointRef {
        paths.checkpoint_manifest(Generation(1), Epoch(epoch))
    }

    fn data_ref(epoch: u64) -> CheckpointRef {
        CheckpointRef::new(format!(
            "P/J/generations/1/checkpoints/checkpoint-{epoch:07}/operator-op/table-table-000"
        ))
        .unwrap()
    }

    fn global_operator(files: Vec<CheckpointRef>) -> OperatorCheckpointMetadata {
        OperatorCheckpointMetadata {
            operator_metadata: Some(OperatorMetadata {
                job_id: "J".to_string(),
                operator_id: "op".to_string(),
                epoch: 0,
                min_watermark: None,
                max_watermark: None,
                parallelism: 1,
            }),
            start_time: 0,
            finish_time: 0,
            table_checkpoint_metadata: [(
                "table".to_string(),
                TableCheckpointMetadata {
                    table_type: TableEnum::GlobalKeyValue.into(),
                    data: GlobalKeyedTableTaskCheckpointMetadata {
                        files: files.into_iter().map(|file| file.to_string()).collect(),
                        commit_data_by_subtask: Default::default(),
                    }
                    .encode_to_vec(),
                },
            )]
            .into(),
            table_configs: Default::default(),
        }
    }

    fn checkpoint(
        paths: &ProtocolPaths,
        epoch: u64,
        parent: Option<u64>,
        operators: Vec<OperatorCheckpointMetadata>,
    ) -> CheckpointManifest {
        CheckpointManifest {
            pipeline_id: "P".to_string(),
            job_id: "J".to_string(),
            generation: 1,
            epoch,
            min_epoch: epoch,
            start_time: 0,
            finish_time: 0,
            needs_commit: false,
            operators,
            parent_checkpoint_ref: parent.map(|epoch| checkpoint_ref(paths, epoch).to_string()),
        }
    }

    async fn write_checkpoint(
        store: &MemoryProtocolStore,
        paths: &ProtocolPaths,
        epoch: u64,
        parent: Option<u64>,
        operators: Vec<OperatorCheckpointMetadata>,
    ) {
        let this_checkpoint_ref = checkpoint_ref(paths, epoch);
        let checkpoint = checkpoint(paths, epoch, parent, operators);
        put_protobuf(store, &this_checkpoint_ref, &checkpoint)
            .await
            .unwrap();

        let parent_checkpoint_ref = parent.map(|epoch| checkpoint_ref(paths, epoch));
        let epoch_record = EpochRecord {
            version: PROTOCOL_VERSION,
            pipeline_id: PipelineId::new("P"),
            job_id: JobId::new("J"),
            epoch: Epoch(epoch),
            generation: Generation(1),
            parent_checkpoint_ref,
            checkpoint_ref: this_checkpoint_ref.clone(),
            created_at_micros: 0,
        };
        put_json(store, &paths.epoch_record(Epoch(epoch)), &epoch_record)
            .await
            .unwrap();

        let committed_marker = CommittedMarker::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Epoch(epoch),
            Generation(1),
            Generation(1),
            this_checkpoint_ref,
        );
        put_json(
            store,
            &paths.committed_marker(Generation(1), Epoch(epoch)),
            &committed_marker,
        )
        .await
        .unwrap();
    }

    async fn exists(store: &MemoryProtocolStore, path: &CheckpointRef) -> bool {
        store.read_bytes(path).await.unwrap().is_some()
    }

    #[tokio::test]
    async fn cleanup_deletes_only_checkpoints_below_new_min_epoch() {
        let store = MemoryProtocolStore::default();
        let paths = paths();
        let file1 = data_ref(1);
        let file2 = data_ref(2);
        let file3 = data_ref(3);

        store.put_bytes(&file1, b"1".to_vec()).await.unwrap();
        store.put_bytes(&file2, b"2".to_vec()).await.unwrap();
        store.put_bytes(&file3, b"3".to_vec()).await.unwrap();
        write_checkpoint(
            &store,
            &paths,
            1,
            None,
            vec![global_operator(vec![file1.clone()])],
        )
        .await;
        write_checkpoint(
            &store,
            &paths,
            2,
            Some(1),
            vec![global_operator(vec![file2.clone()])],
        )
        .await;
        write_checkpoint(
            &store,
            &paths,
            3,
            Some(2),
            vec![global_operator(vec![file3.clone()])],
        )
        .await;

        cleanup_leader_checkpoints(&store, &paths, checkpoint_ref(&paths, 3), Epoch(2))
            .await
            .unwrap();

        assert!(!exists(&store, &file1).await);
        assert!(!exists(&store, &paths.epoch_record(Epoch(1))).await);
        assert!(!exists(&store, &paths.committed_marker(Generation(1), Epoch(1))).await);
        assert!(
            read_protobuf::<_, CheckpointManifest>(&store, &checkpoint_ref(&paths, 1))
                .await
                .unwrap()
                .is_none()
        );

        assert!(exists(&store, &file2).await);
        assert!(exists(&store, &file3).await);
        assert!(
            read_protobuf::<_, CheckpointManifest>(&store, &checkpoint_ref(&paths, 2))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            read_protobuf::<_, CheckpointManifest>(&store, &checkpoint_ref(&paths, 3))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn cleanup_deletes_checkpoint_manifests_oldest_first() {
        let store = MemoryProtocolStore::default();
        let paths = paths();
        write_checkpoint(&store, &paths, 1, None, vec![global_operator(vec![])]).await;
        write_checkpoint(&store, &paths, 2, Some(1), vec![global_operator(vec![])]).await;
        write_checkpoint(&store, &paths, 3, Some(2), vec![global_operator(vec![])]).await;

        cleanup_leader_checkpoints(&store, &paths, checkpoint_ref(&paths, 3), Epoch(3))
            .await
            .unwrap();

        let deleted = store.deleted_objects();
        let manifest1 = checkpoint_ref(&paths, 1).to_string();
        let manifest2 = checkpoint_ref(&paths, 2).to_string();
        let manifest3 = checkpoint_ref(&paths, 3).to_string();
        let manifest1_pos = deleted
            .iter()
            .position(|path| path == &manifest1)
            .expect("epoch 1 manifest should be deleted");
        let manifest2_pos = deleted
            .iter()
            .position(|path| path == &manifest2)
            .expect("epoch 2 manifest should be deleted");

        assert!(manifest1_pos < manifest2_pos);
        assert!(!deleted.contains(&manifest3));
    }

    #[tokio::test]
    async fn cleanup_stops_at_missing_parent_manifest() {
        let store = MemoryProtocolStore::default();
        let paths = paths();
        write_checkpoint(&store, &paths, 3, Some(2), vec![global_operator(vec![])]).await;

        cleanup_leader_checkpoints(&store, &paths, checkpoint_ref(&paths, 3), Epoch(2))
            .await
            .unwrap();

        assert!(
            read_protobuf::<_, CheckpointManifest>(&store, &checkpoint_ref(&paths, 3))
                .await
                .unwrap()
                .is_some()
        );
        assert!(store.deleted_objects().is_empty());
    }

    #[tokio::test]
    async fn cleanup_detects_checkpoint_parent_cycles() {
        let store = MemoryProtocolStore::default();
        let paths = paths();
        write_checkpoint(&store, &paths, 1, Some(2), vec![global_operator(vec![])]).await;
        write_checkpoint(&store, &paths, 2, Some(1), vec![global_operator(vec![])]).await;

        let err = cleanup_leader_checkpoints(&store, &paths, checkpoint_ref(&paths, 2), Epoch(0))
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::Protocol(ProtocolError::CheckpointCycle {
                generation: Generation(1),
                epoch: Epoch(2)
            })
        ));
        assert!(store.deleted_objects().is_empty());
    }
}
