use crate::ProtocolPaths;
use crate::store::{ProtocolStore, StoreError, read_protobuf};
use crate::types::{CheckpointRef, Epoch, Generation, ProtocolError};
use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, GlobalKeyedTableTaskCheckpointMetadata, TableCheckpointMetadata, TableEnum,
};
use futures::future::join_all;
use prost::Message;
use std::collections::HashSet;
use std::path::Path;
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

    for c in history.iter().rev() {
        debug!(
            generation = c.generation.0,
            epoch = c.epoch.0,
            "cleaning checkpoint"
        );

        let path = paths.checkpoint_manifest(c.generation, c.epoch);
        store.delete_object(&path).await?;

        let dir = paths.checkpoint_dir(c.generation, c.epoch);
        store.delete_directory(dir.as_str()).await;
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
    let mut head = true;
    let mut history = vec![];
    let mut seen = HashSet::new();
    let mut next = Some(current);

    while let Some(checkpoint_ref) = next {
        // TODO: use a metadata cache here so we're not re-reading checkpoints we've just written
        let Some(manifest): Option<CheckpointManifest> =
            read_protobuf(store, &checkpoint_ref).await?
        else {
            if head {
                return Err(StoreError::ExpectedObjectMissing {
                    path: checkpoint_ref,
                });
            }
            break;
        };

        head = false;

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

        if manifest.epoch < *new_min_epoch {
            history.push(owner);
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
            .map(CheckpointRef::new)
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

    let directories: HashSet<_> = to_delete
        .iter()
        .filter_map(|f| Path::new(f.as_str()).parent().and_then(|p| p.to_str()))
        .map(|f| f.to_string())
        .collect();

    to_delete
        .push(paths.committed_marker(Generation(checkpoint.generation), Epoch(checkpoint.epoch)));

    to_delete.push(paths.epoch_record(Epoch(checkpoint.epoch)));

    for r in join_all(to_delete.iter().map(|f| store.delete_object(f))).await {
        r?;
    }

    for d in directories {
        // this will loop over duplicate directory when there are multiples tables per operator,
        // but it's a no-op if the directory is already deleted
        store.delete_directory(&d).await;
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
