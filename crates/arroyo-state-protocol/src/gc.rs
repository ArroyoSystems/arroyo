use crate::ProtocolPaths;
use crate::store::{ProtocolStore, StoreError, read_protobuf};
use crate::types::{CheckpointRef, Epoch, Generation, ProtocolError};
use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, ExpiringKeyedTimeTableCheckpointMetadata,
    GlobalKeyedTableTaskCheckpointMetadata, TableCheckpointMetadata, TableEnum,
};
use futures::{TryStreamExt, stream};
use prost::Message;
use std::collections::HashSet;
use std::path::Path;
use tracing::debug;

const MAX_CONCURRENT_DELETES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct CheckpointOwner {
    pub generation: Generation,
    pub epoch: Epoch,
}

/// A fully classified GC operation. Classification must complete before any deletion begins.
struct CleanupPlan {
    /// Checkpoints below the retention boundary, ordered newest to oldest by traversal order.
    old_checkpoints: Vec<CheckpointOwner>,
    /// Deduplicated files referenced only by old checkpoints.
    data_files: Vec<CheckpointRef>,
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
    let cleanup = classify_checkpoint_history(store, head, new_min_epoch).await?;

    let data_directories: HashSet<_> = cleanup
        .data_files
        .iter()
        .filter_map(|f| Path::new(f.as_str()).parent().and_then(|p| p.to_str()))
        .map(|f| f.to_string())
        .collect();
    let checkpoint_directories: HashSet<_> = data_directories
        .iter()
        .filter_map(|directory| Path::new(directory).parent().and_then(|p| p.to_str()))
        .map(|directory| directory.to_string())
        .collect();

    let mut objects = cleanup.data_files;
    for checkpoint in &cleanup.old_checkpoints {
        objects.push(paths.committed_marker(checkpoint.generation, checkpoint.epoch));
        objects.push(paths.epoch_record(checkpoint.epoch));
    }

    delete_objects(store, objects).await?;

    for directory in data_directories {
        store.delete_directory(&directory).await;
    }

    // A carried-forward file may live under a checkpoint whose manifest was deleted by an earlier
    // GC pass. Retry its checkpoint directory now that the file and its operator directory are
    // gone. Local storage removes only empty directories; object stores treat this as a no-op.
    for directory in checkpoint_directories {
        store.delete_directory(&directory).await;
    }

    // Traversal records checkpoints newest-to-oldest. Delete manifests in reverse so a failed
    // cleanup cannot create a gap that makes still-reachable older checkpoints undiscoverable.
    for c in cleanup.old_checkpoints.iter().rev() {
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

async fn delete_objects<S>(store: &S, objects: Vec<CheckpointRef>) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
{
    stream::iter(objects.into_iter().map(Ok::<_, StoreError>))
        .try_for_each_concurrent(MAX_CONCURRENT_DELETES, |object| async move {
            store.delete_object(&object).await
        })
        .await
}

/// Traverses the reachable checkpoint history and classifies all files before deleting anything.
///
/// Data files referenced by retained checkpoints are protected even if an older checkpoint also
/// references them. This intentionally buffers deduplicated candidate and protected file refs for
/// the reachable chain, but not full manifests. That memory cost is required to safely handle
/// cumulative table metadata such as expiring keyed-time tables.
async fn classify_checkpoint_history<S>(
    store: &S,
    current: CheckpointRef,
    new_min_epoch: Epoch,
) -> Result<CleanupPlan, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let mut head = true;
    let mut old_checkpoints = vec![];
    let mut candidate_files = HashSet::new();
    let mut protected_files = HashSet::new();
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

        let owner = CheckpointOwner {
            generation: Generation(manifest.generation),
            epoch: Epoch(manifest.epoch),
        };

        if head && owner.epoch < new_min_epoch {
            return Err(ProtocolError::CheckpointGcMinEpochBeyondHead {
                head_epoch: owner.epoch,
                new_min_epoch,
            }
            .into());
        }

        head = false;

        if !seen.insert(owner) {
            return Err(ProtocolError::CheckpointCycle {
                generation: owner.generation,
                epoch: owner.epoch,
            }
            .into());
        }

        let files = checkpoint_data_files(&checkpoint_ref, &manifest)?;
        if manifest.epoch < *new_min_epoch {
            old_checkpoints.push(owner);
            candidate_files.extend(files);
        } else {
            protected_files.extend(files);
        }

        next = manifest
            .parent_checkpoint_ref
            .map(CheckpointRef::new)
            .transpose()?;
    }

    candidate_files.retain(|file| !protected_files.contains(file));

    Ok(CleanupPlan {
        old_checkpoints,
        data_files: candidate_files.into_iter().collect(),
    })
}

fn checkpoint_data_files(
    manifest_path: &CheckpointRef,
    checkpoint: &CheckpointManifest,
) -> Result<Vec<CheckpointRef>, StoreError> {
    let mut files = vec![];
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
                &mut files,
            )?;
        }
    }

    Ok(files)
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
            let metadata =
                ExpiringKeyedTimeTableCheckpointMetadata::decode(metadata.data.as_slice())
                    .map_err(|e| StoreError::DecodeProtobuf {
                        path: metadata_path.clone(),
                        source: e,
                    })?;

            for file in metadata.files {
                files.push(CheckpointRef::new(file.file)?);
            }
        }
    }

    Ok(())
}
