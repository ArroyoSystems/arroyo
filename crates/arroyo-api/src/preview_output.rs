use anyhow::anyhow;
use arroyo_rpc::api_types::pipelines::OutputData;
use arroyo_storage::StorageProvider;
use futures::StreamExt;
use std::collections::HashSet;

use crate::rest_utils::{ErrorResp, bad_request, log_and_map};

pub(crate) struct PreviewOutputPoller {
    storage: StorageProvider,
    seen: HashSet<String>,
}

impl PreviewOutputPoller {
    pub(crate) async fn new(base_path: &str, job_id: &str) -> Result<Self, ErrorResp> {
        if base_path.is_empty() {
            return Err(bad_request("preview output path must not be empty"));
        }

        let job_path = format!("{}/{job_id}", base_path.trim_end_matches('/'));
        let storage = StorageProvider::for_url(&job_path).await.map_err(|e| {
            log_and_map(anyhow!(
                "failed to access preview output path '{job_path}': {e}"
            ))
        })?;

        Ok(Self {
            storage,
            seen: HashSet::new(),
        })
    }

    pub(crate) async fn poll(&mut self) -> Result<Vec<OutputData>, ErrorResp> {
        let mut files = self
            .storage
            .list(true)
            .await
            .map_err(|e| log_and_map(anyhow!("failed to list preview output files: {e}")))?;
        let mut new_files = Vec::new();

        while let Some(file) = files.next().await {
            let file = file
                .map_err(|e| log_and_map(anyhow!("failed to list a preview output file: {e}")))?;
            let key = file.to_string();
            if key.ends_with(".json") && !self.seen.contains(&key) {
                new_files.push(file);
            }
        }
        drop(files);

        new_files.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        let object_store = self.storage.get_backing_store();
        let mut output = Vec::with_capacity(1);

        // Read one immutable chunk at a time so a slow SSE client applies
        // backpressure without the API buffering the whole preview.
        for file in new_files.into_iter().take(1) {
            let bytes = object_store
                .get(&file)
                .await
                .map_err(|e| {
                    log_and_map(anyhow!("failed to read preview output file '{file}': {e}"))
                })?
                .bytes()
                .await
                .map_err(|e| {
                    log_and_map(anyhow!("failed to read preview output file '{file}': {e}"))
                })?;
            let data = serde_json::from_slice(&bytes).map_err(|e| {
                log_and_map(anyhow!(
                    "failed to decode preview output file '{file}': {e}"
                ))
            })?;
            self.seen.insert(file.to_string());
            output.push(data);
        }

        Ok(output)
    }
}

pub(crate) fn flatten_preview_output(
    chunks: Vec<OutputData>,
) -> Result<Vec<serde_json::Value>, ErrorResp> {
    let mut output = Vec::new();
    for chunk in chunks {
        let rows: Vec<serde_json::Value> = serde_json::from_str(&chunk.batch).map_err(|e| {
            log_and_map(anyhow!(
                "failed to decode rows in preview output for operator '{}': {e}",
                chunk.operator_id
            ))
        })?;
        output.extend(rows);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn poller_replays_each_file_once() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("arroyo-preview-output-{unique}"));
        let path_string = path.to_string_lossy().to_string();
        let storage = StorageProvider::for_url(&path_string).await.unwrap();
        let data = OutputData {
            operator_id: "preview_1".to_string(),
            subtask_idx: 0,
            timestamps: vec![1],
            start_id: 0,
            batch: "[{\"id\":1}]".to_string(),
        };
        storage
            .put(
                "job_1/preview_1/0/00000000000000000000.json",
                serde_json::to_vec(&data).unwrap(),
            )
            .await
            .unwrap();
        let mut second = data.clone();
        second.start_id = 1;
        second.batch = "[{\"id\":2}]".to_string();
        storage
            .put(
                "job_1/preview_1/0/00000000000000000001.json",
                serde_json::to_vec(&second).unwrap(),
            )
            .await
            .unwrap();

        let mut poller = PreviewOutputPoller::new(&path_string, "job_1")
            .await
            .unwrap();
        let first = poller.poll().await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].start_id, 0);
        let second = poller.poll().await.unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].start_id, 1);
        assert!(poller.poll().await.unwrap().is_empty());

        std::fs::remove_dir_all(path).unwrap();
    }
}
