use crate::test::get_test_schema_provider;
use crate::{parse_and_get_program, SqlConfig};
use rstest::rstest;
use std::path::{Path, PathBuf};
use tokio_stream::wrappers::ReadDirStream;
use tokio_stream::StreamExt;

#[rstest]
fn for_each_file(#[files("src/test/queries/*.sql")] path: PathBuf) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            validate_query(&path).await;
        });
}

async fn validate_query(path: &Path) {
    let query = tokio::fs::read_to_string(path).await.unwrap();
    let fail = query.starts_with("--fail");
    let error_message = query.starts_with("--fail=").then(|| {
        query
            .lines()
            .next()
            .unwrap()
            .split_once('=')
            .unwrap()
            .1
            .trim()
    });

    let mut schema_provider = get_test_schema_provider();

    let udfs: Vec<_> = ReadDirStream::new(tokio::fs::read_dir("src/test/udfs").await.unwrap())
        .map(|f| f.unwrap().path())
        .collect()
        .await;

    for udf_path in udfs {
        let udf = tokio::fs::read_to_string(&udf_path).await.unwrap();
        schema_provider.add_rust_udf(&udf, "").unwrap();
    }

    let result = parse_and_get_program(&query, schema_provider, SqlConfig::default()).await;

    if fail {
        let err = result.unwrap_err();
        if let Some(error_message) = error_message {
            let err_s = err.to_string();
            let err: Vec<_> = err_s.split_whitespace().collect();
            let err = err.join(" ");
            assert!(
                err.contains(error_message),
                "expected error message '{}' not found; instead got '{}'",
                error_message,
                err
            );
        }
    } else {
        result.unwrap();
    }
}
