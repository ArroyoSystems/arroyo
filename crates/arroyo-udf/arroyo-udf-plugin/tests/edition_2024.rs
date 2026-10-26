use arroyo_udf_plugin::udf;

#[udf]
fn sync_identity(value: i64) -> i64 {
    value
}

#[udf]
async fn async_identity(value: i64) -> i64 {
    tokio::task::yield_now().await;
    value
}

#[tokio::test]
async fn exported_udfs_compile_in_edition_2024() {
    assert_eq!(sync_identity(42), 42);
    assert_eq!(async_identity(42).await, 42);
}
