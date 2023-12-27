use arrow_schema::SchemaRef;

pub struct SchemaRefWithMeta {
    schema: SchemaRef,
    timestamp_index: usize,
    key_indices: Vec<usize>,
}
