/*
[dependencies]
serde_cbor = "0.11"
serde_json = "1"
serde = {version = "1", features = ["derive"]}
serde-transcode = "1"
*/

use arroyo_udf_plugin::udf;


#[udf]
fn cbor_to_json(data: &[u8]) -> Option<String> {
    let mut deserializer = serde_cbor::Deserializer::from_slice(&data[..]);
    let mut buf = std::io::BufWriter::new(Vec::new());
    let mut serializer = serde_json::Serializer::new(&mut buf);
    serde_transcode::transcode(&mut deserializer, &mut serializer).ok()?;
    let bytes = buf.into_inner().unwrap();

    Some(String::from_utf8(bytes).ok()?)
}
