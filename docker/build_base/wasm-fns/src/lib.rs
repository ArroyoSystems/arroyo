use std::time::SystemTime;
use types::*;
extern "C" {
    pub fn send(s: *const u8, length: u32) -> i32;
    pub fn fetch_data(s: *const u8) -> i32;
}
fn get_record<K: arroyo_types::Key, T: arroyo_types::Data>(len: u32) -> arroyo_types::Record<K, T> {
    let mut data = vec![0u8; len as usize];
    unsafe { fetch_data(data.as_mut_ptr()) };
    let (record, _): (arroyo_types::Record<K, T>, usize) =
        bincode::decode_from_slice(&data[..], bincode::config::standard()).unwrap();
    record
}
fn send_record<K: arroyo_types::Key, T: arroyo_types::Data>(record: arroyo_types::Record<K, T>) {
    let encoded = bincode::encode_to_vec(record, bincode::config::standard()).unwrap();
    unsafe { send(encoded.as_ptr(), encoded.len() as u32) };
}
