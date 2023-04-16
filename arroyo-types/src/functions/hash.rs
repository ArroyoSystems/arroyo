use base64::{engine::general_purpose::STANDARD_NO_PAD as encoder, Engine};
use md5::{Digest, Md5};

// would like to make this parameterized based on the hasher, since everything else looks the same
// but I couldn't quite figure out what the common trait was between all of them...
// I'll ask later if anyone has ideas.
pub fn md5(argument: String) -> String {
    let mut hasher = Md5::new();
    hasher.update(argument.as_bytes());
    let res = hasher.finalize();
    // chose to encode as base64 to encode bytes to string, but not sure if that's correct
    encoder.encode(&res)
}

pub fn sha224(argument: String) -> String {
    let mut hasher = sha2::Sha224::new();
    hasher.update(argument.as_bytes());
    let res = hasher.finalize();
    // chose to encode as base64 to encode bytes to string, but not sure if that's correct
    encoder.encode(&res)
}

pub fn sha256(argument: String) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(argument.as_bytes());
    let res = hasher.finalize();
    // chose to encode as base64 to encode bytes to string, but not sure if that's correct
    encoder.encode(&res)
}

pub fn sha384(argument: String) -> String {
    let mut hasher = sha2::Sha384::new();
    hasher.update(argument.as_bytes());
    let res = hasher.finalize();
    // chose to encode as base64 to encode bytes to string, but not sure if that's correct
    encoder.encode(&res)
}

pub fn sha512(argument: String) -> String {
    let mut hasher = sha2::Sha512::new();
    hasher.update(argument.as_bytes());
    let res = hasher.finalize();
    // chose to encode as base64 to encode bytes to string, but not sure if that's correct
    encoder.encode(&res)
}
