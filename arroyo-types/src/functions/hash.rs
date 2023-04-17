use base64::{engine::general_purpose::STANDARD_NO_PAD as encoder, Engine};
use md5::{Digest, Md5};

pub fn md5(argument: String) -> String {
    let mut hasher = Md5::new();
    hasher.update(argument.as_bytes());
    let res = hasher.finalize();
    encoder.encode(&res)
}

pub fn sha224(argument: String) -> String {
    let mut hasher = sha2::Sha224::new();
    hasher.update(argument.as_bytes());
    let res = hasher.finalize();
    encoder.encode(&res)
}

pub fn sha256(argument: String) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(argument.as_bytes());
    let res = hasher.finalize();
    encoder.encode(&res)
}

pub fn sha384(argument: String) -> String {
    let mut hasher = sha2::Sha384::new();
    hasher.update(argument.as_bytes());
    let res = hasher.finalize();
    encoder.encode(&res)
}

pub fn sha512(argument: String) -> String {
    let mut hasher = sha2::Sha512::new();
    hasher.update(argument.as_bytes());
    let res = hasher.finalize();
    encoder.encode(&res)
}
