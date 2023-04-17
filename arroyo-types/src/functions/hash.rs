use hex;
use md5::{Digest, Md5};

pub fn md5(argument: String) -> String {
    let mut hasher = Md5::new();
    hasher.update(argument.as_bytes());
    let res = hasher.finalize();
    hex::encode(&res)
}

pub fn sha224(argument: Vec<u8>) -> Vec<u8> {
    let mut hasher = sha2::Sha224::new();
    hasher.update(argument);
    hasher.finalize().to_vec()
}

pub fn sha256(argument: Vec<u8>) -> Vec<u8> {
    let mut hasher = sha2::Sha256::new();
    hasher.update(argument);
    hasher.finalize().to_vec()
}

pub fn sha384(argument: Vec<u8>) -> Vec<u8> {
    let mut hasher = sha2::Sha384::new();
    hasher.update(argument);
    hasher.finalize().to_vec()
}

pub fn sha512(argument: Vec<u8>) -> Vec<u8> {
    let mut hasher = sha2::Sha512::new();
    hasher.update(argument);
    hasher.finalize().to_vec()
}
