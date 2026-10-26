use nanoid::nanoid;

const ID_LENGTH: usize = 10;

pub const MAX_PUBLIC_ID_LENGTH: usize = 255;

const ALPHABET: [char; 62] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
    'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b',
    'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
    'v', 'w', 'x', 'y', 'z',
];

pub enum IdTypes {
    ApiKey,
    ConnectionProfile,
    Schema,
    Pipeline,
    JobConfig,
    Checkpoint,
    JobStatus,
    ClusterInfo,
    JobLogMessage,
    ConnectionTable,
    ConnectionTablePipeline,
    Udf,
}

pub fn validate_public_id(id: &str) -> Result<(), &'static str> {
    if id.is_empty() {
        return Err("id must not be empty");
    }

    if id.len() > MAX_PUBLIC_ID_LENGTH {
        return Err("id must be at most 255 bytes");
    }

    if id == "." || id == ".." {
        return Err("id must not be a relative path segment");
    }

    if id.contains('/') || id.contains('\\') {
        return Err("id must not contain path separators");
    }

    if id.chars().any(char::is_control) {
        return Err("id must not contain control characters");
    }

    Ok(())
}

pub fn generate_id(id_type: IdTypes) -> String {
    let prefix = match id_type {
        IdTypes::ApiKey => "ak",
        IdTypes::ConnectionProfile => "cp",
        IdTypes::Schema => "sch",
        IdTypes::Pipeline => "pl",
        IdTypes::JobConfig => "job",
        IdTypes::Checkpoint => "chk",
        IdTypes::JobStatus => "js",
        IdTypes::ClusterInfo => "ci",
        IdTypes::JobLogMessage => "jlm",
        IdTypes::ConnectionTable => "ct",
        IdTypes::ConnectionTablePipeline => "ctp",
        IdTypes::Udf => "udf",
    };
    let id = nanoid!(ID_LENGTH, &ALPHABET);
    format!("{prefix}_{id}")
}
