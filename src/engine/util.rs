use serde::{Deserialize, Serialize};

/// KV for kvstore
#[derive(Serialize, Deserialize)]
pub struct KV {
    /// version
    pub version: u32,
    /// key
    pub key: String,
    /// value
    pub value: String,
}

impl KV {
    /// new KV
    pub fn new(key: String, value: String, version: u32) -> KV {
        KV {
            version: version,
            key: key,
            value: value,
        }
    }
}
