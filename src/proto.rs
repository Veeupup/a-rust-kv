use crate::error::KvsError;
use serde::{Deserialize, Serialize};

/// Operation Type
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// get
    GET {
        /// get key
        key: String,
    },
    /// put
    SET {
        /// set key
        key: String,
        /// set value
        value: String,
    },
    /// remove
    RM {
        /// remove key
        key: String,
    },
}

/// Response
#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    /// KvsError
    pub status: KvsError,
    /// status
    pub value: String,
}
