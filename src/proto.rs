use crate::error::KvsError;
use serde::{Deserialize, Serialize};

/// Operation Type
#[derive(Serialize, Deserialize, Debug)]
pub enum OpType {
    /// get
    GET,
    /// put
    SET,
    /// remove
    RM,
}

/// Request
#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    /// optype
    pub op: OpType,
    /// key
    pub key: String,
    /// value
    pub value: String,
}

/// Response
#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    /// KvsError
    pub status: KvsError,
    /// status
    pub value: String,
}
