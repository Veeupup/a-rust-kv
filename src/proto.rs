use serde::{Deserialize, Serialize};
use crate::error::KvsError;

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
    /// status
    pub status: KvsError,
    /// status
    pub value: String,
}
