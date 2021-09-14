extern crate failure;
use failure::Fail;
use serde::{Deserialize, Serialize};

/// Error in Kvs Store
#[derive(Serialize, Deserialize, Debug, Fail)]
pub enum KvsError {
    /// Key not found
    #[fail(display = "Key not found")]
    ErrKeyNotFound,
    /// OK
    #[fail(display = "OK")]
    ErrOk,
}

/// The Result for kvs store
pub type Result<T> = std::result::Result<T, KvsError>;
