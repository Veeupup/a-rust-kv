extern crate failure;
use failure::Fail;
use serde::{Deserialize, Serialize};

/// Error in Kvs Store
#[derive(Serialize, Deserialize, Debug)]
#[derive(Fail)]
pub enum KvsError {
    /// a
    #[fail(display = "File not found")]
    ErrStoreFileNotFound,
    ///
    #[fail(display = "Key not found")]
    ErrKeyNotFound,
    /// ok
    #[fail(display = "OK")]
    ErrOk,
}

/// The Result for kvs store
pub type Result<T> = std::result::Result<T, KvsError>;
