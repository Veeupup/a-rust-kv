extern crate failure;
use failure::Fail;

/// Error in Kvs Store
#[derive(Debug, Fail)]
pub enum KvsError {
    /// a
    #[fail(display = "File not found")]
    StoreFileNotFound,
    ///
    #[fail(display = "Key not found")]
    KeyNotFound,
}

/// The Result for kvs store
pub type Result<T> = std::result::Result<T, KvsError>;
