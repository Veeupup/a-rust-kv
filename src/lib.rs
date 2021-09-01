#![deny(missing_docs)]
//! A simple kv store

pub use error::KvsError;
pub use error::Result;
pub use kv::KvStore;

mod error;
mod io;
mod kv;
