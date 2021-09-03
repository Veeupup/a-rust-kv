#![deny(missing_docs)]
//! A simple kv store

pub use engine::{KvStore, KvsEngine, KV};
pub use error::KvsError;
pub use error::Result;
pub use proto::{Request, Response, OpType};
pub use io::read_n;

mod engine;
mod error;
mod io;
mod proto;
