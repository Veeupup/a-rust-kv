#![deny(missing_docs)]
//! A simple kv store

pub use engine::{KvStore, KvsEngine, SledStore, KV};
pub use error::{KvsError, Result};
pub use proto::{Request, Response, OpType};
pub use server::KvServer;
pub use client::KvsClient;

mod engine;
mod error;
mod io;
mod proto;
mod server;
mod client;
/// thread pool
pub mod thread_pool;
