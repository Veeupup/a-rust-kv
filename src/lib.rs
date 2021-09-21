#![deny(missing_docs)]
//! A simple kv store

pub use client::KvsClient;
pub use engine::{KvStore, KvsEngine, SledStore, KV};
pub use error::{KvsError, Result};
pub use proto::{Request, Response};
pub use server::KvServer;

mod client;
mod engine;
mod error;
mod io;
mod proto;
mod server;
/// thread pool
pub mod thread_pool;
