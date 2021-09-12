pub use kvstore::{KV, KvStore};
pub use self::sled::SledStore;

use crate::error::Result;

/// KvsEngine
pub trait KvsEngine: Clone + Send + 'static {
    /// set kv pair
    fn set(&self, key: String, value: String) -> Result<()>;
    /// get kv pair
    fn get(&self, key: String) -> Result<Option<String>>;
    /// remove kv pair
    fn remove(&self, key: String) -> Result<()>; 
}


mod kvstore;
mod sled;