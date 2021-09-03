use crate::error::Result;

/// KvsEngine
pub trait KvsEngine {
    /// set kv pair
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// get kv pair
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// remove kv pair
    fn remove(&mut self, key: String) -> Result<()>; 
}
