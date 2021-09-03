use std::ops::Deref;
use std::path::PathBuf;

use crate::KvsEngine;
use crate::KvsError;
use crate::Result;
use crate::io::own_dir_or_not;

/// Seld store
pub struct SledStore {
    db: sled::Db,
}

impl KvsEngine for SledStore {
    /// set kv pair
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let _ = self.db.insert(key.as_bytes(), value.as_bytes());
        self.db.flush().unwrap();
        Ok(())
    }
    /// get kv pair
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let result = self.db.get(key);
        match result {
            Ok(result) => {
                if let Some(value) = result {
                    let value = String::from(std::str::from_utf8(&value.deref()).unwrap());
                    return Ok(Some(value));
                }else {
                    return Ok(None)
                }
            }
            Err(_) => {
                return Ok(None);
            }
        }
    }
    /// remove kv pair
    fn remove(&mut self, key: String) -> Result<()> {
        match self.db.get(key.clone()).unwrap() {
            Some(_) => {},
            None => {
                return Err(KvsError::ErrKeyNotFound);
            }
        }
        self.db.remove(key).unwrap();
        self.db.flush().unwrap();
        Ok(())
    }
}

impl SledStore {
    /// open
    pub fn open(path: impl Into<PathBuf>) -> Result<SledStore> {
        let path = path.into();
        own_dir_or_not(path.clone(), "sled");
        let tree = sled::open(path).unwrap();
        Ok(SledStore { db: tree })
    }
}
