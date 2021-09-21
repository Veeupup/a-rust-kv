use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use crate::io::own_dir_or_not;
use crate::KvsEngine;
use crate::KvsError;
use crate::Result;

/// Sled store
/// A kv store based on seld
/// ```
/// use kvs::{SledStore, KvsEngine, Result};
/// use tempfile::TempDir;
///
/// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
/// // open a kv store from a path
/// let store = SledStore::open(temp_dir.path()).unwrap();
/// // set a kv
/// store.set("key1".to_owned(), "value1".to_owned()).unwrap();
/// // get a kv
/// assert_eq!(store.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
/// // rm a kv
/// assert!(store.remove("key1".to_owned()).is_ok());
/// // check it!
/// assert_eq!(store.get("key1".to_owned()).unwrap(), None);
/// ```
pub struct SledStore {
    db: Arc<Mutex<sled::Db>>,
}

impl Clone for SledStore {
    fn clone(&self) -> Self {
        SledStore {
            db: self.db.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.db = source.db.clone();
    }
}

impl KvsEngine for SledStore {
    /// set kv pair
    fn set(&self, key: String, value: String) -> Result<()> {
        let _ = self
            .db
            .lock()
            .unwrap()
            .insert(key.as_bytes(), value.as_bytes());
        self.db.lock().unwrap().flush().unwrap();
        Ok(())
    }
    /// get kv pair
    fn get(&self, key: String) -> Result<Option<String>> {
        let result = self.db.lock().unwrap().get(key);
        match result {
            Ok(result) => {
                if let Some(value) = result {
                    let value = String::from(std::str::from_utf8(&value.deref()).unwrap());
                    return Ok(Some(value));
                } else {
                    return Ok(None);
                }
            }
            Err(_) => {
                return Ok(None);
            }
        }
    }
    /// remove kv pair
    fn remove(&self, key: String) -> Result<()> {
        match self.db.lock().unwrap().get(key.clone()).unwrap() {
            Some(_) => {}
            None => {
                return Err(KvsError::ErrKeyNotFound);
            }
        }
        self.db.lock().unwrap().remove(key).unwrap();
        self.db.lock().unwrap().flush().unwrap();
        Ok(())
    }
}

impl SledStore {
    /// open
    pub fn open(path: impl Into<PathBuf>) -> Result<SledStore> {
        let path = path.into();
        own_dir_or_not(path.clone(), "sled");
        let tree = sled::open(path).unwrap();
        Ok(SledStore {
            db: Arc::new(Mutex::new(tree)),
        })
    }
}
