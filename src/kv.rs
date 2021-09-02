use crate::error::{KvsError, Result};
use crate::io::{read_n, write_kv};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::{
    collections::HashMap,
    fs::{self, File},
    path::Path,
};

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// store.remove("key".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, None);
/// ```
pub struct KvStore {
    index: HashMap<String, u64>,
    write_handler: File,
    read_handler: File,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KV {
    version: u32,
    key: String,
    value: String,
}

impl KV {
    fn new(key: String, value: String, version: u32) -> KV {
        KV {
            version: version,
            key: key,
            value: value,
        }
    }
}

impl KvStore {
    /// create a kv store
    pub fn new() -> KvStore {
        let path = Path::new(".");
        return KvStore::open(path).unwrap();
    }

    /// Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let kv = KV::new(key.clone(), val, 1);
        let len = self.write_handler.metadata().unwrap().len();
        self.index.insert(key, len);
        write_kv(&mut self.write_handler, kv);
        Ok(())
    }

    /// Set the value of a string key to a string. Return an error if the value is not written successfully.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(index) = self.index.get(&key).cloned() {
            return self.get_value_by_index(index);
        }
        return Ok(None);
    }

    fn get_value_by_index(&mut self, index: u64) -> Result<Option<String>> {
        self.read_handler.seek(SeekFrom::Start(index)).unwrap();
        let mut meta_buffer: [u8; 4] = [0; 4]; // 8 byte
        self.read_handler.read(&mut meta_buffer).unwrap();
        let key_len = u32::from_be_bytes(meta_buffer);
        let data = read_n(&mut self.read_handler, key_len as u64);
        let kv: KV = serde_json::from_slice(&data).unwrap();
        if kv.version == 0 {
            return Ok(None);
        } else {
            return Ok(Some(kv.value));
        }
    }

    /// Remove a given key. Return an error if the key does not exist or is not removed successfully.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let result = self.get(key.clone()).unwrap();
        match result {
            Some(_) => {}
            None => {
                return Err(KvsError::KeyNotFound);
            }
        }
        let len = self.write_handler.metadata().unwrap().len();
        self.index.insert(key.clone(), len);
        let kv = KV::new(key, "".to_owned(), 0);
        write_kv(&mut self.write_handler, kv);
        Ok(())
    }

    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut path: PathBuf = path.into();
        path.push("store.txt");
        let write_handler = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path.clone())
            .unwrap_or_else(|err| {
                panic!("can not open the path : {}", err);
            });
        let read_handler = fs::OpenOptions::new()
            .read(true)
            .open(path)
            .unwrap_or_else(|err| {
                panic!("can not open the path : {}", err);
            });
        let mut store = KvStore {
            index: HashMap::new(),
            write_handler: write_handler,
            read_handler: read_handler,
        };
        store.init();
        Ok(store)
    }

    /// init kvstore, read all index into memory
    pub fn init(&mut self) {
        self.read_all_index();
    }

    fn read_all_index(&mut self) {
        self.read_handler.seek(SeekFrom::Start(0)).unwrap();
        let mut offset = 0;
        loop {
            let mut meta_buffer: [u8; 4] = [0; 4]; // 8 byte
            self.read_handler.read(&mut meta_buffer).unwrap();
            let key_len = u32::from_be_bytes(meta_buffer);
            if key_len == 0 {
                break;
            }
            let data = read_n(&mut self.read_handler, key_len as u64);
            let kv: KV = serde_json::from_slice(&data).unwrap();
            self.index.insert(kv.key, offset);
            offset += 4 + key_len as u64;
        }
    }
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
