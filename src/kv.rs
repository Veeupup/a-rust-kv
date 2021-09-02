use crate::error::{KvsError, Result};
use crate::io::{get_sst_from_dir, read_n, write_kv};
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
    index_new: HashMap<String, FileOffset>,
    write_handler: File,
    current_dir: PathBuf,
    current_write_file: String,
    sst_files: Vec<String>,
}

struct FileOffset {
    file: String,
    offset: u64,
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
        // self.index.insert(key, len);
        self.index_new.insert(
            key,
            FileOffset {
                file: self.current_write_file.clone(),
                offset: len,
            },
        );
        write_kv(&mut self.write_handler, kv);
        Ok(())
    }

    /// Set the value of a string key to a string. Return an error if the value is not written successfully.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(fo) = self.index_new.get(&key) {
            return KvStore::get_value_by_file_index(
                self.current_dir.clone(),
                fo.file.clone(),
                fo.offset,
            );
        }
        return Ok(None);
    }

    fn get_value_by_file_index(
        current_dir: PathBuf,
        filename: String,
        offset: u64,
    ) -> Result<Option<String>> {
        let filename = current_dir.clone().join(filename);
        let mut reader = fs::OpenOptions::new()
            .read(true)
            .open(filename)
            .unwrap_or_else(|err| {
                panic!("can not open the path : {}", err);
            });
        reader.seek(SeekFrom::Start(offset)).unwrap();
        let mut meta_buffer: [u8; 4] = [0; 4]; // 8 byte
        reader.read(&mut meta_buffer).unwrap();
        let key_len = u32::from_be_bytes(meta_buffer);
        let data = read_n(&mut reader, key_len as u64);
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
        // self.index.insert(key.clone(), len);
        self.index_new.insert(
            key.clone(),
            FileOffset {
                file: self.current_write_file.clone(),
                offset: len,
            },
        );
        let kv = KV::new(key, "".to_owned(), 0);
        write_kv(&mut self.write_handler, kv);
        Ok(())
    }

    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        // 应该根据传入的 目录，保存这个路径, 并且在之后进行读取每个文件
        let path: PathBuf = path.into();

        let mut sst_files = get_sst_from_dir(path.clone());
        if sst_files.is_empty() {
            sst_files.push("sst_1".to_owned());
        }

        let write_file = sst_files.last().cloned().unwrap();
        let write_file_path = path.join(write_file.clone());

        let write_handler = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(write_file_path)
            .unwrap_or_else(|err| {
                panic!("can not open the path : {}", err);
            });
        let mut store = KvStore {
            index_new: HashMap::new(),
            write_handler: write_handler,
            current_write_file: write_file,
            current_dir: path,
            sst_files: sst_files,
        };
        store.init();
        Ok(store)
    }

    /// init kvstore, read all index into memory
    pub fn init(&mut self) {
        self.read_all_index();
    }

    fn read_all_index(&mut self) {
        for file in &self.sst_files {
            let file = file.clone();
            let current_dir = self.current_dir.clone();
            let path = current_dir.join(file.clone());

            let mut read_handler = fs::OpenOptions::new().read(true).open(path).unwrap();

            let mut offset = 0;
            loop {
                let mut meta_buffer: [u8; 4] = [0; 4];
                read_handler.read(&mut meta_buffer).unwrap();
                let key_len = u32::from_be_bytes(meta_buffer);
                if key_len == 0 {
                    break;
                }
                let data = read_n(&mut read_handler, key_len as u64);
                let kv: KV = serde_json::from_slice(&data).unwrap();
                self.index_new.insert(
                    kv.key,
                    FileOffset {
                        file: file.clone(),
                        offset: offset,
                    },
                );
                offset += 4 + key_len as u64;
            }
        }
    }
}
