use crate::engine::KvsEngine;
use crate::error::{KvsError, Result};
use crate::io::{get_sst_from_dir_with_prefix, own_dir_or_not, read_n, write_kv};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::{
    collections::HashMap,
    fs::{self, File},
};

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory
///
pub struct KvStore {
    index: Arc<RwLock<HashMap<String, FileOffset>>>,
    write_handler: Arc<RwLock<File>>,
    current_dir: Arc<RwLock<PathBuf>>,
    current_write_file: Arc<RwLock<u64>>,
}

struct FileOffset {
    file: u64,
    offset: u64,
}

/// KV for kvstore
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

const ONE_SST_FILE_MAX_SIZE: u64 = 1024 * 1024 * 10;

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore {
            index: self.index.clone(),
            write_handler: self.write_handler.clone(),
            current_write_file: self.current_write_file.clone(),
            current_dir: self.current_dir.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.index = source.index.clone();
        self.write_handler = source.write_handler.clone();
        self.current_write_file = source.current_write_file.clone();
        self.current_dir = source.current_dir.clone();
    }
}

impl KvsEngine for KvStore {
    /// set kv pair
    /// Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.
    fn set(&self, key: String, val: String) -> Result<()> {
        self.compaction();

        let mut index = self.index.write().unwrap();
        let mut write_handler = self.write_handler.write().unwrap();
        let current_write_file = self.current_write_file.write().unwrap();

        let len = write_handler.metadata().unwrap().len();
        let kv = KV::new(key.clone(), val, 1);
        index.insert(
            key,
            FileOffset {
                file: current_write_file.clone(),
                offset: len,
            },
        );
        write_kv(&mut write_handler, kv);

        Ok(())
    }
    /// get kv pair
    /// Set the value of a string key to a string. Return an error if the value is not written successfully.
    fn get(&self, key: String) -> Result<Option<String>> {
        let index = self.index.read().unwrap();
        let current_dir = self.current_dir.read().unwrap();

        if let Some(fo) = index.get(&key) {
            return KvStore::get_value_by_file_index(
                current_dir.clone(),
                fo.file.clone(),
                fo.offset,
            );
        }
        return Ok(None);
    }
    /// remove kv pair
    /// Remove a given key. Return an error if the key does not exist or is not removed successfully.
    fn remove(&self, key: String) -> Result<()> {
        let mut index = self.index.write().unwrap();
        let mut write_handler = self.write_handler.write().unwrap();
        let current_write_file = self.current_write_file.write().unwrap();

        let idx = index.get(&key);
        match idx {
            Some(_) => {}
            None => return Err(KvsError::ErrKeyNotFound),
        }

        let len = write_handler.metadata().unwrap().len();
        index.insert(
            key.clone(),
            FileOffset {
                file: current_write_file.clone(),
                offset: len,
            },
        );
        let kv = KV::new(key, "".to_owned(), 0);
        write_kv(&mut write_handler, kv);
        Ok(())
    }
}

impl KvStore {
    fn compaction(&self) {
        let mut index = self.index.write().unwrap();
        let mut write_handler = self.write_handler.write().unwrap();
        let current_dir = self.current_dir.write().unwrap();
        let mut current_write_file = self.current_write_file.write().unwrap();

        let len = write_handler.metadata().unwrap().len();
        if len < ONE_SST_FILE_MAX_SIZE {
            return;
        }

        // 由于已经有了所有的 index 信息在内存中，所以可以直接构造新的 sst files，并且更新新的 sst files
        let mut index_new: HashMap<String, FileOffset> = HashMap::new();

        let mut file_idx = 1;
        let mut tmp_filename = format!("tmpsst_{}", file_idx);
        let mut tmp_file = current_dir.clone().join(tmp_filename);
        let mut tmp_write_handler = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(tmp_file)
            .unwrap();

        for (key, _) in &*index {
            // 只处理仍然存在的 key，如果 key 不存在或者被删除了，那么就不需要写到新的里面去了
            if let Some(fo) = index.get(key) {
                let value = KvStore::get_value_by_file_index(
                    current_dir.clone(),
                    fo.file.clone(),
                    fo.offset,
                )
                .unwrap()
                .unwrap();

                let len = tmp_write_handler.metadata().unwrap().len();
                let kv = KV::new(key.clone(), value, 1);
                index_new.insert(
                    key.clone(),
                    FileOffset {
                        file: file_idx,
                        offset: len,
                    },
                );
                write_kv(&mut tmp_write_handler, kv);
                if len > ONE_SST_FILE_MAX_SIZE {
                    file_idx += 1;
                    tmp_filename = format!("tmpsst_{}", file_idx);
                    tmp_file = current_dir.clone().join(tmp_filename);
                    tmp_write_handler = fs::OpenOptions::new()
                        .write(true)
                        .append(true)
                        .create(true)
                        .open(tmp_file)
                        .unwrap();
                }
            }
        }

        // 先把原来的 sst 都删除了
        let files = get_sst_from_dir_with_prefix(current_dir.clone(), "sst_".to_owned());

        for file in files {
            let file = current_dir.clone().join(file);
            fs::remove_file(file).unwrap();
        }
        // 现在已经全部写到了新的里面去了，删除一下之前的文件并且 rename 一下新的临时文件即可
        let files = get_sst_from_dir_with_prefix(current_dir.clone(), "tmp".to_owned());

        for (idx, file) in files.iter().enumerate() {
            let tmp_file = current_dir.clone().join(file);
            let sst_file = current_dir.clone().join(format!("sst_{}", idx + 1));
            fs::rename(tmp_file, sst_file).unwrap();
        }

        // 更新 index， 换 write_handler
        *index = index_new;
        let write_file = format!("sst_{}", files.len());
        let write_file_path = current_dir.clone().join(write_file.clone());
        *write_handler = fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(write_file_path)
            .unwrap();
        *current_write_file = files.len() as u64;
    }

    fn get_value_by_file_index(
        current_dir: PathBuf,
        file_idx: u64,
        offset: u64,
    ) -> Result<Option<String>> {
        let filename = format!("sst_{}", file_idx);
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

    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        // 应该根据传入的 目录，保存这个路径, 并且在之后进行读取每个文件
        let path: PathBuf = path.into();

        own_dir_or_not(path.clone(), "kvs");

        let mut sst_files = get_sst_from_dir_with_prefix(path.clone(), "sst".to_owned());
        if sst_files.is_empty() {
            sst_files.push("sst_1".to_owned());
        }

        let write_file = sst_files.last().cloned().unwrap();
        let write_file_path = path.join(write_file.clone());

        let pos = write_file.find("_").unwrap();
        let file_idx = write_file[(pos + 1)..].parse::<u64>().unwrap();

        let write_handler = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(write_file_path)
            .unwrap_or_else(|err| {
                panic!("can not open the path : {}", err);
            });
        let store = KvStore {
            index: Arc::new(RwLock::new(HashMap::new())),
            write_handler: Arc::new(RwLock::new(write_handler)),
            current_write_file: Arc::new(RwLock::new(file_idx)),
            current_dir: Arc::new(RwLock::new(path)),
        };
        store.init();
        Ok(store)
    }

    /// init kvstore, read all index into memory
    pub fn init(&self) {
        let mut index = self.index.write().unwrap();
        *index = self.read_all_index();
    }

    fn read_all_index(&self) -> HashMap<String, FileOffset> {
        let current_dir = self.current_dir.write().unwrap();

        let mut index: HashMap<String, FileOffset> = HashMap::new();
        let sst_files = get_sst_from_dir_with_prefix(current_dir.clone(), "sst".to_owned());
        for file in sst_files {
            let pos = file.find("_").unwrap();
            let file_idx = file[(pos + 1)..].parse::<u64>().unwrap();

            let file = file.clone();
            let current_dir = current_dir.clone();
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
                index.insert(
                    kv.key,
                    FileOffset {
                        file: file_idx,
                        offset: offset,
                    },
                );
                offset += 4 + key_len as u64;
            }
        }
        return index;
    }
}
