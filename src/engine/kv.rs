use crate::error::{KvsError, Result};
use crate::io::{get_sst_from_dir_with_prefix, read_n, write_kv};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::{
    collections::HashMap,
    fs::{self, File},
};

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// use std::env::current_dir;
/// let mut store = KvStore::open(current_dir().unwrap()).unwrap();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned()).unwrap();
/// assert_eq!(val, Some("value".to_owned()));
/// store.remove("key".to_owned());
/// let val = store.get("key".to_owned()).unwrap();
/// assert_eq!(val, None);
/// ```
pub struct KvStore {
    index: HashMap<String, FileOffset>,
    write_handler: File,
    current_dir: PathBuf,
    current_write_file: u64,
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

const ONE_SST_FILE_MAX_SIZE: u64 = 1024 * 1024;

impl KvStore {
    /// Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let kv = KV::new(key.clone(), val, 1);
        let len = self.write_handler.metadata().unwrap().len();
        self.index.insert(
            key,
            FileOffset {
                file: self.current_write_file.clone(),
                offset: len,
            },
        );
        write_kv(&mut self.write_handler, kv);
        if len > ONE_SST_FILE_MAX_SIZE {
            self.compaction();
        }
        Ok(())
    }

    fn compaction(&mut self) {
        // 由于已经有了所有的 index 信息在内存中，所以可以直接构造新的 sst files，并且更新新的 sst files
        let mut index_new: HashMap<String, FileOffset> = HashMap::new();

        let mut file_idx = 1;
        let mut tmp_filename = format!("tmpsst_{}", file_idx);
        let mut tmp_file = self.current_dir.clone().join(tmp_filename);
        let mut write_handler = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(tmp_file)
            .unwrap();

        for (key, _) in &self.index {
            // 只处理仍然存在的 key，如果 key 不存在或者被删除了，那么就不需要写到新的里面去了
            if let Some(value) = self.get(key.clone()).unwrap() {
                let len = write_handler.metadata().unwrap().len();
                let kv = KV::new(key.clone(), value, 1);
                index_new.insert(
                    key.clone(),
                    FileOffset {
                        file: file_idx,
                        offset: len,
                    },
                );
                write_kv(&mut write_handler, kv);
                if len > ONE_SST_FILE_MAX_SIZE {
                    file_idx += 1;
                    tmp_filename = format!("tmpsst_{}", file_idx);
                    tmp_file = self.current_dir.clone().join(tmp_filename);
                    write_handler = fs::OpenOptions::new()
                        .write(true)
                        .append(true)
                        .create(true)
                        .open(tmp_file)
                        .unwrap();
                }
            }
        }

        // 先把原来的 sst 都删除了
        let files = get_sst_from_dir_with_prefix(self.current_dir.clone(), "sst_".to_owned());

        for file in files {
            let file = self.current_dir.clone().join(file);
            fs::remove_file(file).unwrap();
        }
        // 现在已经全部写到了新的里面去了，删除一下之前的文件并且 rename 一下新的临时文件即可
        let files = get_sst_from_dir_with_prefix(self.current_dir.clone(), "tmp".to_owned());

        for (idx, file) in files.iter().enumerate() {
            let tmp_file = self.current_dir.clone().join(file);
            let sst_file = self.current_dir.clone().join(format!("sst_{}", idx + 1));
            fs::rename(tmp_file, sst_file).unwrap();
        }

        // 更新 index， 换 write_handler
        self.index = index_new;
        let write_file = format!("sst_{}", files.len());
        let write_file_path = self.current_dir.clone().join(write_file.clone());
        self.write_handler = fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(write_file_path)
            .unwrap();
        self.current_write_file = files.len() as u64;
    }

    /// Set the value of a string key to a string. Return an error if the value is not written successfully.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(fo) = self.index.get(&key) {
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

    /// Remove a given key. Return an error if the key does not exist or is not removed successfully.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let result = self.get(key.clone()).unwrap();
        match result {
            Some(_) => {}
            None => {
                return Err(KvsError::ErrKeyNotFound);
            }
        }
        let len = self.write_handler.metadata().unwrap().len();
        // self.index.insert(key.clone(), len);
        self.index.insert(
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

        let mut sst_files = get_sst_from_dir_with_prefix(path.clone(), "sst".to_owned());
        if sst_files.is_empty() {
            sst_files.push("sst_1".to_owned());
        }

        let write_file = sst_files.last().cloned().unwrap();
        let write_file_path = path.join(write_file.clone());

        let pos = write_file.find("_").unwrap();
        let file_idx = write_file[(pos+1)..].parse::<u64>().unwrap();

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
            index: HashMap::new(),
            write_handler: write_handler,
            current_write_file: file_idx,
            current_dir: path,
        };
        store.init();
        Ok(store)
    }

    /// init kvstore, read all index into memory
    pub fn init(&mut self) {
        self.index = self.read_all_index();
    }

    fn read_all_index(&mut self) -> HashMap<String, FileOffset> {
        let mut index: HashMap<String, FileOffset> = HashMap::new();
        let sst_files = get_sst_from_dir_with_prefix(self.current_dir.clone(), "sst".to_owned());
        for file in sst_files {
            let pos = file.find("_").unwrap();
            let file_idx = file[(pos+1)..].parse::<u64>().unwrap();

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
