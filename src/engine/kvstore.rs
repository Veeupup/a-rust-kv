use crate::engine::KvsEngine;
use crate::error::{KvsError, Result};
use crate::io::{get_sst_from_dir_with_prefix, own_dir_or_not, read_n, write_kv};
use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::u64;

use super::util::KV;

const ONE_SST_FILE_MAX_SIZE: u64 = 1024;
const UNCOMPACTED_KEY_COUNTS: u64 = 100;

/// for log position
#[derive(Clone)]
struct FileOffset {
    file: u64,
    offset: u64,
}

/// The `KvStore` stores string key/value pairs.
/// It is a reader lock-free kv store, and it will compact automatically
///
/// here is the usage
/// ```
/// use kvs::{KvStore, KvsEngine, Result};
/// use tempfile::TempDir;
///
/// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
/// // open a kv store from a path
/// let store = KvStore::open(temp_dir.path()).unwrap();
/// // set a kv
/// store.set("key1".to_owned(), "value1".to_owned()).unwrap();
/// // get a kv
/// assert_eq!(store.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
/// // rm a kv
/// assert!(store.remove("key1".to_owned()).is_ok());
/// // check it!
/// assert_eq!(store.get("key1".to_owned()).unwrap(), None);
/// ```
#[derive(Clone)]
pub struct KvStore {
    current_dir: PathBuf,
    index: Arc<DashMap<String, FileOffset>>,
    reader_count: Arc<AtomicU32>, // readers count
    write_handler: Arc<RwLock<File>>,
    writer_index: Arc<RwLock<u64>>,
    uncompacted: Arc<AtomicCell<u64>>, // repeated keys count, for compaction
}

impl KvsEngine for KvStore {
    /// set kv pair
    /// Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.
    fn set(&self, key: String, val: String) -> Result<()> {
        if self.uncompacted.load() > UNCOMPACTED_KEY_COUNTS {
            self.compaction();
        }

        let mut write_handler = self.write_handler.write().unwrap();
        let mut writer_index = self.writer_index.write().unwrap();

        // if duplicate key insert, add uncompacted
        if let Some(_) = self.index.get(&key) {
            self.uncompacted.fetch_add(1);
        }

        let len = write_handler.metadata().unwrap().len();
        let kv = KV::new(key.clone(), val, 1);
        self.index.insert(
            key,
            FileOffset {
                file: writer_index.clone(),
                offset: len,
            },
        );
        write_kv(&mut write_handler, kv);

        if len > ONE_SST_FILE_MAX_SIZE {
            *writer_index += 1;
            let filename = format!("log_{}", writer_index);
            let write_file_path = self.current_dir.clone().join(filename);
            *write_handler = get_write_file_handler(write_file_path);
        }

        Ok(())
    }
    /// get kv pair
    /// Set the value of a string key to a string. Return an error if the value is not written successfully.
    fn get(&self, key: String) -> Result<Option<String>> {
        // let index = self.index.read().unwrap();
        let current_dir = self.current_dir.clone();

        if let Some(fo) = self.index.get(&key) {
            self.reader_count.fetch_add(1, Ordering::SeqCst);
            let res =
                KvStore::get_value_by_file_index(current_dir.clone(), fo.file.clone(), fo.offset);
            self.reader_count.fetch_sub(1, Ordering::SeqCst);
            return res;
        }
        return Ok(None);
    }
    /// remove kv pair
    /// Remove a given key. Return an error if the key does not exist or is not removed successfully.
    fn remove(&self, key: String) -> Result<()> {
        // let mut index = self.index.write().unwrap();
        let mut write_handler = self.write_handler.write().unwrap();
        let writer_index = self.writer_index.read().unwrap();

        {
            // if hold the reference of the map, then insert will deadlock
            let idx = self.index.get(&key);
            match idx {
                Some(_) => {
                    self.uncompacted.fetch_add(1);
                }
                None => return Err(KvsError::ErrKeyNotFound),
            }
        }
        let len = write_handler.metadata().unwrap().len();
        self.index.insert(
            key.clone(),
            FileOffset {
                file: *writer_index,
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
        let current_dir = self.current_dir.clone();
        let mut write_handler = self.write_handler.write().unwrap();
        let mut writer_index = self.writer_index.write().unwrap();

        let len = write_handler.metadata().unwrap().len();
        if len < ONE_SST_FILE_MAX_SIZE {
            return;
        }

        let old_files = get_sst_from_dir_with_prefix(current_dir.clone(), "log_".to_owned());

        *writer_index += 1;
        let mut filename = format!("log_{}", writer_index);
        let mut file = current_dir.clone().join(filename);
        *write_handler = get_write_file_handler(file);

        // ???????????? index?????????????????????????????? sst
        let old_index = (*self.index).clone();

        for key_file_offset in &old_index {
            let key = key_file_offset.key();
            // ???????????????????????? key????????? key ????????????????????????????????????????????????????????????????????????
            if let Some(fo) = old_index.get(key) {
                let value = KvStore::get_value_by_file_index(
                    current_dir.clone(),
                    fo.file.clone(),
                    fo.offset,
                )
                .unwrap()
                .unwrap();

                let len = write_handler.metadata().unwrap().len();
                let kv = KV::new(key.clone(), value, 1);
                self.index.insert(
                    key.clone(),
                    FileOffset {
                        file: *writer_index,
                        offset: len,
                    },
                );
                write_kv(&mut write_handler, kv);
                if len > ONE_SST_FILE_MAX_SIZE {
                    *writer_index += 1;
                    filename = format!("log_{}", writer_index);
                    file = current_dir.clone().join(filename);
                    *write_handler = get_write_file_handler(file);
                }
            } else {
                self.index.remove(key);
            }
        }

        // ???????????????????????????????????????????????????????????????????????? sst????????????????????????
        // ??????????????????????????????????????????????????????????????????????????? sst ?????????????????????????????????????????????????????????????????????????????????
        // ????????? ????????? log ????????????????????? compaction ????????????????????????
        while self.reader_count.load(Ordering::SeqCst) > 0 {}

        // ???????????? sst
        for filename in &old_files {
            let file = current_dir.clone().join(filename.clone());
            fs::remove_file(file).unwrap();
        }
    }

    fn get_value_by_file_index(
        current_dir: PathBuf,
        file_idx: u64,
        offset: u64,
    ) -> Result<Option<String>> {
        let filename = format!("log_{}", file_idx);
        let filename = current_dir.clone().join(filename);
        let mut reader = fs::OpenOptions::new().read(true).open(filename).unwrap();
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
        // ????????????????????? ???????????????????????????, ???????????????????????????????????????
        let path: PathBuf = path.into();

        own_dir_or_not(path.clone(), "kvs");

        let mut sst_files = get_sst_from_dir_with_prefix(path.clone(), "log".to_owned());
        if sst_files.is_empty() {
            sst_files.push("log_1".to_owned());
        }

        let write_file = sst_files.last().cloned().unwrap();
        let write_file_path = path.join(write_file.clone());

        let pos = write_file.find("_").unwrap();
        let file_idx = write_file[(pos + 1)..].parse::<u64>().unwrap();

        let write_handler = get_write_file_handler(write_file_path);
        let store = KvStore {
            index: Arc::new(DashMap::new()),
            write_handler: Arc::new(RwLock::new(write_handler)),
            reader_count: Arc::new(AtomicU32::new(0)),
            writer_index: Arc::new(RwLock::new(file_idx)),
            current_dir: path,
            uncompacted: Arc::new(AtomicCell::new(0)),
        };
        store.init();
        Ok(store)
    }

    /// init kvstore, read all index into memory
    pub fn init(&self) {
        self.read_all_index();
    }

    fn read_all_index(&self) {
        let current_dir = self.current_dir.clone();

        // let mut index: HashMap<String, FileOffset> = HashMap::new();
        let sst_files = get_sst_from_dir_with_prefix(current_dir.clone(), "log".to_owned());
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
                self.index.insert(
                    kv.key,
                    FileOffset {
                        file: file_idx,
                        offset: offset,
                    },
                );
                offset += 4 + key_len as u64;
            }
        }
    }
}

fn get_write_file_handler(path: PathBuf) -> File {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|err| {
            panic!("can not open the path : {}", err);
        })
}
