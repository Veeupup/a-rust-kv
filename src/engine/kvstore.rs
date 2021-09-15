use crate::engine::KvsEngine;
use crate::error::{KvsError, Result};
use crate::io::{get_sst_from_dir_with_prefix, own_dir_or_not, read_n, write_kv};
use crossbeam::atomic::AtomicCell;
use dashmap::{DashMap, DashSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::u64;

use super::util::KV;

#[derive(Clone)]
struct FileOffset {
    file: u64,
    offset: u64,
}

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory
///
#[derive(Clone)]
pub struct KvStore {
    current_dir: PathBuf,
    index: Arc<DashMap<String, FileOffset>>,
    reader_count: Arc<DashMap<u64, u64>>, // 记录哪些文件正在被读，不能被 compaction 删除
    old_sst_files: Arc<DashSet<String>>, // 旧版本的 sst，不能再被使用，下次 compaction 的时候会被删除掉
    write_handler: Arc<RwLock<File>>,
    writer_index: Arc<RwLock<u64>>,
    uncompacted: Arc<AtomicCell<u64>>,
}

const ONE_SST_FILE_MAX_SIZE: u64 = 1024;
const UNCOMPACTED_KEY_COUNTS: u64 = 100;

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
            let filename = format!("sst_{}", writer_index);
            let write_file_path = self.current_dir.clone().join(filename);
            *write_handler = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .append(true)
                .open(write_file_path)
                .unwrap_or_else(|err| {
                    panic!("can not open the path : {}", err);
                });
        }

        Ok(())
    }
    /// get kv pair
    /// Set the value of a string key to a string. Return an error if the value is not written successfully.
    fn get(&self, key: String) -> Result<Option<String>> {
        // let index = self.index.read().unwrap();
        let current_dir = self.current_dir.clone();

        if let Some(fo) = self.index.get(&key) {
            if self.reader_count.contains_key(&fo.file) {
                *self.reader_count.get_mut(&fo.file).unwrap() += 1;
            } else {
                self.reader_count.insert(fo.file, 1);
            }
            let res =
                KvStore::get_value_by_file_index(current_dir.clone(), fo.file.clone(), fo.offset);
            if self.reader_count.contains_key(&fo.file) {
                *self.reader_count.get_mut(&fo.file).unwrap() -= 1;
            }
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
                Some(_) => {}
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
        // 删除旧的 sst，上次在 compaction 的时候仍有读者在读的旧的 sst
        for (_, file) in self.old_sst_files.iter().enumerate() {
            let file = self.current_dir.clone().join(file.clone());
            // 如果不包含在仍然要保存的 sst 中，那么就可以删除掉
            fs::remove_file(file).unwrap();
        }

        let current_dir = self.current_dir.clone();
        // let mut index = self.index.write().unwrap();
        let mut write_handler = self.write_handler.write().unwrap();
        let mut writer_index = self.writer_index.write().unwrap();

        let len = write_handler.metadata().unwrap().len();
        if len < ONE_SST_FILE_MAX_SIZE {
            return;
        }

        let old_files = get_sst_from_dir_with_prefix(current_dir.clone(), "sst_".to_owned());

        // let mut file_idx = *writer_index + 1;
        *writer_index += 1;
        let mut filename = format!("sst_{}", writer_index);
        let mut file = current_dir.clone().join(filename);
        *write_handler = fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(file)
            .unwrap();

        // 保存旧的 index，并且遍历来生成新的 sst
        let old_index = (*self.index).clone();

        for key_file_offset in &old_index {
            let key = key_file_offset.key();
            // 只处理仍然存在的 key，如果 key 不存在或者被删除了，那么就不需要写到新的里面去了
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
                    filename = format!("sst_{}", writer_index);
                    file = current_dir.clone().join(filename);
                    *write_handler = fs::OpenOptions::new()
                        .write(true)
                        .append(true)
                        .create(true)
                        .open(file)
                        .unwrap();
                }
            } else {
                self.index.remove(key);
            }
        }

        // 这里记录下当前有读者仍然在读的文件，不能删除这些 sst，因为读者还在读
        // 从现在开始，已经更新了索引，现在再来读者也是去新的 sst 里面读，因此只要等到老的读者读完了，就可以安全的删除了
        // 可以将 过期的 log 保存起来，下次 compaction 的时候再删除即可
        for (_, reader_count) in self.reader_count.iter().enumerate() {
            // old sst, can not delete, it will delete in next compaction
            if *reader_count.value() > 0 {
                self.old_sst_files
                    .insert(format!("sst_{}", *reader_count.key()));
            } else {
                self.reader_count.remove(reader_count.key());
            }
        }

        // 删除旧的 sst
        for filename in old_files {
            let file = current_dir.clone().join(filename.clone());
            // 如果不包含在仍然要保存的 sst 中，那么就可以删除掉
            if !self.old_sst_files.contains(&filename) {
                fs::remove_file(file).unwrap();
            }
        }
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
            index: Arc::new(DashMap::new()),
            write_handler: Arc::new(RwLock::new(write_handler)),
            reader_count: Arc::new(DashMap::new()),
            old_sst_files: Arc::new(DashSet::new()),
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
