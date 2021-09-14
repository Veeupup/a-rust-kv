use engine::KV;
use std::fs::{self, read_dir, File};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::exit;

use crate::engine;

/// read n bytes
pub fn read_n<R>(reader: R, bytes_to_read: u64) -> Vec<u8>
where
    R: Read,
{
    let mut buf = vec![];
    let mut chunk = reader.take(bytes_to_read);
    let n = chunk.read_to_end(&mut buf).expect("Didn't read enough");
    assert_eq!(bytes_to_read as usize, n);
    buf
}

/// write a kv to the file
pub fn write_kv(file: &mut File, kv: KV) {
    let serialized = serde_json::to_string(&kv).unwrap();
    let key_len = serialized.len() as u32;
    file.write(&key_len.to_be_bytes()).unwrap();
    file.write(serialized.as_bytes()).unwrap();
}

/// get files from dir by prefix
pub fn get_sst_from_dir_with_prefix(dir: impl Into<PathBuf>, prefix: String) -> Vec<String> {
    let paths = read_dir(dir.into()).unwrap();
    let mut files: Vec<String> = paths
        .map(|path| path.unwrap().file_name().into_string().unwrap())
        .filter(|path| path.starts_with(&prefix))
        .collect();
    let get_version = |filename: &String| -> u32 {
        let pos1 = filename.find("_").unwrap();
        let v1 = filename[(pos1 + 1)..].parse::<u32>().unwrap();
        v1
    };
    files.sort_by(|a, b| {
        let v1 = get_version(a);
        let v2 = get_version(b);
        v1.cmp(&v2)
    });
    files
}

pub fn own_dir_or_not(dir: PathBuf, db_type: &str) {
    let paths = read_dir(dir.clone()).unwrap();

    // 如果没有任何前缀文件，那么认为都没创建过，可以继续做
    for file in paths {
        let filename = file.unwrap().file_name().into_string().unwrap();
        if db_type == "kvs" && filename.starts_with("sled") {
            exit(1);
        }
        if db_type == "sled" && filename.starts_with("kvs") {
            exit(1);
        }
    }

    let filepath = dir.join(db_type);

    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filepath)
        .unwrap_or_else(|err| {
            panic!("can not open the path : {}", err);
        });
}
