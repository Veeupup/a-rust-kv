use crate::kv::KV;
use std::fs::{read_dir, File};
use std::io::{Read, Write};
use std::path::{PathBuf};

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

pub fn write_kv(file: &mut File, kv: KV) {
    let serialized = serde_json::to_string(&kv).unwrap();
    let key_len = serialized.len() as u32;
    file.write(&key_len.to_be_bytes()).unwrap();
    file.write(serialized.as_bytes()).unwrap();
}

pub fn get_sst_from_dir(dir: impl Into<PathBuf>) -> Vec<String> {
    let paths = read_dir(dir.into()).unwrap();
    let mut files: Vec<String> = paths
        .map(|path| path.unwrap().file_name().into_string().unwrap())
        .filter(|path| path.starts_with("sst"))
        .collect();
    files.sort();
    files
}

// pub fn get_value_by_file_index(filename: &String, offset: u64) -> Result<Option<String>> {
//     let mut reader = OpenOptions::new()
//         .read(true)
//         .open(filename)
//         .unwrap_or_else(|err| {
//             panic!("can not open the path : {}", err);
//         });
//     reader.seek(SeekFrom::Start(offset)).unwrap();
//     let mut meta_buffer: [u8; 4] = [0; 4]; // 8 byte
//     reader.read(&mut meta_buffer).unwrap();
//     let key_len = u32::from_be_bytes(meta_buffer);
//     let data = read_n(&mut reader, key_len as u64);
//     let kv: KV = serde_json::from_slice(&data).unwrap();
//     if kv.version == 0 {
//         return Ok(None);
//     } else {
//         return Ok(Some(kv.value));
//     }
// }
