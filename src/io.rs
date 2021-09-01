use crate::kv::KV;
use std::io::Write;
use std::{fs::File, io::Read};

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
