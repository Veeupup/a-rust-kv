use std::net::TcpStream;

use log::{error, info};

use crate::io::read_n;
use crate::{OpType, Request, Response};
use std::io::{Read, Write};
use std::process::exit;

/// kvsclient
/// it can send network request to the kv server
pub struct KvsClient {}

impl KvsClient {
    /// set
    pub fn set(key: String, value: String, addr: &str) -> Response {
        let mut stream = TcpStream::connect(&addr).unwrap_or_else(|err| {
            error!("Error happened when connect {}, error: {}", addr, &err);
            exit(1);
        });

        let request = Request {
            op: OpType::SET,
            key: key,
            value: value,
        };

        let response = hand_rpc(request, &mut stream);
        return response;
    }

    /// get
    pub fn get(key: String, addr: &str) -> Response {
        let mut stream = TcpStream::connect(&addr).unwrap_or_else(|err| {
            error!("Error happened when connect {}, error: {}", addr, &err);
            exit(1);
        });

        let request = Request {
            op: OpType::GET,
            key: key,
            value: "".to_owned(),
        };

        let response = hand_rpc(request, &mut stream);
        return response;
    }

    /// rm
    pub fn remove(key: String, addr: &str) -> Response {
        let mut stream = TcpStream::connect(&addr).unwrap_or_else(|err| {
            error!("Error happened when connect {}, error: {}", addr, &err);
            exit(1);
        });

        let request = Request {
            op: OpType::RM,
            key: key,
            value: "".to_owned(),
        };

        let response = hand_rpc(request, &mut stream);
        return response;
    }
}

fn hand_rpc(request: Request, stream: &mut TcpStream) -> Response {
    let request = serde_json::to_string(&request).unwrap();
    let request_len = request.len() as u32;
    stream.write(&request_len.to_be_bytes()).unwrap();
    stream.write(request.as_bytes()).unwrap();
    stream.flush().unwrap();

    let mut buffer = [0; 4]; // request len
    stream.read(&mut buffer).unwrap();
    let request_len = u32::from_be_bytes(buffer);
    let data = read_n(stream, request_len as u64);
    let response: Response = serde_json::from_slice(&data).unwrap();
    return response;
}
