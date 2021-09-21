use std::net::TcpStream;

use log::error;

use crate::io::read_n;
use crate::{Request, Response};
use std::io::{Read, Write};
use std::process::exit;

/// kvsclient
/// it can send network request to the kv server
///
/// ```
/// use std::net::TcpStream;
/// use std::sync::mpsc::{self, Receiver, Sender};
/// use kvs::{KvServer, KvStore, KvsClient, KvsError, thread_pool::*, SledStore};
/// use tempfile::TempDir;
///
/// const SERVER_SOCKET_ADDR: &str = "127.0.0.1:4000";
///
/// let temp_dir =
/// TempDir::new().expect("unable to create temporary working directory");
/// let store = KvStore::open(temp_dir.path()).unwrap();
/// let pool = SharedQueueThreadPool::new(5).unwrap();
/// // server stop signal
/// let (server_stop_tx, server_stop_rx): (Sender<i32>, Receiver<i32>) =
///     mpsc::channel();
/// let server = KvServer::new(store, pool, SERVER_SOCKET_ADDR, server_stop_rx);
/// let handle = std::thread::spawn(move || {
///     server.start();
/// });
///
/// // client usage
/// KvsClient::set("key".to_owned(), "value".to_owned(), "127.0.0.1:4000");
/// let response = KvsClient::get("key".to_owned(), "127.0.0.1:4000");
/// match response.status {
///     KvsError::ErrKeyNotFound => {
///         panic!("key no found");
///     }
///     KvsError::ErrOk => {
///         assert_eq!("value", response.value);
///     }
/// }
///
/// server_stop_tx.send(0).unwrap();
/// TcpStream::connect(SERVER_SOCKET_ADDR).unwrap();
/// handle.join().unwrap();
/// ```
pub struct KvsClient {}

impl KvsClient {
    /// set
    pub fn set(key: String, value: String, addr: &str) -> Response {
        let mut stream = TcpStream::connect(&addr).unwrap_or_else(|err| {
            error!("Error happened when connect {}, error: {}", addr, &err);
            exit(1);
        });

        let request = Request::SET {
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

        let request = Request::GET { key: key };

        let response = hand_rpc(request, &mut stream);
        return response;
    }

    /// rm
    pub fn remove(key: String, addr: &str) -> Response {
        let mut stream = TcpStream::connect(&addr).unwrap_or_else(|err| {
            error!("Error happened when connect {}, error: {}", addr, &err);
            exit(1);
        });

        let request = Request::RM { key: key };

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
