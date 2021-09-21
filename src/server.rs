use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::exit;
use std::sync::mpsc::Receiver;

use log::{error, info};

use crate::io::read_n;
use crate::{thread_pool::ThreadPool, KvsEngine};
use crate::{KvsError, Request, Response};

/// kvserver
/// it can specify store engine and thread pool
/// it will serving network requests
pub struct KvServer<E, P> {
    engine: E,
    pool: P,
    listener: TcpListener,
    stop_rx: Receiver<i32>,
}

impl<E: KvsEngine, P: ThreadPool> KvServer<E, P> {
    /// new server
    pub fn new(engine: E, pool: P, addr: &str, stop_rx: Receiver<i32>) -> KvServer<E, P> {
        let listener = TcpListener::bind(addr).unwrap_or_else(|err| {
            error!("Error happened when tcp listen: {}", err);
            exit(1);
        });
        info!("Now Server is listening on: {}", addr);
        KvServer {
            engine: engine,
            pool: pool,
            listener: listener,
            stop_rx: stop_rx,
        }
    }

    /// server start
    pub fn start(&self) {
        for stream in self.listener.incoming() {
            let stop_singal = self.stop_rx.try_recv();
            match stop_singal {
                Ok(_) => {
                    info!("Server stop");
                    break;
                }
                Err(_) => {}
            }

            let store = self.engine.clone();
            self.pool.spawn(move || {
                handle_connection(store, stream.unwrap());
            });
        }
    }
}

fn handle_connection<E: KvsEngine>(store: E, mut stream: TcpStream) {
    let mut buffer = [0; 4]; // request len
    stream.read(&mut buffer).unwrap();
    let request_len = u32::from_be_bytes(buffer);
    let data = read_n(&mut stream, request_len as u64);
    let request: Request = serde_json::from_slice(&data).unwrap();

    info!("Request : {:?}", request);

    let mut write_reponse = |response: &mut Response| {
        let response = serde_json::to_string(&response).unwrap();
        let response_len = response.len() as u32;
        stream.write(&response_len.to_be_bytes()).unwrap();
        stream.write(response.as_bytes()).unwrap();
    };
    match request {
        Request::GET { key } => {
            let result = store.get(key).unwrap();
            if let Some(value) = result {
                let mut response = Response {
                    status: KvsError::ErrOk,
                    value: value,
                };
                write_reponse(&mut response);
            } else {
                let mut response = Response {
                    status: KvsError::ErrKeyNotFound,
                    value: "".to_owned(),
                };
                write_reponse(&mut response);
            }
        }
        Request::SET { key, value } => {
            store.set(key, value).unwrap();
            let mut response = Response {
                status: KvsError::ErrOk,
                value: "".to_owned(),
            };
            write_reponse(&mut response);
        }
        Request::RM { key } => {
            let result = store.remove(key);
            match result {
                Ok(()) => {
                    let mut response = Response {
                        status: KvsError::ErrOk,
                        value: "".to_owned(),
                    };
                    write_reponse(&mut response);
                }
                Err(_) => {
                    let mut response = Response {
                        status: KvsError::ErrKeyNotFound,
                        value: "".to_owned(),
                    };
                    write_reponse(&mut response);
                }
            }
        }
    }

    stream.flush().unwrap();
}
