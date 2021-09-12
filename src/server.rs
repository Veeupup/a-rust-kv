use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::exit;
use std::sync::mpsc::Receiver;

use log::{info, error};


use crate::io::read_n;
use crate::{KvsError, OpType, Request, Response};
use crate::{thread_pool::ThreadPool, KvsEngine};

/// kvserver
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
                    info!("server stop");
                    break;
                },
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
    match request.op {
        OpType::GET => {
            let result = store.get(request.key).unwrap();
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
        OpType::SET => {
            store.set(request.key, request.value).unwrap();
            let mut response = Response {
                status: KvsError::ErrOk,
                value: "".to_owned(),
            };
            write_reponse(&mut response);
        }
        OpType::RM => {
            let result = store.remove(request.key);
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
