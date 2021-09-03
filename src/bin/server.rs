extern crate clap;
extern crate failure_derive;

use clap::{App, Arg};
use kvs::{read_n, KvStore, KvsError, OpType, Request, Response};
#[allow(unused)]
use log::{debug, error, info, warn, LevelFilter};
use std::env::current_dir;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::{env, process::exit};

fn main() {
    env_logger::Builder::new()
        .target(env_logger::Target::Stderr)
        .filter_level(LevelFilter::Info)
        .init();
    info!("{}", env!("CARGO_PKG_VERSION"));
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::new("addr")
                .long("addr")
                .takes_value(true)
                .default_value("127.0.0.1:4000"),
        )
        .arg(
            Arg::new("engine")
                .long("engine")
                .possible_values(&["kvs", "sled"])
                .takes_value(true)
                .default_value("kvs"),
        )
        .arg(Arg::new("version").short('V'))
        .get_matches();

    let addr = matches.value_of("addr").unwrap();
    let engine = matches.value_of("engine").unwrap();
    info!("Addr: {}, Engine: {}", addr, engine);

    let listener = TcpListener::bind(addr).unwrap_or_else(|err| {
        error!("Error happened when tcp listen: {}", err);
        exit(1);
    });
    info!("Now Server is listening on: {}", addr);

    let mut store = KvStore::open(current_dir().unwrap()).unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(&mut store, stream);
    }
}

fn handle_connection(store: &mut KvStore, mut stream: TcpStream) {
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
