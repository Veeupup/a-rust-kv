extern crate clap;
use std::io::Write;
use std::{env, process::exit};
extern crate failure_derive;

use clap::{App, Arg};
use kvs::{KvsError, OpType, Request, Response, read_n};
use log::{error, info};
use std::io::prelude::*;
use std::net::TcpStream;

fn main() {
    env_logger::init();
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            App::new("get")
                .arg(Arg::new("KEY").required(true).index(1))
                .arg(
                    Arg::new("addr")
                        .long("addr")
                        .takes_value(true)
                        .default_value("127.0.0.1:4000"),
                ),
        )
        .subcommand(
            App::new("set")
                .arg(Arg::new("KEY").index(1).required(true))
                .arg(Arg::new("VALUE").index(2).required(true))
                .arg(
                    Arg::new("addr")
                        .long("addr")
                        .takes_value(true)
                        .default_value("127.0.0.1:4000"),
                ),
        )
        .subcommand(
            App::new("rm").arg(Arg::new("KEY").required(true)).arg(
                Arg::new("addr")
                    .long("addr")
                    .takes_value(true)
                    .default_value("127.0.0.1:4000"),
            ),
        )
        .arg(Arg::new("version").short('V'))
        .get_matches();

    match matches.subcommand() {
        Some(("get", sub_m)) => {
            let key = String::from(sub_m.value_of("KEY").unwrap());
            let addr = sub_m.value_of("addr").unwrap();
            info!("get key: {}, addr: {}", key, addr);

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
            info!("{:?}", response);
            match response.status {
                KvsError::ErrKeyNotFound => {
                    println!("{}", response.status);
                },
                KvsError::ErrOk => {
                    println!("{}", response.value);
                },
                _ => {}
            }
            

        } // get was used
        Some(("set", sub_m)) => {
            let key = String::from(sub_m.value_of("KEY").unwrap());
            let value = String::from(sub_m.value_of("VALUE").unwrap());
            let addr = String::from(sub_m.value_of("addr").unwrap());

            info!("set key: {}, value: {}, addr: {}", key, value, addr);

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
            info!("{:?}", response);

        } // set was used
        Some(("rm", sub_m)) => {
            let key = String::from(sub_m.value_of("KEY").unwrap());
            let addr = String::from(sub_m.value_of("addr").unwrap());

            info!("rm key: {}, addr: {}", key, addr);

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
            info!("{:?}", response);

            match response.status {
                KvsError::ErrKeyNotFound => {
                    eprintln!("Key not found");
                    exit(1);
                },
                KvsError::ErrOk => {
                    
                },
                _ => {}
            }

        } // rm was used
        _ => {
            panic!("unknown err");
        }
    }
}

fn hand_rpc(request: Request, stream: &mut TcpStream) -> Response {
    let request = serde_json::to_string(&request).unwrap();
    info!("request: {}", request);
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
