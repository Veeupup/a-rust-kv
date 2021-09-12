extern crate clap;
use std::env;
extern crate failure_derive;

use clap::{App, Arg};
use kvs::{KvsClient, KvsError};
use log::info;
use std::process::exit;

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

            let response = KvsClient::get(key, addr);
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
        }
        Some(("set", sub_m)) => {
            let key = String::from(sub_m.value_of("KEY").unwrap());
            let value = String::from(sub_m.value_of("VALUE").unwrap());
            let addr = sub_m.value_of("addr").unwrap();

            KvsClient::set(key, value, addr);
        }
        Some(("rm", sub_m)) => {
            let key = String::from(sub_m.value_of("KEY").unwrap());
            let addr = sub_m.value_of("addr").unwrap();

            let response = KvsClient::remove(key, addr);

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
