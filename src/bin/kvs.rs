extern crate clap;
use std::env::current_dir;
use std::{env, process::exit};
extern crate failure_derive;

use clap::{App, Arg};
use kvs::KvStore;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(App::new("get").arg(Arg::new("KEY").required(true)))
        .subcommand(
            App::new("set")
                .arg(Arg::new("KEY").required(true))
                .arg(Arg::new("VALUE").required(true)),
        )
        .subcommand(App::new("rm").arg(Arg::new("KEY").required(true)))
        .arg(Arg::new("version").short('V'))
        .get_matches();

    // let mut store = KvStore::open(current_dir().unwrap()).unwrap();
    // store.set("key1".to_owned(), "val1".to_owned());
    // store.set("key2".to_owned(), "val2".to_owned());
    // store.set("key3".to_owned(), "val3".to_owned());
    // store.get("key1".to_owned());

    match matches.subcommand() {
        Some(("get", sub_m)) => {
            let key = String::from(sub_m.value_of("KEY").unwrap());

            let kvs = KvStore::open(current_dir().unwrap()).unwrap();

            if let Some(result) = kvs.get(key).unwrap() {
                println!("{}", result);
                exit(0);
            }
            println!("Key not found");
            exit(0);
        } // get was used
        Some(("set", sub_m)) => {
            let key = String::from(sub_m.value_of("KEY").unwrap());
            let value = String::from(sub_m.value_of("VALUE").unwrap());

            let mut kvs = KvStore::open(current_dir().unwrap()).unwrap();
            kvs.set(key, value).unwrap();

            exit(0);
        } // set was used
        Some(("rm", sub_m)) => {
            let key = String::from(sub_m.value_of("KEY").unwrap());

            let mut kvs = KvStore::open(current_dir().unwrap()).unwrap();
            let _ = kvs.remove(key).unwrap_or_else(|err| {
                println!("{}", err);
                exit(1);
            });

            exit(0);
        } // rm was used
        _ => {
            panic!("unknown err");
        }
    }
}
