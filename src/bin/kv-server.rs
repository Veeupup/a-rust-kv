extern crate clap;
extern crate failure_derive;
extern crate num_cpus;

use clap::{App, Arg};
use kvs::{thread_pool::*, KvServer, KvStore, SledStore};
#[allow(unused)]
use log::{debug, error, info, warn, LevelFilter};
use std::env;
use std::env::current_dir;
use std::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
};

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

    let pool = SharedQueueThreadPool::new(num_cpus::get() as u32).unwrap();
    let (_, server_stop_rx): (Sender<i32>, Receiver<i32>) = mpsc::channel();

    match engine {
        "kvs" => {
            let store = KvStore::open(current_dir().unwrap()).unwrap();
            let server = KvServer::new(store, pool, addr, server_stop_rx);
            server.start();
        }
        "sled" => {
            let store = SledStore::open(current_dir().unwrap()).unwrap();
            let server = KvServer::new(store, pool, addr, server_stop_rx);
            server.start();
        }
        _ => {
            panic!("{} engine is not satisfied.", engine)
        }
    }
}
