extern crate clap;
extern crate failure_derive;
extern crate num_cpus;

use clap::{App, Arg};
use kvs::{KvServer, KvStore, SledStore, thread_pool::*};
#[allow(unused)]
use log::{debug, error, info, warn, LevelFilter};
use core::time;
use std::env::current_dir;
use std::env;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::sync::{mpsc::{Receiver, Sender}, mpsc};

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
    let (server_stop_tx, server_stop_rx): (Sender<i32>, Receiver<i32>) = mpsc::channel();

    match engine {
        "kvs" => {
            let store = KvStore::open(current_dir().unwrap()).unwrap();
            let server = KvServer::new(store, pool, addr, server_stop_rx);
            std::thread::spawn(move || {
                server.start();
            }); 
        },
        "sled" => {
            let store = SledStore::open(current_dir().unwrap()).unwrap();
            let server = KvServer::new(store, pool, addr, server_stop_rx);
            std::thread::spawn(move || {
                server.start();
            }); 
        },
        _ => {
            panic!("{} engine is not satisfied.", engine)
        }
    }

    std::thread::sleep(time::Duration::from_secs(1));
    server_stop_tx.send(1).unwrap();
    TcpStream::connect(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4000)).unwrap();
    std::thread::sleep(time::Duration::from_secs(3));
}
