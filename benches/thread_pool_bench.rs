use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, Sender};

use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use kvs::{thread_pool::*, SledStore};
use kvs::{KvServer, KvStore, KvsClient, KvsError};
use tempfile::TempDir;

const SERVER_SOCKET_ADDR: &str = "127.0.0.1:4000";
const SERVER_POOL_THREADS_NUMBER: &[u32] = &[4];
const CLIENT_POOL_THREADS_NUMBER: u32 = 32;

fn client_write_workload(pool: impl ThreadPool) {
    // make it sync
    let (tx, rx): (Sender<i32>, Receiver<i32>) = mpsc::channel();
    for i in 0..1000 {
        let sender = tx.clone();
        pool.spawn(move || {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            let response = KvsClient::set(key, value, SERVER_SOCKET_ADDR);
            assert!(matches!(response.status, KvsError::ErrOk));
            sender.send(0).unwrap();
        });
    }
    for _ in 0..1000 {
        rx.recv().unwrap();
    }
}

fn client_read_workload(pool: impl ThreadPool) {
    // make it sync
    let (tx, rx): (Sender<i32>, Receiver<i32>) = mpsc::channel();
    for i in 0..1000 {
        let sender = tx.clone();
        pool.spawn(move || {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            let response = KvsClient::get(key, SERVER_SOCKET_ADDR);
            assert!(matches!(response.status, KvsError::ErrOk));
            assert_eq!(response.value, value);
            sender.send(0).unwrap();
        });
    }
    for _ in 0..1000 {
        rx.recv().unwrap();
    }
}

fn write_queued_kvstore(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_queued_kvstore");
    group.sample_size(10);
    for threads in SERVER_POOL_THREADS_NUMBER {
        group.bench_with_input(
            format!("queued_kvstore_{}", threads),
            threads,
            |c, threads| {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store = KvStore::open(temp_dir.path()).unwrap();
                let pool = SharedQueueThreadPool::new(*threads).unwrap();
                // server stop signal
                let (server_stop_tx, server_stop_rx): (Sender<i32>, Receiver<i32>) =
                    mpsc::channel();
                let server = KvServer::new(store, pool, SERVER_SOCKET_ADDR, server_stop_rx);
                let handle = std::thread::spawn(move || {
                    server.start();
                });

                c.iter(|| {
                    let pool = SharedQueueThreadPool::new(CLIENT_POOL_THREADS_NUMBER).unwrap();
                    client_write_workload(pool);
                });

                server_stop_tx.send(0).unwrap();
                TcpStream::connect(SERVER_SOCKET_ADDR).unwrap();
                handle.join().unwrap();
            },
        );
    }
    group.finish();
}

fn read_queued_kvstore(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_queued_kvstore");
    group.sample_size(10);
    for threads in SERVER_POOL_THREADS_NUMBER {
        group.bench_with_input(
            format!("queued_kvstore_{}", threads),
            threads,
            |c, threads| {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store = KvStore::open(temp_dir.path()).unwrap();
                let pool = SharedQueueThreadPool::new(*threads).unwrap();
                // server stop signal
                let (server_stop_tx, server_stop_rx): (Sender<i32>, Receiver<i32>) =
                    mpsc::channel();
                let server = KvServer::new(store, pool, SERVER_SOCKET_ADDR, server_stop_rx);
                let handle = std::thread::spawn(move || {
                    server.start();
                });
                // set target
                let pool = SharedQueueThreadPool::new(4).unwrap();
                client_write_workload(pool);

                c.iter(|| {
                    let pool = SharedQueueThreadPool::new(CLIENT_POOL_THREADS_NUMBER).unwrap();
                    client_read_workload(pool);
                });

                server_stop_tx.send(0).unwrap();
                TcpStream::connect(SERVER_SOCKET_ADDR).unwrap();
                handle.join().unwrap();
            },
        );
    }
    group.finish();
}

fn write_rayon_kvstore(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_rayon_kvstore");
    group.sample_size(10);
    for threads in SERVER_POOL_THREADS_NUMBER {
        group.bench_with_input(
            format!("rayon_kvstore_{}", threads),
            threads,
            |c, threads| {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store = KvStore::open(temp_dir.path()).unwrap();
                let pool = RayonThreadPool::new(*threads).unwrap();
                // server stop signal
                let (server_stop_tx, server_stop_rx): (Sender<i32>, Receiver<i32>) =
                    mpsc::channel();
                let server = KvServer::new(store, pool, SERVER_SOCKET_ADDR, server_stop_rx);
                let handle = std::thread::spawn(move || {
                    server.start();
                });

                c.iter(|| {
                    let pool = RayonThreadPool::new(CLIENT_POOL_THREADS_NUMBER).unwrap();
                    client_write_workload(pool);
                });

                server_stop_tx.send(0).unwrap();
                TcpStream::connect(SERVER_SOCKET_ADDR).unwrap();
                handle.join().unwrap();
            },
        );
    }
    group.finish();
}

fn read_rayon_kvstore(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_rayon_kvstore");
    group.sample_size(10);
    for threads in SERVER_POOL_THREADS_NUMBER {
        group.bench_with_input(
            format!("rayon_kvstore_{}", threads),
            threads,
            |c, threads| {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store = KvStore::open(temp_dir.path()).unwrap();
                let pool = RayonThreadPool::new(*threads).unwrap();
                // server stop signal
                let (server_stop_tx, server_stop_rx): (Sender<i32>, Receiver<i32>) =
                    mpsc::channel();
                let server = KvServer::new(store, pool, SERVER_SOCKET_ADDR, server_stop_rx);
                let handle = std::thread::spawn(move || {
                    server.start();
                });
                // set target
                let pool = RayonThreadPool::new(4).unwrap();
                client_write_workload(pool);

                c.iter(|| {
                    let pool = RayonThreadPool::new(CLIENT_POOL_THREADS_NUMBER).unwrap();
                    client_read_workload(pool);
                });

                server_stop_tx.send(0).unwrap();
                TcpStream::connect(SERVER_SOCKET_ADDR).unwrap();
                handle.join().unwrap();
            },
        );
    }
    group.finish();
}

fn write_rayon_sledengine(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_rayon_sledengine");
    group.sample_size(10);
    for threads in SERVER_POOL_THREADS_NUMBER {
        group.bench_with_input(
            format!("rayon_sledengine_{}", threads),
            threads,
            |c, threads| {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store = SledStore::open(temp_dir.path()).unwrap();
                let pool = RayonThreadPool::new(*threads).unwrap();
                // server stop signal
                let (server_stop_tx, server_stop_rx): (Sender<i32>, Receiver<i32>) =
                    mpsc::channel();
                let server = KvServer::new(store, pool, SERVER_SOCKET_ADDR, server_stop_rx);
                let handle = std::thread::spawn(move || {
                    server.start();
                });

                c.iter(|| {
                    let pool = RayonThreadPool::new(CLIENT_POOL_THREADS_NUMBER).unwrap();
                    client_write_workload(pool);
                });

                server_stop_tx.send(0).unwrap();
                TcpStream::connect(SERVER_SOCKET_ADDR).unwrap();
                handle.join().unwrap();
            },
        );
    }
    group.finish();
}

fn read_rayon_sledengine(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_rayon_sledengine");
    group.sample_size(10);
    for threads in SERVER_POOL_THREADS_NUMBER {
        group.bench_with_input(
            format!("rayon_sledengine_{}", threads),
            threads,
            |c, threads| {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store = SledStore::open(temp_dir.path()).unwrap();
                let pool = RayonThreadPool::new(*threads).unwrap();
                // server stop signal
                let (server_stop_tx, server_stop_rx): (Sender<i32>, Receiver<i32>) =
                    mpsc::channel();
                let server = KvServer::new(store, pool, SERVER_SOCKET_ADDR, server_stop_rx);
                let handle = std::thread::spawn(move || {
                    server.start();
                });
                // set target
                let pool = RayonThreadPool::new(4).unwrap();
                client_write_workload(pool);

                c.iter(|| {
                    let pool = RayonThreadPool::new(CLIENT_POOL_THREADS_NUMBER).unwrap();
                    client_read_workload(pool);
                });

                server_stop_tx.send(0).unwrap();
                TcpStream::connect(SERVER_SOCKET_ADDR).unwrap();
                handle.join().unwrap();
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    write_queued_kvstore,
    read_queued_kvstore,
    write_rayon_kvstore,
    read_rayon_kvstore,
    write_rayon_sledengine,
    read_rayon_sledengine
);
criterion_main!(benches);
