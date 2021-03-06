use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use kvs::KvStore;
use kvs::KvsEngine;
use kvs::SledStore;
use rand::prelude::StdRng;
use rand::{random, Rng, SeedableRng};
use tempfile::TempDir;

fn store_write<E: KvsEngine>(r: &mut StdRng, store: &E) {
    for i in 1..=100 {
        let key_len = r.gen_range(1..=100000);
        let key_content = std::iter::repeat('a').take(key_len).collect::<String>();
        let key = format!("key{}{}", i, key_content);

        let val_len = r.gen_range(1..=100000);
        let val = std::iter::repeat('a').take(val_len).collect::<String>();

        store.set(key, val).unwrap();
    }
}

fn store_read<E: KvsEngine>(r: &mut StdRng, store: &E) {
    for i in 1..=100 {
        let key_len = r.gen_range(1..=100000);
        let key_content = std::iter::repeat('a').take(key_len).collect::<String>();
        let key = format!("key{}{}", i, key_content);

        let val_len = r.gen_range(1..=100000);
        let expected_val = std::iter::repeat('a').take(val_len).collect::<String>();

        let get_val = store.get(key).unwrap().unwrap();

        assert_eq!(expected_val, get_val);
    }
}

fn kvs_write_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("write bench");
    group.sample_size(10);
    let seed: u64 = random();
    group.bench_with_input("kvs_write", &seed, |c, seed| {
        let mut r = StdRng::seed_from_u64(*seed);
        c.iter(|| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let store = KvStore::open(temp_dir.path()).unwrap();

            store_write(&mut r, &store);
        });
    });

    group.bench_with_input("sled_write", &seed, |c, seed| {
        let mut r = StdRng::seed_from_u64(*seed);
        c.iter(|| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let store = SledStore::open(temp_dir.path()).unwrap();

            store_write(&mut r, &store);
        });
    });
    group.finish();
}

fn kvs_read_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("read bench");
    group.sample_size(10);
    let seed: u64 = random();
    group.bench_with_input("kvs_read", &seed, |c, seed| {
        let mut r = StdRng::seed_from_u64(*seed);
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        // ???????????????????????????????????????????????????????????????????????????????????????
        let store = KvStore::open(temp_dir.path()).unwrap();
        store_write(&mut r, &store);

        c.iter(|| {
            // ??????????????????????????????????????????????????????????????????????????? key
            let mut r = StdRng::seed_from_u64(*seed);
            store_read(&mut r, &store);
        });
    });

    group.bench_with_input("sled_read", &seed, |c, seed| {
        let mut r = StdRng::seed_from_u64(*seed);
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        // ???????????????????????????????????????????????????????????????????????????????????????
        let store = SledStore::open(temp_dir.path()).unwrap();
        store_write(&mut r, &store);

        c.iter(|| {
            // ??????????????????????????????????????????????????????????????????????????? key
            let mut r = StdRng::seed_from_u64(*seed);
            store_read(&mut r, &store);
        });
    });
    group.finish();
}

criterion_group!(benches, kvs_write_bench, kvs_read_bench);
criterion_main!(benches);
