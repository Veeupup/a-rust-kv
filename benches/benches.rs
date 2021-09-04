use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use kvs::KvStore;
use kvs::KvsEngine;
use kvs::SledStore;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

fn kvs_write_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("write bench");
    group.sample_size(10);
    for seed in [1, 3, 5].iter() {
        group.bench_with_input(format!("kvs_seed_{}", seed), seed, |c, seed| {
            let mut r = StdRng::seed_from_u64(*seed);
            c.iter(|| {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let mut store: Box<dyn KvsEngine> =
                    Box::new(KvStore::open(temp_dir.path()).unwrap());

                store_write(&mut r, &mut store);
            });
        });

        group.bench_with_input(format!("sled_seed_{}", seed), seed, |c, seed| {
            let mut r = StdRng::seed_from_u64(*seed);
            c.iter(|| {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let mut store: Box<dyn KvsEngine> =
                    Box::new(SledStore::open(temp_dir.path()).unwrap());

                store_write(&mut r, &mut store);
            });
        });
    }
    group.finish();
}

fn store_write(r: &mut StdRng, store: &mut Box<dyn KvsEngine>) {
    for i in 1..=100 {
        let key_len = r.gen_range(1..=100000);
        let key_content = std::iter::repeat('a').take(key_len).collect::<String>();
        let key = format!("key{}{}", i, key_content);

        let val_len = r.gen_range(1..=100000);
        let val = std::iter::repeat('a').take(val_len).collect::<String>();

        store.set(key, val).unwrap();
    }
}

fn kvs_read_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("read bench");
    group.sample_size(10);
    for seed in [1, 3, 5].iter() {
        group.bench_with_input(format!("kvs_seed_{}", seed), seed, |c, seed| {
            let mut r = StdRng::seed_from_u64(*seed);
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            // 这里可以放到外面是因为只需要生成一次数据，后面的都可以只读
            let mut store: Box<dyn KvsEngine> = Box::new(KvStore::open(temp_dir.path()).unwrap());
            store_write(&mut r, &mut store);

            c.iter(|| {
                // 注意这里需要重新生成随机数，因为需要生成一样的读取 key
                let mut r = StdRng::seed_from_u64(*seed);
                store_read(&mut r, &mut store);
            });
        });

        group.bench_with_input(format!("sled_seed_{}", seed), seed, |c, seed| {
            let mut r = StdRng::seed_from_u64(*seed);
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            // 这里可以放到外面是因为只需要生成一次数据，后面的都可以只读
            let mut store: Box<dyn KvsEngine> = Box::new(SledStore::open(temp_dir.path()).unwrap());
            store_write(&mut r, &mut store);

            c.iter(|| {
                // 注意这里需要重新生成随机数，因为需要生成一样的读取 key
                let mut r = StdRng::seed_from_u64(*seed);
                store_read(&mut r, &mut store);
            });
        });
    }
    group.finish();
}

fn store_read(r: &mut StdRng, store: &mut Box<dyn KvsEngine>) {
    for i in 1..=100 {
        let key_len = r.gen_range(1..=100000);
        let key_content = std::iter::repeat('a').take(key_len).collect::<String>();
        let key = format!("key{}{}", i, key_content);

        store.get(key).unwrap();
    }
}

criterion_group!(benches, kvs_write_bench, kvs_read_bench);
criterion_main!(benches);
