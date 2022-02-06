use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use ultra_batch::{Batcher, Cache, Fetcher};

struct FetchIdent;

#[async_trait]
impl Fetcher for FetchIdent {
    type Key = u64;
    type Value = u64;
    type Error = anyhow::Error;

    async fn fetch(&self, keys: &[u64], values: &mut Cache<'_, u64, u64>) -> anyhow::Result<()> {
        for key in keys {
            values.insert(*key, *key);
        }

        Ok(())
    }
}

fn bench_batch_simple_load_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("load misses");
    for size in [250, 1000] {
        group.bench_with_input("load misses", &size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();

            let _enter = runtime.enter();

            let batcher = Batcher::new(FetchIdent).build();
            let handle = runtime.handle();
            b.iter(|| {
                let mut tasks = vec![];
                for n in 0..size {
                    let batcher = batcher.clone();
                    let task = handle.spawn(async move { batcher.load(n).await.unwrap() });
                    tasks.push((n, task));
                }

                handle.block_on(async move {
                    for (n, task) in tasks {
                        let result = task.await.unwrap();
                        assert_eq!(result, n);
                    }
                });
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("load hits");
    for size in [250, 1000] {
        group.bench_with_input("load hits", &size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _enter = runtime.enter();
            let batcher = Batcher::new(FetchIdent).build();
            let handle = runtime.handle();

            handle.block_on({
                let batcher = batcher.clone();
                async move {
                    // Pre-load all keys
                    batcher
                        .load_many(&(0..size).collect::<Vec<_>>())
                        .await
                        .unwrap();
                }
            });

            b.iter(|| {
                let mut tasks = vec![];
                for n in 0..size {
                    let batcher = batcher.clone();
                    let task = handle.spawn(async move { batcher.load(n).await.unwrap() });
                    tasks.push((n, task));
                }

                handle.block_on(async move {
                    for (n, task) in tasks {
                        let result = task.await.unwrap();
                        assert_eq!(result, n);
                    }
                });
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("load hits+misses");
    for size in [250, 1000] {
        group.bench_with_input("load hits+misses", &size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _enter = runtime.enter();

            let batcher = Batcher::new(FetchIdent).build();
            let handle = runtime.handle();

            handle.block_on({
                let batcher = batcher.clone();
                async move {
                    // Pre-load some keys
                    let preload_keys = (0..size).filter(|n| n % 2 == 0).collect::<Vec<_>>();
                    batcher.load_many(&preload_keys).await.unwrap();
                }
            });

            b.iter(|| {
                let mut tasks = vec![];
                for n in 0..size {
                    let batcher = batcher.clone();
                    let task = handle.spawn(async move { batcher.load(n).await.unwrap() });
                    tasks.push((n, task));
                }

                handle.block_on(async move {
                    for (n, task) in tasks {
                        let result = task.await.unwrap();
                        assert_eq!(result, n);
                    }
                });
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("load_many misses");
    for size in [250, 1000] {
        group.bench_with_input("load_many misses", &size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();

            let _enter = runtime.enter();

            let batcher = Batcher::new(FetchIdent).build();
            let handle = runtime.handle();
            b.iter(|| {
                let mut tasks = vec![];
                let keys = (0..size).collect::<Vec<_>>();
                for batch in keys.chunks(25) {
                    let batch = batch.to_vec();
                    let batcher = batcher.clone();
                    let task = handle.spawn({
                        let batch = batch.clone();
                        async move { batcher.load_many(&batch).await.unwrap() }
                    });
                    tasks.push((batch, task));
                }

                handle.block_on(async move {
                    for (batch, task) in tasks {
                        let results = task.await.unwrap();
                        assert_eq!(results, batch);
                    }
                });
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("load_many hits");
    for size in [250, 1000] {
        group.bench_with_input("load_many hits", &size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _enter = runtime.enter();
            let batcher = Batcher::new(FetchIdent).build();
            let handle = runtime.handle();

            handle.block_on({
                let batcher = batcher.clone();
                async move {
                    // Pre-load all keys
                    batcher
                        .load_many(&(0..size).collect::<Vec<_>>())
                        .await
                        .unwrap();
                }
            });

            b.iter(|| {
                let mut tasks = vec![];
                let keys = (0..size).collect::<Vec<_>>();
                for batch in keys.chunks(25) {
                    let batch = batch.to_vec();
                    let batcher = batcher.clone();
                    let task = handle.spawn({
                        let batch = batch.clone();
                        async move { batcher.load_many(&batch).await.unwrap() }
                    });
                    tasks.push((batch, task));
                }

                handle.block_on(async move {
                    for (batch, task) in tasks {
                        let results = task.await.unwrap();
                        assert_eq!(results, batch);
                    }
                });
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("load_many hits+misses");
    for size in [250, 1000] {
        group.bench_with_input("load_many hits+misses", &size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _enter = runtime.enter();

            let batcher = Batcher::new(FetchIdent).build();
            let handle = runtime.handle();

            handle.block_on({
                let batcher = batcher.clone();
                async move {
                    // Pre-load some keys
                    let preload_keys = (0..size).filter(|n| n % 2 == 0).collect::<Vec<_>>();
                    batcher.load_many(&preload_keys).await.unwrap();
                }
            });

            b.iter(|| {
                let mut tasks = vec![];
                let keys = (0..size).collect::<Vec<_>>();
                for batch in keys.chunks(25) {
                    let batch = batch.to_vec();
                    let batcher = batcher.clone();
                    let task = handle.spawn({
                        let batch = batch.clone();
                        async move { batcher.load_many(&batch).await.unwrap() }
                    });
                    tasks.push((batch, task));
                }

                handle.block_on(async move {
                    for (batch, task) in tasks {
                        let results = task.await.unwrap();
                        assert_eq!(results, batch);
                    }
                });
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_batch_simple_load_miss);
criterion_main!(benches);
