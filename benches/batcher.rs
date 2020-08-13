use async_trait::async_trait;
use ultra_batch::{Batcher, Fetcher, Cache};
use criterion::{criterion_group, criterion_main, Criterion};

struct FetchIdent;

#[async_trait]
impl Fetcher for FetchIdent {
    type Key = u64;
    type Value = u64;
    type Error = anyhow::Error;

    async fn fetch(&self, keys: &[u64], values: &Cache<u64, u64>) -> anyhow::Result<()> {
        for key in keys {
            values.insert(*key, *key);
        }

        Ok(())
    }
}

fn bench_batch_simple_load_miss(c: &mut Criterion) {
    c.bench_function_over_inputs("load misses", |b, &size| {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        runtime.enter(|| {
            let batcher = Batcher::new(FetchIdent).build();
            let handle = runtime.handle();
            b.iter(|| {
                let mut tasks = vec![];
                for n in 0..size {
                    let batcher = batcher.clone();
                    let task = handle.spawn(async move {
                        batcher.load(n).await.unwrap()
                    });
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
    }, vec![250, 1000]);

    c.bench_function_over_inputs("load hits", |b, &size| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.enter(|| {
            let batcher = Batcher::new(FetchIdent).build();
            let handle = runtime.handle();

            handle.block_on({
                let batcher = batcher.clone();
                async move {
                    // Pre-load all keys
                    batcher.load_many(&(0..size).collect::<Vec<_>>()).await.unwrap();
                }
            });

            b.iter(|| {
                let mut tasks = vec![];
                for n in 0..size {
                    let batcher = batcher.clone();
                    let task = handle.spawn(async move {
                        batcher.load(n).await.unwrap()
                    });
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
    }, vec![250, 1000]);

    c.bench_function_over_inputs("load hits+misses", |b, &size| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.enter(|| {
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
                    let task = handle.spawn(async move {
                        batcher.load(n).await.unwrap()
                    });
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
    }, vec![250, 1000]);

    c.bench_function_over_inputs("load_many misses", |b, &size| {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        runtime.enter(|| {
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
                        async move {
                            batcher.load_many(&batch).await.unwrap()
                        }
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
    }, vec![250, 1000]);

    c.bench_function_over_inputs("load_many hits", |b, &size| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.enter(|| {
            let batcher = Batcher::new(FetchIdent).build();
            let handle = runtime.handle();

            handle.block_on({
                let batcher = batcher.clone();
                async move {
                    // Pre-load all keys
                    batcher.load_many(&(0..size).collect::<Vec<_>>()).await.unwrap();
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
                        async move {
                            batcher.load_many(&batch).await.unwrap()
                        }
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
    }, vec![250, 1000]);

    c.bench_function_over_inputs("load_many hits+misses", |b, &size| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.enter(|| {
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
                        async move {
                            batcher.load_many(&batch).await.unwrap()
                        }
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
    }, vec![250, 1000]);
}

criterion_group!(benches, bench_batch_simple_load_miss);
criterion_main!(benches);
