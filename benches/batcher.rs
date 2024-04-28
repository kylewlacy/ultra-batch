use async_trait::async_trait;
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

fn main() {
    divan::main();
}

#[divan::bench(args = [250, 1000])]
fn load_misses(bencher: divan::Bencher, size: u64) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _enter = runtime.enter();
    let batcher = Batcher::build(FetchIdent).finish();
    let handle = runtime.handle();

    bencher.counter(size).bench(|| {
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
}

#[divan::bench(args = [250, 1000])]
fn load_hits(bencher: divan::Bencher, size: u64) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _enter = runtime.enter();
    let batcher = Batcher::build(FetchIdent).finish();
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

    bencher.counter(size).bench(|| {
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
}

#[divan::bench(args = [250, 1000])]
fn load_hits_and_misses(bencher: divan::Bencher, size: u64) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _enter = runtime.enter();
    let batcher = Batcher::build(FetchIdent).finish();
    let handle = runtime.handle();

    handle.block_on({
        let batcher = batcher.clone();
        async move {
            // Pre-load some keys
            let preload_keys = (0..size).filter(|n| n % 2 == 0).collect::<Vec<_>>();
            batcher.load_many(&preload_keys).await.unwrap();
        }
    });

    bencher.counter(size).bench(|| {
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
}

#[divan::bench(args = [250, 1000])]
fn load_many_misses(bencher: divan::Bencher, size: u64) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _enter = runtime.enter();
    let batcher = Batcher::build(FetchIdent).finish();
    let handle = runtime.handle();

    bencher.counter(size).bench(|| {
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
}

#[divan::bench(args = [250, 1000])]
fn load_many_hits(bencher: divan::Bencher, size: u64) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _enter = runtime.enter();
    let batcher = Batcher::build(FetchIdent).finish();
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

    bencher.counter(size).bench(|| {
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
}

#[divan::bench(args = [250, 1000])]
fn load_many_hits_and_misses(bencher: divan::Bencher, size: u64) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _enter = runtime.enter();
    let batcher = Batcher::build(FetchIdent).finish();
    let handle = runtime.handle();

    handle.block_on({
        let batcher = batcher.clone();
        async move {
            // Pre-load some keys
            let preload_keys = (0..size).filter(|n| n % 2 == 0).collect::<Vec<_>>();
            batcher.load_many(&preload_keys).await.unwrap();
        }
    });

    bencher.counter(size).bench(|| {
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
}
