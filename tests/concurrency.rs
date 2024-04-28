use ultra_batch::{Batcher, Cache, Fetcher};

fn fib(n: usize) -> usize {
    match n {
        0 => 0,
        1 => 1,
        n => fib(n - 1).wrapping_add(fib(n - 2)),
    }
}

#[derive(Debug, Clone)]
struct BigValue {
    items: Vec<bool>,
}

// A fetcher that spawns multiple tasks and returns a `Vec` for each key (and
// also delays for a bit before fetching). This should be a good candidate to
// test lots of tasks running in parallel.
struct SlowFetcher;

impl Fetcher for SlowFetcher {
    type Key = usize;
    type Value = BigValue;
    type Error = anyhow::Error;

    async fn fetch(
        &self,
        keys: &[usize],
        values: &mut Cache<'_, usize, BigValue>,
    ) -> anyhow::Result<()> {
        tokio::time::sleep(tokio::time::Duration::from_millis(15)).await;

        let value_tasks = keys
            .iter()
            .copied()
            .map(|key| {
                tokio::task::spawn_blocking(move || {
                    let len = (fib(key % 25)) + 1;
                    let items = (0..len).map(|value| value % 2 == 0).collect::<Vec<_>>();
                    let value = BigValue { items };
                    (key, value)
                })
            })
            .collect::<Vec<_>>();

        for task in value_tasks {
            let (key, value) = task.await?;
            values.insert(key, value);
        }

        Ok(())
    }
}

async fn concurrency_task() -> anyhow::Result<()> {
    let batcher = Batcher::build(SlowFetcher).finish();
    let load_tasks = (0..2000)
        .map(|n| {
            let key = n / 3;
            let batcher = batcher.clone();
            tokio::spawn(async move {
                let result = batcher.load(key).await?;

                if !result.items.is_empty() {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("length was 0"))
                }
            })
        })
        .collect::<Vec<_>>();

    for load_task in load_tasks {
        let () = load_task.await??;
    }

    Ok(())
}

#[test]
fn test_concurrency_basic_scheduler() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()?;

    runtime.block_on(concurrency_task())
}

#[test]
fn test_concurrency_one_thread() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()?;

    runtime.block_on(concurrency_task())
}

#[test]
fn test_concurrency_eight_threads() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(8)
        .build()?;

    runtime.block_on(concurrency_task())
}
