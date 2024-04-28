use ultra_batch::{BatchExecutor, Executor};

struct ExecuteIdent;

impl Executor for ExecuteIdent {
    type Value = u64;
    type Result = u64;
    type Error = anyhow::Error;

    async fn execute(&self, values: Vec<u64>) -> anyhow::Result<Vec<u64>> {
        Ok(values)
    }
}

fn main() {
    divan::main();
}

#[divan::bench(args = [250, 1000])]
fn execute(bencher: divan::Bencher, size: u64) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _enter = runtime.enter();
    let batch_executor = BatchExecutor::build(ExecuteIdent).finish();
    let handle = runtime.handle();

    bencher.counter(size).bench(|| {
        let mut tasks = vec![];
        for n in 0..size {
            let batch_executor = batch_executor.clone();
            let task = handle.spawn(async move { batch_executor.execute(n).await.unwrap() });
            tasks.push((n, task));
        }

        handle.block_on(async move {
            for (n, task) in tasks {
                let result = task.await.unwrap();
                assert_eq!(result, Some(n));
            }
        });
    });
}

#[divan::bench(args = [250, 1000])]
fn execute_many(bencher: divan::Bencher, size: u64) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _enter = runtime.enter();
    let batch_executor = BatchExecutor::build(ExecuteIdent).finish();
    let handle = runtime.handle();

    bencher.counter(size).bench(|| {
        let mut tasks = vec![];
        let keys = (0..size).collect::<Vec<_>>();
        for batch in keys.chunks(25) {
            let batch = batch.to_vec();
            let batch_executor = batch_executor.clone();
            let task = handle.spawn({
                let batch = batch.clone();
                async move { batch_executor.execute_many(batch).await.unwrap() }
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
