use std::sync::{atomic::AtomicUsize, Arc, RwLock};

use ultra_batch::{BatchExecutor, ExecuteError, Executor};

mod db;
mod stubs;

#[tokio::test]
async fn test_execute() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let db = Arc::new(RwLock::new(db));

    let new_user = db::User::fake();

    let batch_inserter = BatchExecutor::build(db::InsertUsers { db: db.clone() }).finish();
    let result = batch_inserter.execute(new_user.clone()).await?;

    assert_eq!(result, Some(Some(new_user.id)));

    let db = db.read().unwrap();
    assert!(db.users.contains_key(&new_user.id));

    Ok(())
}

#[tokio::test]
async fn test_execute_returning_nothing() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let db = Arc::new(RwLock::new(db));

    let new_user = db::User::fake();

    let batch_inserter = BatchExecutor::build(stubs::ExecutorReturnsEmpty(db::InsertUsers {
        db: db.clone(),
    }))
    .finish();
    let result = batch_inserter.execute(new_user.clone()).await?;

    assert_eq!(result, None);

    let db = db.read().unwrap();
    assert!(db.users.contains_key(&new_user.id));

    Ok(())
}

#[tokio::test]
async fn test_execute_many_with_one_element() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let db = Arc::new(RwLock::new(db));

    let new_user = db::User::fake();

    let batch_inserter = BatchExecutor::build(db::InsertUsers { db: db.clone() }).finish();
    let result = batch_inserter.execute_many(vec![new_user.clone()]).await?;

    assert_eq!(result, [Some(new_user.id)]);

    let db = db.read().unwrap();
    assert!(db.users.contains_key(&new_user.id));

    Ok(())
}

#[tokio::test]
async fn test_execute_many_with_one_element_returning_nothing() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let db = Arc::new(RwLock::new(db));

    let new_user = db::User::fake();

    let batch_inserter = BatchExecutor::build(stubs::ExecutorReturnsEmpty(db::InsertUsers {
        db: db.clone(),
    }))
    .finish();
    let result = batch_inserter.execute_many(vec![new_user.clone()]).await?;

    assert_eq!(result, []);

    let db = db.read().unwrap();
    assert!(db.users.contains_key(&new_user.id));

    Ok(())
}

#[tokio::test]
async fn test_execute_many_ordering() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let existing_user = db.users.values().next().cloned().unwrap();
    let new_user_1 = db::User::fake();
    let new_user_2 = db::User::fake();

    let db = Arc::new(RwLock::new(db));

    let inserts = vec![
        existing_user.clone(),
        new_user_1.clone(),
        new_user_1.clone(),
        new_user_2.clone(),
        new_user_2.clone(),
    ];

    let batch_executor = BatchExecutor::build(db::InsertUsers { db: db.clone() }).finish();
    let results = batch_executor.execute_many(inserts).await?;

    assert_eq!(
        results,
        [None, Some(new_user_1.id), None, Some(new_user_2.id), None]
    );
    Ok(())
}

#[tokio::test]
async fn test_execute_big_batch() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let db = Arc::new(RwLock::new(db));

    let inserts: Vec<_> = (0..1000).map(|_| db::User::fake()).collect();

    let executor = stubs::ObserveExecutor::new(db::InsertUsers { db: db.clone() });
    let batch_executor = BatchExecutor::build(executor.clone())
        .eager_batch_size(Some(50))
        .finish();
    let results = batch_executor.execute_many(inserts).await?;
    let num_results = results.into_iter().flatten().count();

    assert_eq!(num_results, 1000);
    assert_eq!(executor.total_calls(), 1);

    Ok(())
}

#[tokio::test]
async fn test_execute_small_awaited_batches() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let db = Arc::new(RwLock::new(db));

    let executor = stubs::ObserveExecutor::new(db::InsertUsers { db: db.clone() });
    let batch_executor = BatchExecutor::build(executor.clone())
        .eager_batch_size(Some(50))
        .finish();

    // Because each execution is `await`ed, each execution runs in sequence.
    // This means each batch should execute before the next one is started,
    // meaning we do 50 different batches
    let mut num_results = 0;
    for _ in 0..50 {
        let inserts: Vec<_> = (0..10).map(|_| db::User::fake()).collect();

        let results = batch_executor.execute_many(inserts).await?;
        num_results += results.into_iter().flatten().count();
    }

    assert_eq!(num_results, 500);
    assert_eq!(executor.total_calls(), 50);

    Ok(())
}

#[tokio::test]
async fn test_execute_merged_batches() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let db = Arc::new(RwLock::new(db));

    let executor = stubs::ObserveExecutor::new(db::InsertUsers { db: db.clone() });
    let batch_executor = BatchExecutor::build(executor.clone())
        .eager_batch_size(Some(50))
        .finish();

    let num_results = Arc::new(AtomicUsize::new(0));

    let spawn_batch_executor = || {
        let num_results = num_results.clone();
        let batch_executor = batch_executor.clone();
        let inserts: Vec<_> = (0..10).map(|_| db::User::fake()).collect();
        async move {
            let task =
                tokio::spawn(async move { batch_executor.execute_many(inserts).await.unwrap() });
            let results = task.await.unwrap();
            num_results.fetch_add(
                results.into_iter().flatten().count(),
                std::sync::atomic::Ordering::SeqCst,
            );
        }
    };

    // Execute each batch in parallel, which should get grouped into 3
    // different batches
    tokio::join![
        // Batch 1
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        // Batch 2
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        // Batch 3
        spawn_batch_executor(),
        spawn_batch_executor(),
    ];

    assert_eq!(num_results.load(std::sync::atomic::Ordering::SeqCst), 120);
    assert_eq!(executor.total_calls(), 3);

    Ok(())
}

#[tokio::test]
async fn test_execute_merged_batches_returning_none() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let db = Arc::new(RwLock::new(db));

    let executor = stubs::ObserveExecutor::new(stubs::ExecutorReturnsEmpty(db::InsertUsers {
        db: db.clone(),
    }));
    let batch_executor = BatchExecutor::build(executor.clone())
        .eager_batch_size(Some(50))
        .finish();

    let num_results = Arc::new(AtomicUsize::new(0));

    let spawn_batch_executor = || {
        let num_results = num_results.clone();
        let batch_executor = batch_executor.clone();
        let inserts: Vec<_> = (0..10).map(|_| db::User::fake()).collect();
        async move {
            let task =
                tokio::spawn(async move { batch_executor.execute_many(inserts).await.unwrap() });
            let results = task.await.unwrap();

            // This should return 0 because each batch is returning an empty
            // `Vec`
            num_results.fetch_add(
                results.into_iter().flatten().count(),
                std::sync::atomic::Ordering::SeqCst,
            );
        }
    };

    // Execute each batch in parallel, which should get grouped into 3
    // different batches
    tokio::join![
        // Batch 1
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        // Batch 2
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        // Batch 3
        spawn_batch_executor(),
        spawn_batch_executor(),
    ];

    assert_eq!(num_results.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(executor.total_calls(), 3);

    Ok(())
}

#[tokio::test]
async fn test_execute_merged_batches_returning_error() -> anyhow::Result<()> {
    struct ErrorExecutor;

    impl Executor for ErrorExecutor {
        type Value = db::User;
        type Result = Option<uuid::Uuid>;
        type Error = anyhow::Error;

        async fn execute(
            &self,
            _values: Vec<Self::Value>,
        ) -> Result<Vec<Self::Result>, Self::Error> {
            anyhow::bail!("uh oh");
        }
    }

    let executor = stubs::ObserveExecutor::new(ErrorExecutor);
    let batch_executor = BatchExecutor::build(executor.clone())
        .eager_batch_size(Some(50))
        .finish();

    let results = Arc::new(RwLock::new(vec![]));

    let spawn_batch_executor = || {
        let results = results.clone();
        let batch_executor = batch_executor.clone();
        let inserts: Vec<_> = (0..10).map(|_| db::User::fake()).collect();
        async move {
            let task = tokio::spawn(async move { batch_executor.execute_many(inserts).await });
            let task_results = task.await.unwrap();

            let mut results = results.write().unwrap();
            results.push(task_results);
        }
    };

    // Execute each batch in parallel, which should get grouped into 3
    // different batches
    tokio::join![
        // Batch 1
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        // Batch 2
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        spawn_batch_executor(),
        // Batch 3
        spawn_batch_executor(),
        spawn_batch_executor(),
    ];

    let results = results.read().unwrap();
    assert_eq!(executor.total_calls(), 3);
    assert_eq!(results.len(), 12);
    for result in &*results {
        assert!(matches!(result, Err(ExecuteError::ExecutorError(_))));
    }

    Ok(())
}
