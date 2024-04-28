use std::sync::{Arc, RwLock};

use ultra_batch::{BatchFetcher, Cache, Fetcher, LoadError};

mod db;
mod stubs;

#[tokio::test]
async fn test_load() -> anyhow::Result<()> {
    let db = db::Database::fake();

    let expected_user = db.users.values().next().unwrap().clone();

    let batch_fetcher = BatchFetcher::build(db::FetchUsers {
        db: Arc::new(RwLock::new(db)),
    })
    .finish();
    let actual_user = batch_fetcher.load(expected_user.id).await?;

    assert_eq!(actual_user, expected_user);
    Ok(())
}

#[tokio::test]
async fn test_load_many_with_one_element() -> anyhow::Result<()> {
    let db = db::Database::fake();

    let expected_user = db.users.values().next().unwrap().clone();

    let batch_fetcher = BatchFetcher::build(db::FetchUsers {
        db: Arc::new(RwLock::new(db)),
    })
    .finish();
    let actual_users = batch_fetcher.load_many(&[expected_user.id]).await?;

    assert_eq!(actual_users, &[expected_user.clone()]);
    Ok(())
}

#[tokio::test]
async fn test_load_many_ordering() -> anyhow::Result<()> {
    let db = db::Database::fake();

    let expected_users: Vec<_> = db.users.values().take(5).cloned().collect();
    let user_ids: Vec<_> = expected_users.iter().map(|user| user.id).collect();

    let batch_fetcher = BatchFetcher::build(db::FetchUsers {
        db: Arc::new(RwLock::new(db)),
    })
    .finish();
    let actual_users = batch_fetcher.load_many(&user_ids).await?;

    assert_eq!(actual_users, expected_users);
    Ok(())
}

#[tokio::test]
async fn test_load_fetching() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let user_ids: Vec<_> = db.users.keys().copied().collect();

    let fetcher = stubs::ObserveFetcher::new(db::FetchUsers {
        db: Arc::new(RwLock::new(db)),
    });
    let batch_fetcher = BatchFetcher::build(fetcher.clone()).finish();

    assert_eq!(fetcher.total_calls(), 0);

    let batch = batch_fetcher.load(user_ids[0]).await?;
    assert_eq!(fetcher.total_calls(), 1);
    assert_eq!(fetcher.calls_for_key(&batch.id), 1);

    let batch = batch_fetcher.load_many(&user_ids[10..15]).await?;
    assert_eq!(batch.len(), 5);
    assert_eq!(fetcher.total_calls(), 2);
    assert_eq!(fetcher.calls_for_key(&batch[0].id), 1);

    let batch = batch_fetcher.load_many(&user_ids[100..200]).await?;
    assert_eq!(batch.len(), 100);
    assert_eq!(fetcher.total_calls(), 3);
    assert_eq!(fetcher.calls_for_key(&batch[0].id), 1);

    let batch = batch_fetcher.load_many(&user_ids[200..501]).await?;
    assert_eq!(batch.len(), 301);
    assert_eq!(fetcher.total_calls(), 4);
    assert_eq!(fetcher.calls_for_key(&batch[0].id), 1);

    Ok(())
}

#[tokio::test]
async fn test_load_caching() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let user_ids: Vec<_> = db.users.keys().copied().collect();

    let fetcher = stubs::ObserveFetcher::new(db::FetchUsers {
        db: Arc::new(RwLock::new(db)),
    });
    let batch_fetcher = BatchFetcher::build(fetcher.clone()).finish();

    assert_eq!(fetcher.total_calls(), 0);

    let batch = batch_fetcher.load(user_ids[0]).await?;
    assert_eq!(fetcher.total_calls(), 1);
    assert_eq!(fetcher.calls_for_key(&batch.id), 1);

    let batch = batch_fetcher.load(user_ids[0]).await?;
    assert_eq!(fetcher.total_calls(), 1);
    assert_eq!(fetcher.calls_for_key(&batch.id), 1);

    let batch = batch_fetcher.load_many(&user_ids[0..2]).await?;
    assert_eq!(batch.len(), 2);
    assert_eq!(fetcher.total_calls(), 2);
    assert_eq!(fetcher.calls_for_key(&batch[0].id), 1);
    assert_eq!(fetcher.calls_for_key(&batch[1].id), 1);

    let batch = batch_fetcher.load_many(&user_ids[1..3]).await?;
    assert_eq!(batch.len(), 2);
    assert_eq!(fetcher.total_calls(), 3);
    assert_eq!(fetcher.calls_for_key(&batch[0].id), 1);
    assert_eq!(fetcher.calls_for_key(&batch[1].id), 1);

    let batch = batch_fetcher.load_many(&user_ids[0..3]).await?;
    assert_eq!(batch.len(), 3);
    assert_eq!(fetcher.total_calls(), 3);
    assert_eq!(fetcher.calls_for_key(&batch[0].id), 1);
    assert_eq!(fetcher.calls_for_key(&batch[1].id), 1);
    assert_eq!(fetcher.calls_for_key(&batch[2].id), 1);

    Ok(())
}

#[tokio::test]
async fn test_load_batching() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let user_ids: Vec<_> = db.users.keys().copied().collect();

    let fetcher = stubs::ObserveFetcher::new(db::FetchUsers {
        db: Arc::new(RwLock::new(db)),
    });
    let batch_fetcher = BatchFetcher::build(fetcher.clone()).finish();

    let spawn_batch_fetcher = |batch: &[uuid::Uuid]| {
        let batch_fetcher = batch_fetcher.clone();
        let batch = batch.to_vec();
        async move {
            let task = tokio::spawn(async move { batch_fetcher.load_many(&batch).await.unwrap() });
            task.await.unwrap()
        }
    };

    tokio::join![
        spawn_batch_fetcher(&user_ids[0..1]),
        spawn_batch_fetcher(&user_ids[0..10]),
        spawn_batch_fetcher(&user_ids[5..15]),
        spawn_batch_fetcher(&user_ids[10..20]),
        spawn_batch_fetcher(&user_ids[20..30]),
        spawn_batch_fetcher(&user_ids[30..40]),
        spawn_batch_fetcher(&user_ids[40..50]),
        spawn_batch_fetcher(&user_ids[50..60]),
        spawn_batch_fetcher(&user_ids[60..70]),
        spawn_batch_fetcher(&user_ids[70..80]),
        spawn_batch_fetcher(&user_ids[80..90]),
        spawn_batch_fetcher(&user_ids[0..90]),
    ];

    assert_eq!(fetcher.total_calls(), 1);
    for loaded_user_id in &user_ids[0..90] {
        assert_eq!(fetcher.calls_for_key(loaded_user_id), 1);
    }
    for unloaded_user_id in &user_ids[90..] {
        assert_eq!(fetcher.calls_for_key(unloaded_user_id), 0);
    }

    Ok(())
}

#[tokio::test]
async fn test_load_eager_batch_size() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let user_ids: Vec<_> = db.users.keys().copied().collect();

    let fetcher = stubs::ObserveFetcher::new(db::FetchUsers {
        db: Arc::new(RwLock::new(db)),
    });
    let batch_fetcher = BatchFetcher::build(fetcher.clone())
        .eager_batch_size(Some(50))
        .finish();

    let spawn_batch_fetcher = |batch: &[uuid::Uuid]| {
        let batch_fetcher = batch_fetcher.clone();
        let batch = batch.to_vec();
        async move {
            let task = tokio::spawn(async move { batch_fetcher.load_many(&batch).await.unwrap() });
            task.await.unwrap()
        }
    };

    // We should keep batching until hitting the eager batch threshold
    tokio::join![
        spawn_batch_fetcher(&user_ids[0..1]),
        spawn_batch_fetcher(&user_ids[0..10]),
    ];
    assert_eq!(fetcher.total_calls(), 1);
    for user_id in &user_ids[0..10] {
        assert_eq!(fetcher.calls_for_key(user_id), 1);
    }

    // We should not break up a batch based on the eager batch threshold
    tokio::join![spawn_batch_fetcher(&user_ids[100..200]),];
    assert_eq!(fetcher.total_calls(), 2);
    for user_id in &user_ids[100..200] {
        assert_eq!(fetcher.calls_for_key(user_id), 1);
    }

    // We should keep taking incoming requests until the eager batch threshold is crossed
    tokio::join![
        spawn_batch_fetcher(&user_ids[200..250]),
        spawn_batch_fetcher(&user_ids[250..300]),
    ];
    assert_eq!(fetcher.total_calls(), 4);
    for user_id in &user_ids[200..300] {
        assert_eq!(fetcher.calls_for_key(user_id), 1);
    }

    // The eager batch threshold should only be based on the number of keys that weren't already cached
    tokio::join![
        spawn_batch_fetcher(&user_ids[290..349]),
        spawn_batch_fetcher(&user_ids[349..400]),
    ];
    assert_eq!(fetcher.total_calls(), 5);
    for user_id in &user_ids[290..400] {
        assert_eq!(fetcher.calls_for_key(user_id), 1);
    }

    Ok(())
}

#[tokio::test]
async fn test_load_no_eager_batch_size() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let user_ids: Vec<_> = db.users.keys().copied().collect();

    let fetcher = stubs::ObserveFetcher::new(db::FetchUsers {
        db: Arc::new(RwLock::new(db)),
    });
    let batch_fetcher = BatchFetcher::build(fetcher.clone())
        .eager_batch_size(None)
        .finish();

    let tasks: Vec<_> = user_ids
        .iter()
        .cloned()
        .map(|user_id| {
            let batch_fetcher = batch_fetcher.clone();
            tokio::spawn(async move { batch_fetcher.load(user_id).await.unwrap() })
        })
        .collect();

    for task in tasks {
        task.await?;
    }

    // When no eager batch size is set, we should just keep accepting new keys into the batch (assuming
    // we don't exceed the delay duration)
    assert_eq!(fetcher.total_calls(), 1);
    for user_id in &user_ids {
        assert_eq!(fetcher.calls_for_key(user_id), 1);
    }

    Ok(())
}

#[tokio::test]
async fn test_batch_delay() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let user_ids: Vec<_> = db.users.keys().copied().collect();

    let fetcher = stubs::ObserveFetcher::new(db::FetchUsers {
        db: Arc::new(RwLock::new(db)),
    });
    let batch_fetcher = BatchFetcher::build(fetcher.clone())
        .delay_duration(tokio::time::Duration::from_millis(10))
        .eager_batch_size(None)
        .finish();

    // Batch run if we exceed the delay duration
    let batch_task = tokio::spawn({
        let batch_fetcher = batch_fetcher.clone();
        let user_id = user_ids[0];
        async move { batch_fetcher.load(user_id).await }
    });
    assert_eq!(fetcher.total_calls(), 0);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert_eq!(fetcher.total_calls(), 1);
    batch_task.await??;
    assert_eq!(fetcher.total_calls(), 1);

    Ok(())
}

#[tokio::test]
async fn test_insert_extra_keys() -> Result<(), anyhow::Error> {
    // Fetcher that returns the input value, and also always inserts the value 1
    struct OneFetcher;

    impl Fetcher for OneFetcher {
        type Key = u64;
        type Value = u64;
        type Error = anyhow::Error;

        async fn fetch(
            &self,
            keys: &[u64],
            values: &mut Cache<'_, u64, u64>,
        ) -> Result<(), Self::Error> {
            values.insert(1, 1);
            for key in keys {
                values.insert(*key, *key);
            }

            Ok(())
        }
    }

    let fetcher = stubs::ObserveFetcher::new(OneFetcher);
    let batch_fetcher = BatchFetcher::build(fetcher.clone()).finish();

    let batch = batch_fetcher.load_many(&[2, 3, 4]).await?;
    assert_eq!(batch, vec![2, 3, 4]);
    assert_eq!(fetcher.total_calls(), 1);
    assert_eq!(fetcher.calls_for_key(&1), 0);
    assert_eq!(fetcher.calls_for_key(&2), 1);
    assert_eq!(fetcher.calls_for_key(&3), 1);
    assert_eq!(fetcher.calls_for_key(&4), 1);

    let batch = batch_fetcher.load(1).await?;
    assert_eq!(batch, 1);
    assert_eq!(fetcher.total_calls(), 1);
    assert_eq!(fetcher.calls_for_key(&1), 0);
    assert_eq!(fetcher.calls_for_key(&2), 1);
    assert_eq!(fetcher.calls_for_key(&3), 1);
    assert_eq!(fetcher.calls_for_key(&4), 1);

    let batch = batch_fetcher.load_many(&[1, 2, 3]).await?;
    assert_eq!(batch, vec![1, 2, 3]);
    assert_eq!(fetcher.total_calls(), 1);
    assert_eq!(fetcher.calls_for_key(&1), 0);
    assert_eq!(fetcher.calls_for_key(&2), 1);
    assert_eq!(fetcher.calls_for_key(&3), 1);
    assert_eq!(fetcher.calls_for_key(&4), 1);

    Ok(())
}

#[tokio::test]
async fn test_keys_not_returned() -> Result<(), anyhow::Error> {
    // Fetcher that only returns values for even keys (odd keys are ignored)
    struct EvenFetcher;

    impl Fetcher for EvenFetcher {
        type Key = u64;
        type Value = u64;
        type Error = anyhow::Error;

        async fn fetch(
            &self,
            keys: &[u64],
            values: &mut Cache<'_, u64, u64>,
        ) -> Result<(), Self::Error> {
            for key in keys {
                if key % 2 == 0 {
                    values.insert(*key, *key);
                }
            }

            Ok(())
        }
    }

    let fetcher = stubs::ObserveFetcher::new(EvenFetcher);
    let batch_fetcher = BatchFetcher::build(fetcher.clone()).finish();

    let batch = batch_fetcher.load_many(&[2, 4, 6]).await?;
    assert_eq!(batch, vec![2, 4, 6]);
    assert_eq!(fetcher.total_calls(), 1);
    assert_eq!(fetcher.calls_for_key(&2), 1);
    assert_eq!(fetcher.calls_for_key(&4), 1);
    assert_eq!(fetcher.calls_for_key(&6), 1);

    let batch_result = batch_fetcher.load_many(&[2, 8, 10, 11]).await;
    assert!(matches!(batch_result, Err(LoadError::NotFound)));
    assert_eq!(fetcher.total_calls(), 2);
    assert_eq!(fetcher.calls_for_key(&2), 1);
    assert_eq!(fetcher.calls_for_key(&8), 1);
    assert_eq!(fetcher.calls_for_key(&10), 1);
    assert_eq!(fetcher.calls_for_key(&11), 1);

    let batch = batch_fetcher.load_many(&[2, 4, 6, 8, 10]).await?;
    assert_eq!(batch, vec![2, 4, 6, 8, 10]);
    assert_eq!(fetcher.total_calls(), 2);
    assert_eq!(fetcher.calls_for_key(&2), 1);
    assert_eq!(fetcher.calls_for_key(&4), 1);
    assert_eq!(fetcher.calls_for_key(&6), 1);
    assert_eq!(fetcher.calls_for_key(&8), 1);
    assert_eq!(fetcher.calls_for_key(&10), 1);
    assert_eq!(fetcher.calls_for_key(&11), 1);

    let batch_result = batch_fetcher.load_many(&[11, 12]).await;
    assert!(matches!(batch_result, Err(LoadError::NotFound)));
    assert_eq!(fetcher.calls_for_key(&11), 1); // "Not found" status should be cached
    assert_eq!(fetcher.calls_for_key(&12), 1);

    Ok(())
}

#[tokio::test]
async fn test_fetch_error_before_inserting() -> Result<(), anyhow::Error> {
    // Fetcher that first validates no odd keys are present, then stores even keys
    struct EvenFetcher;

    impl Fetcher for EvenFetcher {
        type Key = u64;
        type Value = u64;
        type Error = anyhow::Error;

        async fn fetch(
            &self,
            keys: &[u64],
            values: &mut Cache<'_, u64, u64>,
        ) -> Result<(), Self::Error> {
            let (even_keys, mut odd_keys): (Vec<u64>, Vec<u64>) =
                keys.iter().partition(|&&key| key % 2 == 0);

            // Sort odd keys so we return consistent error messages
            odd_keys.sort_unstable();
            if !odd_keys.is_empty() {
                return Err(anyhow::anyhow!("odd keys: {:?}", odd_keys));
            }

            for key in even_keys {
                values.insert(key, key);
            }

            Ok(())
        }
    }

    let fetcher = stubs::ObserveFetcher::new(EvenFetcher);
    let batch_fetcher = BatchFetcher::build(fetcher.clone()).finish();

    let batch = batch_fetcher.load_many(&[2, 4, 6]).await?;
    assert_eq!(batch, vec![2, 4, 6]);
    assert_eq!(fetcher.total_calls(), 1);
    assert_eq!(fetcher.calls_for_key(&2), 1);
    assert_eq!(fetcher.calls_for_key(&4), 1);
    assert_eq!(fetcher.calls_for_key(&6), 1);

    let batch_result = batch_fetcher.load_many(&[2, 8, 10, 11, 13]).await;
    assert!(matches!(batch_result, Err(LoadError::FetchError(msg)) if msg == "odd keys: [11, 13]"));
    assert_eq!(fetcher.total_calls(), 2);
    assert_eq!(fetcher.calls_for_key(&2), 1);
    assert_eq!(fetcher.calls_for_key(&8), 1);
    assert_eq!(fetcher.calls_for_key(&10), 1);
    assert_eq!(fetcher.calls_for_key(&11), 1);
    assert_eq!(fetcher.calls_for_key(&13), 1);

    let batch = batch_fetcher.load_many(&[2, 4, 6, 8, 10]).await?;
    assert_eq!(batch, vec![2, 4, 6, 8, 10]);
    assert_eq!(fetcher.total_calls(), 3);
    assert_eq!(fetcher.calls_for_key(&2), 1);
    assert_eq!(fetcher.calls_for_key(&4), 1);
    assert_eq!(fetcher.calls_for_key(&6), 1);
    assert_eq!(fetcher.calls_for_key(&8), 2); // Previously errored out, so it should be retried
    assert_eq!(fetcher.calls_for_key(&10), 2); // Previously errored out, so it should be retried
    assert_eq!(fetcher.calls_for_key(&11), 1);

    let batch_result = batch_fetcher.load_many(&[11, 12]).await;
    assert!(matches!(batch_result, Err(LoadError::FetchError(msg)) if msg == "odd keys: [11]"));
    assert_eq!(fetcher.calls_for_key(&11), 2); // Previously errored out, so it should be retried
    assert_eq!(fetcher.calls_for_key(&12), 1);

    Ok(())
}

#[tokio::test]
async fn test_fetch_error_after_inserting() -> Result<(), anyhow::Error> {
    // Fetcher that stores even keys, then errors out if any odd keys are present
    struct EvenFetcher;

    impl Fetcher for EvenFetcher {
        type Key = u64;
        type Value = u64;
        type Error = anyhow::Error;

        async fn fetch(
            &self,
            keys: &[u64],
            values: &mut Cache<'_, u64, u64>,
        ) -> Result<(), Self::Error> {
            let (even_keys, mut odd_keys): (Vec<u64>, Vec<u64>) =
                keys.iter().partition(|&&key| key % 2 == 0);

            for key in even_keys {
                values.insert(key, key);
            }

            // Sort odd keys so we return consistent error messages
            odd_keys.sort_unstable();
            if !odd_keys.is_empty() {
                return Err(anyhow::anyhow!("odd keys: {:?}", odd_keys));
            }

            Ok(())
        }
    }

    let fetcher = stubs::ObserveFetcher::new(EvenFetcher);
    let batch_fetcher = BatchFetcher::build(fetcher.clone()).finish();

    let batch = batch_fetcher.load_many(&[2, 4, 6]).await?;
    assert_eq!(batch, vec![2, 4, 6]);
    assert_eq!(fetcher.total_calls(), 1);
    assert_eq!(fetcher.calls_for_key(&2), 1);
    assert_eq!(fetcher.calls_for_key(&4), 1);
    assert_eq!(fetcher.calls_for_key(&6), 1);

    let batch_result = batch_fetcher.load_many(&[2, 8, 10, 11, 13]).await;
    assert!(matches!(batch_result, Err(LoadError::FetchError(msg)) if msg == "odd keys: [11, 13]"));
    assert_eq!(fetcher.total_calls(), 2);
    assert_eq!(fetcher.calls_for_key(&2), 1);
    assert_eq!(fetcher.calls_for_key(&8), 1);
    assert_eq!(fetcher.calls_for_key(&10), 1);
    assert_eq!(fetcher.calls_for_key(&11), 1);
    assert_eq!(fetcher.calls_for_key(&13), 1);

    let batch = batch_fetcher.load_many(&[2, 4, 6, 8, 10]).await?;
    assert_eq!(batch, vec![2, 4, 6, 8, 10]);
    assert_eq!(fetcher.total_calls(), 2);
    assert_eq!(fetcher.calls_for_key(&2), 1);
    assert_eq!(fetcher.calls_for_key(&4), 1);
    assert_eq!(fetcher.calls_for_key(&6), 1);
    assert_eq!(fetcher.calls_for_key(&8), 1); // Saved in previous (failed) batch-- value is still valid
    assert_eq!(fetcher.calls_for_key(&10), 1); // Saved in previous (failed) batch-- value is still valid
    assert_eq!(fetcher.calls_for_key(&11), 1);

    let batch_result = batch_fetcher.load_many(&[11, 12]).await;
    assert!(matches!(batch_result, Err(LoadError::FetchError(msg)) if msg == "odd keys: [11]"));
    assert_eq!(fetcher.calls_for_key(&11), 2); // Previously errored out, so it should be retried
    assert_eq!(fetcher.calls_for_key(&12), 1);

    Ok(())
}
