use ultra_batch::Batcher;

mod db;
mod stubs;

#[tokio::test]
async fn test_load() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let batcher = Batcher::new(db::FetchUsers { db: db.clone() });

    let expected_user = &db.users[0];

    let actual_user = batcher.load(expected_user.id).await?;

    assert_eq!(&actual_user, expected_user);
    Ok(())
}

#[tokio::test]
async fn test_load_many_with_one_element() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let batcher = Batcher::new(db::FetchUsers { db: db.clone() });

    let expected_user = &db.users[0];

    let actual_users = batcher.load_many(&[expected_user.id]).await?;

    assert_eq!(actual_users, &[expected_user.clone()]);
    Ok(())
}

#[tokio::test]
async fn test_load_many_ordering() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let batcher = Batcher::new(db::FetchUsers { db: db.clone() });

    let expected_users = &db.users[0..5];

    let user_ids: Vec<_> = expected_users.iter().map(|user| user.id).collect();
    let actual_users = batcher.load_many(&user_ids).await?;

    assert_eq!(actual_users, expected_users);
    Ok(())
}

#[tokio::test]
async fn test_load_fetching() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let fetcher = stubs::ObserveFetcher::new(db::FetchUsers { db: db.clone() });
    let batcher = Batcher::new(fetcher.clone());

    let user_ids: Vec<_> = db.users.iter().map(|user| user.id).collect();

    assert_eq!(fetcher.total_calls(), 0);

    let batch = batcher.load(user_ids[0]).await?;
    assert_eq!(fetcher.total_calls(), 1);
    assert_eq!(fetcher.calls_for_key(&batch.id), 1);

    let batch = batcher.load_many(&user_ids[10..15]).await?;
    assert_eq!(batch.len(), 5);
    assert_eq!(fetcher.total_calls(), 2);
    assert_eq!(fetcher.calls_for_key(&batch[0].id), 1);

    let batch = batcher.load_many(&user_ids[100..200]).await?;
    assert_eq!(batch.len(), 100);
    assert_eq!(fetcher.total_calls(), 3);
    assert_eq!(fetcher.calls_for_key(&batch[0].id), 1);

    let batch = batcher.load_many(&user_ids[200..501]).await?;
    assert_eq!(batch.len(), 301);
    assert_eq!(fetcher.total_calls(), 4);
    assert_eq!(fetcher.calls_for_key(&batch[0].id), 1);

    Ok(())
}

#[tokio::test]
async fn test_load_caching() -> anyhow::Result<()> {
    let db = db::Database::fake();
    let fetcher = stubs::ObserveFetcher::new(db::FetchUsers { db: db.clone() });
    let batcher = Batcher::new(fetcher.clone());

    let user_ids: Vec<_> = db.users.iter().map(|user| user.id).collect();

    assert_eq!(fetcher.total_calls(), 0);

    let batch = batcher.load(user_ids[0]).await?;
    assert_eq!(fetcher.total_calls(), 1);
    assert_eq!(fetcher.calls_for_key(&batch.id), 1);

    let batch = batcher.load(user_ids[0]).await?;
    assert_eq!(fetcher.total_calls(), 1);
    assert_eq!(fetcher.calls_for_key(&batch.id), 1);

    let batch = batcher.load_many(&user_ids[0..2]).await?;
    assert_eq!(batch.len(), 2);
    assert_eq!(fetcher.total_calls(), 2);
    assert_eq!(fetcher.calls_for_key(&batch[0].id), 1);
    assert_eq!(fetcher.calls_for_key(&batch[1].id), 1);

    let batch = batcher.load_many(&user_ids[1..3]).await?;
    assert_eq!(batch.len(), 2);
    assert_eq!(fetcher.total_calls(), 3);
    assert_eq!(fetcher.calls_for_key(&batch[0].id), 1);
    assert_eq!(fetcher.calls_for_key(&batch[1].id), 1);

    let batch = batcher.load_many(&user_ids[0..3]).await?;
    assert_eq!(batch.len(), 3);
    assert_eq!(fetcher.total_calls(), 3);
    assert_eq!(fetcher.calls_for_key(&batch[0].id), 1);
    assert_eq!(fetcher.calls_for_key(&batch[1].id), 1);
    assert_eq!(fetcher.calls_for_key(&batch[2].id), 1);

    Ok(())
}
