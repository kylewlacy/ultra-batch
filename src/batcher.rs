use crate::cache::{CacheLookup, CacheLookupState, CacheStore};
use crate::Fetcher;
use std::collections::HashSet;
use std::sync::Arc;

/// Used to batch and cache loads from some datastore. A `Batcher` can be used
/// with any type that implements [`Fetcher`](trait.Fetcher.html). `Batcher`s
/// are asynchronous, and designed to be passed and shared between threads or
/// tasks. Cloning a `Batcher` is shallow and can be used to use the same
/// `Fetcher` across multiple threads or tasks.
///
/// A `Batcher` is designed primarily around batching database lookups-- for
/// example, fetching a user from a user ID, where a signle query to retrieve
/// 50 users by ID is significantly faster than 50 separate queries to look up
/// the same set of users.
///
/// A `Batcher` is designed to be ephemeral. In the context of a web service,
/// this means callers should most likely create a new `Batcher` for each
/// request, and **not** a `Batcher` shared across multiple requests.
/// `Batcher`s have no concept of cache invalidation, so old values are stored
/// indefinitely (which means callers may get stale data or may exhaust memory
/// endlessly).
///
/// `Batcher`s introduce a small amount of latency for loads. Each time a
/// `Batcher` receives a key to fetch that hasn't been cached (or a set of
/// keys), it will first wait for more keys to build a batch. The load will only
/// trigger after a timeout is reached or once enough keys have been queued in
/// the batch. See [`BatcherBuilder`](struct.BatcherBuilder.html) for options
/// to tweak latency and batch sizes.
///
/// ## Load semantics
///
/// If the underlying `Fetcher` returns an error during the batch request, then
/// all pending [`load`](struct.Batcher.html#method.load) and [`load_many`](struct.Batcher.html#method.load_many)
/// requests will fail. Subsequent calls to [`load`](struct.Batcher.html#method.load)
/// or [`load_many`](struct.Batcher.html#method.load_many) with the same
/// keys **will retry**.
///
/// If the underlying `Fetcher` succeeds but does not return a value for a
/// given key during a batch request, then the `Batcher` will mark that key as
/// "not found" and an eror value of [`NotFound`](enum.LoadError.html#variant.NotFound)
/// will be returned to all pending [`load`](struct.Batcher.html#method.load)
/// and [`load_many`](struct.Batcher.html#method.load_many) requests. The
/// "not found" status will be preserved, so subsequent calls with the same key
/// will fail and **will not retry**.
pub struct Batcher<F>
where
    F: Fetcher,
{
    cache_store: CacheStore<F::Key, F::Value>,
    _fetch_task: Arc<tokio::task::JoinHandle<()>>,
    fetch_request_tx: tokio::sync::mpsc::Sender<FetchRequest<F::Key>>,
}

impl<F> Batcher<F>
where
    F: Fetcher + Send + Sync + 'static,
{
    /// Create a new `Batcher` that uses the given `Fetcher` to retrieve data.
    /// Returns a [`BatcherBuilder`](struct.BatcherBuilder.html), which can be
    /// used to customize the `Batcher`. Call [`build()`](struct.BatcherBuilder.html#method.build)
    /// to create the `Batcher`.
    ///
    /// # Examples
    ///
    /// Creating a `Batcher` with default options:
    ///
    /// ```
    /// # use async_trait::async_trait;
    /// # use ultra_batch::{Batcher, Fetcher, Cache};
    /// # struct UserFetcher;
    /// # impl UserFetcher {
    /// #     fn new(db_conn: ()) -> Self { UserFetcher }
    /// #  }
    /// # #[async_trait]
    /// # impl Fetcher for UserFetcher {
    /// #     type Key = ();
    /// #     type Value = ();
    /// #     type Error = anyhow::Error;
    /// #     async fn fetch(&self, keys: &[()], values: &mut Cache<'_, (), ()>) -> anyhow::Result<()> {
    /// #         unimplemented!();
    /// #     }
    /// # }
    /// # #[tokio::main] async fn main() -> anyhow::Result<()> {
    /// # let db_conn = ();
    /// let user_fetcher = UserFetcher::new(db_conn);
    /// let batcher = Batcher::new(user_fetcher).build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Creating a `Batcher` with custom options:
    ///
    /// ```
    /// # use async_trait::async_trait;
    /// # use ultra_batch::{Batcher, Fetcher, Cache};
    /// # struct UserFetcher;
    /// # impl UserFetcher {
    /// #     fn new(db_conn: ()) -> Self { UserFetcher }
    /// #  }
    /// # #[async_trait]
    /// # impl Fetcher for UserFetcher {
    /// #     type Key = ();
    /// #     type Value = ();
    /// #     type Error = anyhow::Error;
    /// #     async fn fetch(&self, keys: &[()], values: &mut Cache<'_, (), ()>) -> anyhow::Result<()> {
    /// #         unimplemented!();
    /// #     }
    /// # }
    /// # #[tokio::main] async fn main() -> anyhow::Result<()> {
    /// # let db_conn = ();
    /// let user_fetcher = UserFetcher::new(db_conn);
    /// let batcher = Batcher::new(user_fetcher)
    ///     .eager_batch_size(Some(50))
    ///     .delay_duration(tokio::time::Duration::from_millis(5))
    ///     .build();
    /// # Ok(()) }
    /// ```
    pub fn new(fetcher: F) -> BatcherBuilder<F> {
        BatcherBuilder {
            fetcher,
            delay_duration: tokio::time::Duration::from_millis(10),
            eager_batch_size: Some(100),
        }
    }

    /// Load the value with the associated key, either by calling the `Fetcher`
    /// or by loading the cached value. Returns an error if the value could
    /// not be loaded or if a value for the given key was not found.
    ///
    /// See the type-level docs for [`Batcher`](#load-semantics) for more
    /// detailed loading semantics.
    pub async fn load(&self, key: F::Key) -> Result<F::Value, LoadError> {
        let mut values = self.load_many(&[key]).await?;
        Ok(values.remove(0))
    }

    /// Load all the values for the given keys, either by calling the `Fetcher`
    /// or by loading cached values. Values are returned in the same order as
    /// the input keys. Returns an error if _any_ load fails.
    ///
    /// See the type-level docs for [`Batcher`](#load-semantics) for more
    /// detailed loading semantics.
    pub async fn load_many(&self, keys: &[F::Key]) -> Result<Vec<F::Value>, LoadError> {
        let mut cache_lookup = CacheLookup::new(keys.to_vec());

        match cache_lookup.lookup(&self.cache_store).await {
            CacheLookupState::Done(result) => {
                return result;
            }
            CacheLookupState::Pending => {}
        }
        let pending_keys = cache_lookup.pending_keys();

        let mut fetch_request_tx = self.fetch_request_tx.clone();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let fetch_request = FetchRequest {
            keys: pending_keys,
            result_tx,
        };
        fetch_request_tx
            .send(fetch_request)
            .await
            .map_err(|_| LoadError::SendError)?;

        match result_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(fetch_error)) => {
                return Err(LoadError::FetchError(fetch_error));
            }
            Err(recv_error) => {
                panic!("Batch result channel hung up with error: {}", recv_error);
            }
        }

        match cache_lookup.lookup(&self.cache_store).await {
            CacheLookupState::Done(result) => {
                return result;
            }
            CacheLookupState::Pending => {
                panic!("Batch result is still pending after result channel was sent");
            }
        }
    }
}

impl<F> Clone for Batcher<F>
where
    F: Fetcher,
{
    fn clone(&self) -> Self {
        Batcher {
            cache_store: self.cache_store.clone(),
            _fetch_task: self._fetch_task.clone(),
            fetch_request_tx: self.fetch_request_tx.clone(),
        }
    }
}

/// Used to configure a new [`Batcher`](struct.Batcher.html). A `BatcherBuilder`
/// is returned from [`Batcher::new()`](struct.Batcher.html#method.new).
pub struct BatcherBuilder<F>
where
    F: Fetcher + Send + Sync + 'static,
{
    fetcher: F,
    delay_duration: tokio::time::Duration,
    eager_batch_size: Option<usize>,
}

impl<F> BatcherBuilder<F>
where
    F: Fetcher + Send + Sync + 'static,
{
    /// The maximum amount of time the `Batcher` will wait to queue up more
    /// keys before calling the [`Fetcher`](trait.Fetcher.html).
    pub fn delay_duration(mut self, delay: tokio::time::Duration) -> Self {
        self.delay_duration = delay;
        self
    }

    /// The maximum number of keys to wait for before eagerly calling the [`Fetcher`](trait.Fetcher.html).
    /// A value of `Some(n)` will load the batch once `n` or more keys have
    /// been queued (or once the timeout set by [`delay_duration`](struct.BatcherBuilder.html#method.delay_duration)
    /// is reached, whichever comes first). A value of `None` will never eagerly
    /// dispatch the queue, and the [`Batcher`](struct.Batcher.html) will always
    /// wait for the timeout set by [`delay_duration`](struct.BatcherBuilder.html#method.delay_duration).
    ///
    /// Note that `eager_batch_size` **does not** set an upper limit on the
    /// batch! For example, if [`Batcher.load_many`](struct.Batcher.html#method.load_many)
    /// is called with more than `eager_batch_size` items, then the batch
    /// will be sent immediately with _all_ of the provided keys.
    pub fn eager_batch_size(mut self, eager_batch_size: Option<usize>) -> Self {
        self.eager_batch_size = eager_batch_size;
        self
    }

    /// Create and return a `Batcher` with the given options.
    pub fn build(self) -> Batcher<F> {
        let cache_store = CacheStore::new();

        let (fetch_request_tx, mut fetch_request_rx) =
            tokio::sync::mpsc::channel::<FetchRequest<F::Key>>(1);

        let fetch_task = tokio::spawn({
            let cache_store = cache_store.clone();
            async move {
                'task: loop {
                    // Wait for some keys to come in
                    let mut pending_keys = HashSet::new();
                    let mut result_txs = vec![];
                    match fetch_request_rx.recv().await {
                        Some(fetch_request) => {
                            for key in fetch_request.keys {
                                pending_keys.insert(key);
                            }
                            result_txs.push(fetch_request.result_tx);
                        }
                        None => {
                            // Fetch queue closed, so we're done
                            break 'task;
                        }
                    };

                    // Wait for more keys
                    'wait_for_more_keys: loop {
                        let should_run_batch_now = match self.eager_batch_size {
                            Some(eager_batch_size) => pending_keys.len() >= eager_batch_size,
                            None => false,
                        };
                        if should_run_batch_now {
                            // We have enough keys already, so don't wait for more
                            break 'wait_for_more_keys;
                        }

                        let mut delay = tokio::time::delay_for(self.delay_duration);
                        tokio::select! {
                            fetch_request = fetch_request_rx.recv() => {
                                match fetch_request {
                                    Some(fetch_request) => {
                                        for key in fetch_request.keys {
                                            pending_keys.insert(key);
                                        }
                                        result_txs.push(fetch_request.result_tx);
                                    }
                                    None => {
                                        // Fetch queuie closed, so we're done waiting for keys
                                        break 'wait_for_more_keys;
                                    }
                                }

                            }
                            _ = &mut delay => {
                                // Reached delay, so we're done waiting for keys
                                break 'wait_for_more_keys;
                            }
                        }
                    }

                    let result = {
                        let mut cache = cache_store.as_cache();

                        let pending_keys: Vec<_> = pending_keys.into_iter().collect();
                        let result = self
                            .fetcher
                            .fetch(&pending_keys, &mut cache)
                            .await
                            .map_err(|error| error.to_string());

                        if result.is_ok() {
                            cache.mark_keys_not_found(pending_keys);
                        }

                        result
                    };

                    for result_tx in result_txs {
                        // Ignore error if receiver was already closed
                        let _ = result_tx.send(result.clone());
                    }
                }
            }
        });

        Batcher {
            cache_store,
            _fetch_task: Arc::new(fetch_task),
            fetch_request_tx,
        }
    }
}

struct FetchRequest<K> {
    keys: Vec<K>,
    result_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
}

/// Error indicating that loading one or more values from a [`Batcher`](struct.Batcher.html)
/// failed.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// The [`Fetcher`](trait.Fetcher.html) returned an error while loading
    /// loading the batch. The message contains the error message specified
    /// by [`Fetcher::Error`](trait.Fetcher.html#associatedtype.Error).
    #[error("error while fetching from batch: {}", _0)]
    FetchError(String),

    /// The request could not be sent to the [`Batcher`](struct.Batcher.html).
    #[error("error sending fetch request")]
    SendError,

    /// The [`Fetcher`](trait.Fetcher.html) did not return a value for one or
    /// more keys in the batch.
    #[error("value not found")]
    NotFound,
}
