use crate::cache::{CacheLookup, CacheLookupState, CacheStore};
use crate::Fetcher;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

/// Batches and caches loads from some datastore. A `BatchFetcher` can be
/// used with any type that implements [`Fetcher`]. `BatchFetcher`s are
/// asynchronous and designed to be passed and shared between threads or tasks.
/// Cloning a `BatchFetcher` is shallow and will use the same [`Fetcher`].
///
/// `BatchFetcher` is designed primarily around batching database lookups--
/// for example, fetching a user from a user ID, where a signle query to
/// retrieve 50 users by ID is significantly faster than 50 separate queries to
/// look up the same set of users.
///
/// A `BatchFetcher` is designed to be ephemeral. In the context of a web
/// service, this means callers should most likely create a new `BatchFetcher`
/// for each request, and **not** a `BatchFetcher` shared across multiple
/// requests. `BatchFetcher`s have no concept of cache invalidation, so old
/// values are stored indefinitely (which means callers may get stale data or
/// may exhaust memory endlessly).
///
/// `BatchFetcher`s introduce a small amount of latency for loads. Each time a
/// `BatchFetcher` receives a key to fetch that hasn't been cached (or a set of
/// keys), it will first wait for more keys to build a batch. The load will only
/// trigger after a timeout is reached or once enough keys have been queued in
/// the batch. See [`BatchFetcherBuilder`] for options to tweak latency and
/// batch sizes.
///
/// See also [`BatchExecutor`](crate::BatchExecutor) for a more general type
/// designed primarly for mutations, but can also be used for fetching with
/// more control over how batches are fetched.
///
/// ## Load semantics
///
/// If the underlying [`Fetcher`] returns an error during the batch request,
/// then all pending [`load`](BatchFetcher::load) and [`load_many`](BatchFetcher::load_many)
/// requests will fail. Subsequent calls to [`load`](BatchFetcher::load) or
/// [`load_many`](BatchFetcher::load_many) with the same keys **will retry**.
///
/// If the underlying [`Fetcher`] succeeds but does not return a value for a
/// given key during a batch request, then the `BatchFetcher` will mark that key
/// as "not found" and an error value of [`NotFound`](LoadError::NotFound) will
/// be returned to all pending [`load`](BatchFetcher::load) and
/// [`load_many`](BatchFetcher::load_many) requests. The "not found" status will
/// be preserved, so subsequent calls with the same key will fail and **will
/// not retry**.
pub struct BatchFetcher<F>
where
    F: Fetcher,
{
    label: Cow<'static, str>,
    cache_store: CacheStore<F::Key, F::Value>,
    _fetch_task: Arc<tokio::task::JoinHandle<()>>,
    fetch_request_tx: tokio::sync::mpsc::Sender<FetchRequest<F::Key>>,
}

impl<F> BatchFetcher<F>
where
    F: Fetcher + Send + Sync + 'static,
{
    /// Create a new `BatchFetcher` that uses the given [`Fetcher`] to retrieve
    /// data. Returns a [`BatchFetcherBuilder`], which can be used to customize
    /// the `BatchFetcher`. Call [`.finish()`](BatchFetcherBuilder::finish) to
    /// create the `BatchFetcher`.
    ///
    /// # Examples
    ///
    /// Creating a `BatchFetcher` with default options:
    ///
    /// ```
    /// # use ultra_batch::{BatchFetcher, Fetcher, Cache};
    /// # struct UserFetcher;
    /// # impl UserFetcher {
    /// #     fn new(db_conn: ()) -> Self { UserFetcher }
    /// #  }
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
    /// let batch_fetcher = BatchFetcher::build(user_fetcher).finish();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Creating a `BatchFetcher` with custom options:
    ///
    /// ```
    /// # use ultra_batch::{BatchFetcher, Fetcher, Cache};
    /// # struct UserFetcher;
    /// # impl UserFetcher {
    /// #     fn new(db_conn: ()) -> Self { UserFetcher }
    /// #  }
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
    /// let batch_fetcher = BatchFetcher::build(user_fetcher)
    ///     .eager_batch_size(Some(50))
    ///     .delay_duration(tokio::time::Duration::from_millis(5))
    ///     .finish();
    /// # Ok(()) }
    /// ```
    pub fn build(fetcher: F) -> BatchFetcherBuilder<F> {
        BatchFetcherBuilder {
            fetcher,
            delay_duration: tokio::time::Duration::from_millis(10),
            eager_batch_size: Some(100),
            label: "unlabeled-batch-fetcher".into(),
        }
    }

    /// Load the value with the associated key, either by calling the `Fetcher`
    /// or by loading the cached value. Returns an error if the value could
    /// not be loaded or if a value for the given key was not found.
    ///
    /// See the type-level docs for [`BatchFetcher`](#load-semantics) for more
    /// detailed loading semantics.
    #[tracing::instrument(skip_all, fields(batch_fetcher = %self.label))]
    pub async fn load(&self, key: F::Key) -> Result<F::Value, LoadError> {
        let mut values = self.load_keys(&[key]).await?;
        Ok(values.remove(0))
    }

    /// Load all the values for the given keys, either by calling the `Fetcher`
    /// or by loading cached values. Values are returned in the same order as
    /// the input keys. Returns an error if _any_ load fails.
    ///
    /// See the type-level docs for [`BatchFetcher`](#load-semantics) for more
    /// detailed loading semantics.
    #[tracing::instrument(skip_all, fields(batch_fetcher = %self.label, num_keys = keys.len()))]
    pub async fn load_many(&self, keys: &[F::Key]) -> Result<Vec<F::Value>, LoadError> {
        let values = self.load_keys(keys).await?;
        Ok(values)
    }

    async fn load_keys(&self, keys: &[F::Key]) -> Result<Vec<F::Value>, LoadError> {
        let mut cache_lookup = CacheLookup::new(keys.to_vec());

        match cache_lookup.lookup(&self.cache_store) {
            CacheLookupState::Done(result) => {
                tracing::debug!(batch_fetcher = %self.label, "all keys have already been looked up");
                return result;
            }
            CacheLookupState::Pending => {}
        }
        let pending_keys = cache_lookup.pending_keys();

        let fetch_request_tx = self.fetch_request_tx.clone();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        tracing::debug!(
            num_pending_keys = pending_keys.len(),
            batch_fetcher = %self.label,
            "sending a batch of keys to fetch",
        );
        let fetch_request = FetchRequest {
            keys: pending_keys,
            result_tx,
        };
        fetch_request_tx
            .send(fetch_request)
            .await
            .map_err(|_| LoadError::SendError)?;

        match result_rx.await {
            Ok(Ok(())) => {
                tracing::debug!(batch_fetcher = %self.label, "fetch response returned successfully");
            }
            Ok(Err(fetch_error)) => {
                tracing::info!("error returned while fetching keys: {fetch_error}");
                return Err(LoadError::FetchError(fetch_error));
            }
            Err(recv_error) => {
                panic!(
                    "Batch result channel for batch fetcher {} hung up with error: {recv_error}",
                    self.label,
                );
            }
        }

        match cache_lookup.lookup(&self.cache_store) {
            CacheLookupState::Done(result) => {
                tracing::debug!("all keys have now been looked up");
                result
            }
            CacheLookupState::Pending => {
                panic!(
                    "Batch result for batch fetcher {} is still pending after result channel was sent",
                    self.label,
                );
            }
        }
    }
}

impl<F> Clone for BatchFetcher<F>
where
    F: Fetcher,
{
    fn clone(&self) -> Self {
        BatchFetcher {
            cache_store: self.cache_store.clone(),
            _fetch_task: self._fetch_task.clone(),
            fetch_request_tx: self.fetch_request_tx.clone(),
            label: self.label.clone(),
        }
    }
}

/// Used to configure a new [`BatchFetcher`]. A `BatchFetcherBuilder` is
/// returned from [`BatchFetcher::build`].
pub struct BatchFetcherBuilder<F>
where
    F: Fetcher + Send + Sync + 'static,
{
    fetcher: F,
    delay_duration: tokio::time::Duration,
    eager_batch_size: Option<usize>,
    label: Cow<'static, str>,
}

impl<F> BatchFetcherBuilder<F>
where
    F: Fetcher + Send + Sync + 'static,
{
    /// The maximum amount of time the [`BatchFetcher`] will wait to queue up
    /// more keys before calling the [`Fetcher`].
    pub fn delay_duration(mut self, delay: tokio::time::Duration) -> Self {
        self.delay_duration = delay;
        self
    }

    /// The maximum number of keys to wait for before eagerly calling the
    /// [`Fetcher`]. A value of `Some(n)` will load the batch once `n` or more
    /// keys have been queued (or once the timeout set by
    /// [`delay_duration`](BatchFetcherBuilder::delay_duration) is reached,
    /// whichever comes first). A value of `None` will never eagerly dispatch
    /// the queue, and the [`BatchFetcher`] will always wait for the timeout set
    /// by [`delay_duration`](BatchFetcherBuilder::delay_duration).
    ///
    /// Note that `eager_batch_size` **does not** set an upper limit on the
    /// batch! For example, if [`BatchFetcher::load_many`] is called with more
    /// than `eager_batch_size` items, then the batch will be sent immediately
    /// with _all_ of the provided keys.
    pub fn eager_batch_size(mut self, eager_batch_size: Option<usize>) -> Self {
        self.eager_batch_size = eager_batch_size;
        self
    }

    /// Set a label for the [`BatchFetcher`]. This is only used to improve
    /// diagnostic messages, such as log messages.
    pub fn label(mut self, label: impl Into<Cow<'static, str>>) -> Self {
        self.label = label.into();
        self
    }

    /// Create and return a [`BatchFetcher`] with the given options.
    pub fn finish(self) -> BatchFetcher<F> {
        let cache_store = CacheStore::new();

        let (fetch_request_tx, mut fetch_request_rx) =
            tokio::sync::mpsc::channel::<FetchRequest<F::Key>>(1);
        let label = self.label.clone();

        let fetch_task = tokio::spawn({
            let cache_store = cache_store.clone();
            async move {
                'task: loop {
                    // Wait for some keys to come in
                    let mut pending_keys = HashSet::new();
                    let mut result_txs = vec![];

                    tracing::trace!(batch_fetcher = %self.label, "waiting for keys to fetch...");
                    match fetch_request_rx.recv().await {
                        Some(fetch_request) => {
                            tracing::trace!(batch_fetcher = %self.label, num_fetch_request_keys = fetch_request.keys.len(), "received initial fetch request");

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
                            tracing::trace!(
                                batch_fetcher = %self.label,
                                num_pending_keys = pending_keys.len(),
                                eager_batch_size = ?self.eager_batch_size,
                                "batch filled up, ready to fetch keys now",
                            );

                            break 'wait_for_more_keys;
                        }

                        let delay = tokio::time::sleep(self.delay_duration);
                        tokio::pin!(delay);

                        tokio::select! {
                            fetch_request = fetch_request_rx.recv() => {
                                match fetch_request {
                                    Some(fetch_request) => {
                                        tracing::trace!(batch_fetcher = %self.label, num_fetch_request_keys = fetch_request.keys.len(), "retrieved additional fetch request");

                                        for key in fetch_request.keys {
                                            pending_keys.insert(key);
                                        }
                                        result_txs.push(fetch_request.result_tx);
                                    }
                                    None => {
                                        // Fetch queue closed, so we're done waiting for keys
                                        tracing::debug!(batch_fetcher = %self.label, num_pending_keys = pending_keys.len(), "fetch channel closed");
                                        break 'wait_for_more_keys;
                                    }
                                }

                            }
                            _ = &mut delay => {
                                // Reached delay, so we're done waiting for keys
                                tracing::trace!(
                                    batch_fetcher = %self.label,
                                    num_pending_keys = pending_keys.len(),
                                    "delay reached while waiting for more keys to fetch"
                                );
                                break 'wait_for_more_keys;
                            }
                        };
                    }

                    let result = {
                        let mut cache = cache_store.as_cache();

                        tracing::trace!(batch_fetcher = %self.label, num_pending_keys = pending_keys.len(), num_pending_channels = result_txs.len(), "fetching keys");
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

        BatchFetcher {
            label,
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

/// Error indicating that loading one or more values from a [`BatchFetcher`]
/// failed.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// The [`Fetcher`] returned an error while loading the batch. The message
    /// contains the error message specified by [`Fetcher::Error`].
    #[error("error while fetching from batch: {}", _0)]
    FetchError(String),

    /// The request could not be sent to the [`BatchFetcher`].
    #[error("error sending fetch request")]
    SendError,

    /// The [`Fetcher`] did not return a value for one or more keys in the batch.
    #[error("value not found")]
    NotFound,
}
