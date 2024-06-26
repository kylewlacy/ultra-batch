use crate::Executor;
use std::{borrow::Cow, sync::Arc};

/// Batches calls to an [`Executor`], such as for bulk inserting, updating,
/// or deleting records in a datastore. `BatchExecutor`s are asynchronous
/// and designed to be passed and shared between threads or tasks. Cloning
/// a `BatchExecutor` is shallow and will use the same underlying [`Executor`].
///
/// `BatchExecutor` is designed primarily for bulk database operations-- for
/// example, inserting lots of records, where a single query to insert
/// 50 new records is much faster than 50 separate queries. However, it can
/// be used for fetching data or any other bulk operation as well.
///
/// Unlike [`BatchFetcher`](crate::BatchFetcher), `BatchExecutor` has no
/// concepts of keys, values, deduplication, or caching; each executed value
/// is passed directly to the underlying [`Executor`]. As such, it could also
/// be suitable for writing a custom caching layer in situations where
/// [`BatchFetcher`](crate::BatchFetcher) is not suitable.
///
/// `BatchExecutor`s introduce a small amount of latency for executions. Each
/// time a new value or set of values is sent for execution, it will first
/// wait for more values to buid a batch. The execution will only trigger after
/// a timeout is reached or once enough values have been queued in the batch.
/// See [`BatchExecutorBuilder`] for options to tweak latency and batch sizes.
///
/// ## Execution semantics
///
/// If the underlying [`Executor`] returns an error during the batch execution,
/// then all pending [`execute`](BatchExecutor::execute) and [`execute_many`](BatchExecutor::execute_many)
/// requests will fail. The same values can be resubmitted to retry.
///
/// If the underlying [`Executor`] succeeds but does not return a `Vec` that
/// contains results for all values, then calls to [`execute`](BatchExecutor::execute)
/// may return `None`. Calls to [`execute_many`](BatchExecutor::execute_many)
/// may return a `Vec` containing less output values than input values.
pub struct BatchExecutor<E>
where
    E: Executor,
{
    label: Cow<'static, str>,
    _execute_task: Arc<tokio::task::JoinHandle<()>>,
    execute_request_tx: tokio::sync::mpsc::Sender<ExecuteRequest<E::Value, E::Result>>,
}

impl<E> BatchExecutor<E>
where
    E: Executor + Send + Sync + 'static,
{
    /// Create a new `BatchExecutor` athat uses the given [`Executor`] to
    /// execute values. Returns a [`BatchExecutorBuilder`], which can be
    /// used to customize the `BatchExecutor`. Call [`.finish()`](BatchExecutorBuilder::finish)
    /// to create the `BatchExecutor`.
    ///
    /// # Examples
    ///
    /// Creating a `BatchExecutor` with default options:
    ///
    /// ```
    /// # use ultra_batch::{BatchExecutor, Executor};
    /// # struct UserInserter;
    /// # impl UserInserter {
    /// #     fn new(db_conn: ()) -> Self { UserInserter }
    /// #  }
    /// # impl Executor for UserInserter {
    /// #     type Value = ();
    /// #     type Result = ();
    /// #     type Error = anyhow::Error;
    /// #     async fn execute(&self, values: Vec<()>) -> anyhow::Result<Vec<()>> {
    /// #         unimplemented!();
    /// #     }
    /// # }
    /// # #[tokio::main] async fn main() -> anyhow::Result<()> {
    /// # let db_conn = ();
    /// let user_inserter = UserInserter::new(db_conn);
    /// let batch_inserter = BatchExecutor::build(user_inserter).finish();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Creating a `BatchExecutor` with custom options:
    ///
    /// ```
    /// # use ultra_batch::{BatchExecutor, Executor};
    /// # struct UserInserter;
    /// # impl UserInserter {
    /// #     fn new(db_conn: ()) -> Self { UserInserter }
    /// #  }
    /// # impl Executor for UserInserter {
    /// #     type Value = ();
    /// #     type Result = ();
    /// #     type Error = anyhow::Error;
    /// #     async fn execute(&self, values: Vec<()>) -> anyhow::Result<Vec<()>> {
    /// #         unimplemented!();
    /// #     }
    /// # }
    /// # #[tokio::main] async fn main() -> anyhow::Result<()> {
    /// # let db_conn = ();
    /// let user_inserter = UserInserter::new(db_conn);
    /// let batch_inserter = BatchExecutor::build(user_inserter)
    ///     .eager_batch_size(Some(50))
    ///     .delay_duration(tokio::time::Duration::from_millis(5))
    ///     .finish();
    /// # Ok(())
    /// # }
    /// ```
    pub fn build(executor: E) -> BatchExecutorBuilder<E> {
        BatchExecutorBuilder {
            executor,
            delay_duration: tokio::time::Duration::from_millis(10),
            eager_batch_size: Some(100),
            label: "unlabeled-batch-executor".into(),
        }
    }

    /// Submit a value to be executed by the [`Executor`]. Returns the
    /// result value returned by the [`Executor`] for this given item. See
    /// the type-level docs for [`BatchExecutor`](#execution-semantics) for
    /// detailed execution semantics.
    #[tracing::instrument(skip_all, fields(batch_executor = %self.label))]
    pub async fn execute(&self, key: E::Value) -> Result<Option<E::Result>, ExecuteError> {
        let mut values = self.execute_values(vec![key]).await?;
        Ok(values.pop())
    }

    /// Submit multiple values to be executed by the [`Executor`]. Returns a
    /// `Vec` containg values for each result returned by the [`Executor`]
    /// for each given input value (but note that the returned `Vec` may
    /// not have values for all inputs if the [`Executor`] did not return
    /// enough results). See the type-level docs for [`BatchExecutor`](#execution-semantics)
    /// for detailed execution semantics.
    #[tracing::instrument(skip_all, fields(batch_executor = %self.label, num_values = values.len()))]
    pub async fn execute_many(
        &self,
        values: Vec<E::Value>,
    ) -> Result<Vec<E::Result>, ExecuteError> {
        let results = self.execute_values(values).await?;
        Ok(results)
    }

    async fn execute_values(&self, values: Vec<E::Value>) -> Result<Vec<E::Result>, ExecuteError> {
        let execute_request_tx = self.execute_request_tx.clone();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        tracing::debug!(
            batch_executor = %self.label,
            "sending a batch of values to execute",
        );
        let execute_request = ExecuteRequest { values, result_tx };
        execute_request_tx
            .send(execute_request)
            .await
            .map_err(|_| ExecuteError::SendError)?;

        match result_rx.await {
            Ok(Ok(results)) => {
                tracing::debug!(batch_executor = %self.label, "fetch response returned successfully");
                Ok(results)
            }
            Ok(Err(execute_error)) => {
                tracing::info!("error returned while executing: {execute_error}");
                Err(ExecuteError::ExecutorError(execute_error))
            }
            Err(recv_error) => {
                panic!(
                    "Batch result channel for batch executor {} hung up with error: {recv_error}",
                    self.label,
                );
            }
        }
    }
}

impl<E> Clone for BatchExecutor<E>
where
    E: Executor,
{
    fn clone(&self) -> Self {
        BatchExecutor {
            _execute_task: self._execute_task.clone(),
            execute_request_tx: self.execute_request_tx.clone(),
            label: self.label.clone(),
        }
    }
}

/// Used to configure a new [`BatchExecutor`]. A `BatchExecutorBuilder` is
/// returned from [`BatchExecutor::build`].
pub struct BatchExecutorBuilder<E>
where
    E: Executor + Send + Sync + 'static,
{
    executor: E,
    delay_duration: tokio::time::Duration,
    eager_batch_size: Option<usize>,
    label: Cow<'static, str>,
}

impl<E> BatchExecutorBuilder<E>
where
    E: Executor + Send + Sync + 'static,
{
    /// The maximum amount of time the [`BatchExecutor`] will wait to queue up
    /// more keys before calling the [`Executor`].
    pub fn delay_duration(mut self, delay: tokio::time::Duration) -> Self {
        self.delay_duration = delay;
        self
    }

    /// The maximum number of keys to wait for before eagerly calling the
    /// [`Executor`]. A value of `Some(n)` will load the batch once `n` or more
    /// keys have been queued (or once the timeout set by
    /// [`delay_duration`](BatchExecutorBuilder::delay_duration) is reached,
    /// whichever comes first). A value of `None` will never eagerly dispatch
    /// the queue, and the [`BatchExecutor`] will always wait for the timeout
    /// set by [`delay_duration`](BatchExecutorBuilder::delay_duration).
    ///
    /// Note that `eager_batch_size` **does not** set an upper limit on the
    /// batch! For example, if [`BatchExecutor::execute_many`] is called with
    /// more than `eager_batch_size` items, then the batch will be sent
    /// immediately with _all_ of the provided values.
    pub fn eager_batch_size(mut self, eager_batch_size: Option<usize>) -> Self {
        self.eager_batch_size = eager_batch_size;
        self
    }

    /// Set a label for the [`BatchExecutor`]. This is only used to improve
    /// diagnostic messages, such as log messages.
    pub fn label(mut self, label: impl Into<Cow<'static, str>>) -> Self {
        self.label = label.into();
        self
    }

    /// Create and return a [`BatchExecutor`] with the given options.
    pub fn finish(self) -> BatchExecutor<E> {
        let (execute_request_tx, mut execute_request_rx) =
            tokio::sync::mpsc::channel::<ExecuteRequest<E::Value, E::Result>>(1);
        let label = self.label.clone();

        let execute_task = tokio::spawn({
            async move {
                'task: loop {
                    // Wait for some values to come in
                    let mut pending_values = vec![];
                    let mut result_txs = vec![];

                    tracing::trace!(batch_executor = %self.label, "waiting for values to execute...");
                    match execute_request_rx.recv().await {
                        Some(execute_request) => {
                            tracing::trace!(batch_executor = %self.label, num_execute_request_values = execute_request.values.len(), "received initial execute request");

                            let result_start_index = pending_values.len();
                            pending_values.extend(execute_request.values);

                            result_txs.push((result_start_index, execute_request.result_tx));
                        }
                        None => {
                            // Execute queue closed, so we're done
                            break 'task;
                        }
                    };

                    // Wait for more values
                    'wait_for_more_values: loop {
                        let should_run_batch_now = match self.eager_batch_size {
                            Some(eager_batch_size) => pending_values.len() >= eager_batch_size,
                            None => false,
                        };
                        if should_run_batch_now {
                            // We have enough values already, so don't wait for more
                            tracing::trace!(
                                batch_executor = %self.label,
                                num_pending_values = pending_values.len(),
                                eager_batch_size = ?self.eager_batch_size,
                                "batch filled up, ready to execute now",
                            );

                            break 'wait_for_more_values;
                        }

                        let delay = tokio::time::sleep(self.delay_duration);
                        tokio::pin!(delay);

                        tokio::select! {
                            execute_request = execute_request_rx.recv() => {
                                match execute_request {
                                    Some(execute_request) => {
                                        tracing::trace!(batch_executor = %self.label, num_execute_request_values = execute_request.values.len(), "retrieved additional execute request");


                                        let result_start_index = pending_values.len();
                                        pending_values.extend(execute_request.values);

                                        result_txs.push((result_start_index, execute_request.result_tx));
                                    }
                                    None => {
                                        // Executor queue closed, so we're done waiting for keys
                                        tracing::debug!(batch_executor = %self.label, num_pending_values = pending_values.len(), "execute channel closed");
                                        break 'wait_for_more_values;
                                    }
                                }

                            }
                            _ = &mut delay => {
                                // Reached delay, so we're done waiting for keys
                                tracing::trace!(
                                    batch_executor = %self.label,
                                    num_pending_values = pending_values.len(),
                                    "delay reached while waiting for more values to fetch"
                                );
                                break 'wait_for_more_values;
                            }
                        };
                    }

                    tracing::trace!(batch_executor = %self.label, num_pending_values = pending_values.len(), num_pending_channels = result_txs.len(), "fetching values");
                    let mut result = self
                        .executor
                        .execute(pending_values)
                        .await
                        .map_err(|error| error.to_string());

                    for (result_range, result_tx) in result_txs.into_iter().rev() {
                        let result = match &mut result {
                            Ok(result) => {
                                if result_range <= result.len() {
                                    Ok(result.split_off(result_range))
                                } else {
                                    Ok(vec![])
                                }
                            }
                            Err(error) => Err(error.clone()),
                        };

                        // Ignore error if receiver was already closed
                        let _ = result_tx.send(result);
                    }
                }
            }
        });

        BatchExecutor {
            label,
            _execute_task: Arc::new(execute_task),
            execute_request_tx,
        }
    }
}

struct ExecuteRequest<V, R> {
    values: Vec<V>,
    result_tx: tokio::sync::oneshot::Sender<Result<Vec<R>, String>>,
}

/// Error indicating that execution of one or more values from a
/// [`BatchExecutor`] failed.
#[derive(Debug, thiserror::Error)]
pub enum ExecuteError {
    /// The [`Executor`] returned an error while loading the batch. The message
    /// contains the error message specified by [`Executor::Error`].
    #[error("error while executing batch: {}", _0)]
    ExecutorError(String),

    /// The request could not be sent to the [`BatchExecutor`].
    #[error("error sending execution request")]
    SendError,
}
