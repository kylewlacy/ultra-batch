use std::collections::HashSet;
use std::sync::Arc;
use crate::{Cache, Fetcher};
use crate::cache::{CacheLookup, CacheLookupState};

pub struct Batcher<F>
where
    F: Fetcher,
{
    cache: Arc<Cache<F::Key, F::Value>>,
    _fetch_task: Arc<tokio::task::JoinHandle<()>>,
    fetch_request_tx: tokio::sync::mpsc::UnboundedSender<FetchRequest<F::Key>>,
}

impl<F> Batcher<F>
where
    F: Fetcher + Send + Sync + 'static,
{
    pub fn new(fetcher: F) -> Self {
        let cache = Arc::new(Cache::new());
        let delay_duration = tokio::time::Duration::from_millis(10);
        let eager_batch_size = 100;

        let (fetch_request_tx, mut fetch_request_rx) = tokio::sync::mpsc::unbounded_channel::<FetchRequest<F::Key>>();

        let fetch_task = tokio::spawn({
            let cache = cache.clone();
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
                        if pending_keys.len() > eager_batch_size {
                            // We have enough keys already, so don't wait for more
                            break 'wait_for_more_keys;
                        }

                        let mut delay = tokio::time::delay_for(delay_duration);
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

                    let pending_keys: Vec<_> = pending_keys.into_iter().collect();
                    let result = fetcher.fetch(&pending_keys, &cache).await
                        .map_err(|error| error.to_string());

                    if result.is_ok() {
                        cache.mark_keys_not_found(pending_keys);
                    }

                    for result_tx in result_txs {
                        // Ignore error if receiver was already closed
                        let _ = result_tx.send(result.clone());
                    }
                }
            }
        });

        Batcher {
            cache,
            _fetch_task: Arc::new(fetch_task),
            fetch_request_tx,
        }
    }

    pub async fn load(&self, key: F::Key) -> Result<F::Value, LoadError> {
        let mut values = self.load_many(&[key]).await?;
        Ok(values.remove(0))
    }

    pub async fn load_many(&self, keys: &[F::Key]) -> Result<Vec<F::Value>, LoadError> {
        let mut cache_lookup = CacheLookup::new(keys.to_vec());

        match cache_lookup.lookup(&self.cache) {
            CacheLookupState::Done(result) => { return result; }
            CacheLookupState::Pending => { }
        }
        let pending_keys = cache_lookup.pending_keys();

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let fetch_request = FetchRequest { keys: pending_keys, result_tx };
        self.fetch_request_tx.send(fetch_request).map_err(|_| LoadError::SendError)?;

        match result_rx.await {
            Ok(Ok(())) => { }
            Ok(Err(fetch_error)) => {
                return Err(LoadError::FetchError(fetch_error));
            }
            Err(recv_error) => {
                panic!("Batch result channel hung up with error: {}", recv_error);
            }
        }

        match cache_lookup.lookup(&self.cache) {
            CacheLookupState::Done(result) => { return result; }
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
            cache: self.cache.clone(),
            _fetch_task: self._fetch_task.clone(),
            fetch_request_tx: self.fetch_request_tx.clone(),
        }
    }
}

struct FetchRequest<K> {
    keys: Vec<K>,
    result_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("error while fetching from batch: {}", _0)]
    FetchError(String),

    #[error("error sending fetch request")]
    SendError,

    #[error("value not found")]
    NotFound,
}
