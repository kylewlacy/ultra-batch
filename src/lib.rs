use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::hash::Hash;
use std::fmt::Display;
use tokio::stream::StreamExt;

pub struct Batcher<F>
where
    F: Fetcher,
{
    cache: Arc<Cache<F::Key, F::Value>>,
    _fetch_task: Arc<tokio::task::JoinHandle<()>>,
    fetch_queue_tx: tokio::sync::mpsc::UnboundedSender<Vec<F::Key>>,
    fetch_result_rx: tokio::sync::watch::Receiver<Result<(), String>>,
}

impl<F> Batcher<F>
where
    F: Fetcher + Send + Sync + 'static,
{
    pub fn new(fetcher: F) -> Self {
        let cache = Arc::new(Cache::new());
        let delay_duration = tokio::time::Duration::from_millis(10);
        let eager_batch_size = 100;

        let (fetch_result_tx, fetch_result_rx) = tokio::sync::watch::channel::<Result<(), String>>(Ok(()));
        let (fetch_queue_tx, mut fetch_queue_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<F::Key>>();

        let fetch_task = tokio::spawn({
            let cache = cache.clone();
            async move {
                'task: loop {
                    // Wait for some keys to come in
                    let mut pending_keys = HashSet::new();
                    match fetch_queue_rx.recv().await {
                        Some(new_keys) => {
                            for key in new_keys {
                                pending_keys.insert(key);
                            }
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
                            new_keys = fetch_queue_rx.recv() => {
                                match new_keys {
                                    Some(new_keys) => {
                                        for key in new_keys {
                                            pending_keys.insert(key);
                                        }
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
                    let result = fetcher.fetch(&pending_keys, &cache).await;
                    fetch_result_tx.broadcast(result.map_err(|error| error.to_string()))
                        .expect("Error broadcasting fetch result");
                }
            }
        });

        Batcher {
            cache,
            _fetch_task: Arc::new(fetch_task),
            fetch_queue_tx,
            fetch_result_rx,
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
            CacheLookupState::Pending { .. } => { }
        }
        let pending_keys = cache_lookup.pending_keys();

        let mut fetch_result_tx = self.fetch_result_rx.clone();
        let cache = self.cache.clone();
        let retrieve_values_task = tokio::spawn(async move {
            let mut cache_lookup = cache_lookup;
            loop {
                match fetch_result_tx.next().await {
                    None => {
                        // Result channel closed, we won't be gettting our value
                        return Err(LoadError::NotFound);
                    }
                    Some(Err(fetch_error)) => {
                        return Err(LoadError::FetchError(fetch_error));
                    }
                    Some(Ok(())) => {
                        // A fetch completed successfully, so check if we have all our keys
                    }
                }

                match cache_lookup.lookup(&cache) {
                    CacheLookupState::Done(result) => { return result; }
                    CacheLookupState::Pending { .. } => { }
                }
            }
        });

        self.fetch_queue_tx.send(pending_keys).map_err(|_| LoadError::SendError)?;

        let retrieved_value_result = retrieve_values_task.await
            .map_err(LoadError::TaskError)?;
        retrieved_value_result
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
            fetch_queue_tx: self.fetch_queue_tx.clone(),
            fetch_result_rx: self.fetch_result_rx.clone(),
        }
    }
}

#[async_trait]
pub trait Fetcher {
    type Key: Clone + Hash + Eq + Send + Sync;
    type Value: Clone + Send + Sync;
    type Error: Display + Send + Sync + 'static;

    async fn fetch(&self, keys: &[Self::Key], values: &Cache<Self::Key, Self::Value>) -> Result<(), Self::Error>;
}

pub struct Cache<K, V> {
    map: cht::HashMap<K, V>,
}

impl<K, V> Cache<K, V>
where
    K: Hash + Eq,
{
    fn new() -> Self {
        Cache {
            map: cht::HashMap::new(),
        }
    }

    pub fn insert(&self, key: K, value: V) {
        self.map.insert_and(key, value, |_| {});
    }
}

struct CacheLookup<K, V>
where
    K: Hash + Eq,
{
    keys: Vec<K>,
    entries: HashMap<K, Option<V>>,
}

impl<K, V> CacheLookup<K, V>
where
    K: Clone + Hash + Eq,
    V: Clone,
{
    fn new(keys: Vec<K>) -> Self {
        let entries = keys.iter().map(|key| (key.clone(), None)).collect();
        CacheLookup {
            keys,
            entries,
        }
    }

    fn reload_keys_from_cache(&mut self, cache: &Cache<K, V>) {
        let keys: Vec<K> = self.entries.keys().into_iter().cloned().collect();
        for key in keys {
            self.entries.entry(key.clone()).and_modify(|mut load_state| {
                match load_state {
                    Some(_) => {

                    }
                    ref mut load_state @ None => {
                        **load_state = cache.map.get(&key);
                    }
                }
            });
        }
    }

    fn pending_keys(&self) -> Vec<K> {
        self.entries
            .iter()
            .filter_map(|(key, value)| {
                match value {
                    None => Some(key.clone()),
                    Some(_) => None,
                }
            })
            .collect()
    }

    fn lookup_result(&self) -> Result<Vec<V>, LoadError> {
        self.keys
            .iter()
            .map(|key| {
                let load_state = self.entries.get(key)
                    .expect("Cache lookup is missing an expected key");
                match load_state {
                    Some(value) => Ok(value.clone()),
                    None => Err(LoadError::NotFound),
                }
            })
            .collect()
    }

    fn lookup(&mut self, cache: &Cache<K, V>) -> CacheLookupState<V> {
        self.reload_keys_from_cache(cache);
        let pending_keys = self.pending_keys();

        if pending_keys.is_empty() {
            CacheLookupState::Done(self.lookup_result())
        }
        else {
            CacheLookupState::Pending
        }
    }
}

enum CacheLookupState<V> {
    Done(Result<Vec<V>, LoadError>),
    Pending,
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("error while fetching from batch: {}", _0)]
    FetchError(String),

    #[error("error sending fetch request")]
    SendError,

    #[error("error during retrieval task")]
    TaskError(tokio::task::JoinError),

    #[error("value not found")]
    NotFound,
}
