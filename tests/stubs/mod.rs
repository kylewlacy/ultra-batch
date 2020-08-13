use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{atomic, Arc, RwLock};
use ultra_batch::{Cache, Fetcher};

#[derive(Debug, Default, Clone)]
pub struct Counter {
    count: Arc<atomic::AtomicUsize>,
}

impl Counter {
    fn new() -> Self {
        Counter::default()
    }

    fn inc(&self) {
        self.count.fetch_add(1, atomic::Ordering::SeqCst);
    }

    fn count(&self) -> usize {
        self.count.load(atomic::Ordering::SeqCst)
    }
}

pub struct ObserveFetcher<F>
where
    F: Fetcher,
{
    fetcher: Arc<F>,
    total_calls: Counter,
    calls_per_key: Arc<RwLock<HashMap<F::Key, Counter>>>,
}

impl<F> ObserveFetcher<F>
where
    F: Fetcher,
{
    pub fn new(fetcher: F) -> Self {
        ObserveFetcher {
            fetcher: Arc::new(fetcher),
            total_calls: Counter::new(),
            calls_per_key: Default::default(),
        }
    }

    pub fn total_calls(&self) -> usize {
        self.total_calls.count()
    }

    pub fn calls_for_key(&self, key: &F::Key) -> usize {
        let calls_per_key = self.calls_per_key.read().unwrap();
        calls_per_key
            .get(key)
            .map(|count| count.count())
            .unwrap_or_default()
    }
}

impl<F> Clone for ObserveFetcher<F>
where
    F: Fetcher,
{
    fn clone(&self) -> Self {
        ObserveFetcher {
            fetcher: self.fetcher.clone(),
            total_calls: self.total_calls.clone(),
            calls_per_key: self.calls_per_key.clone(),
        }
    }
}

#[async_trait]
impl<F> Fetcher for ObserveFetcher<F>
where
    F: Fetcher + Send + Sync,
{
    type Key = F::Key;
    type Value = F::Value;
    type Error = F::Error;

    async fn fetch(
        &self,
        keys: &[Self::Key],
        values: &Cache<Self::Key, Self::Value>,
    ) -> Result<(), Self::Error> {
        {
            self.total_calls.inc();
            let mut calls_per_key = self.calls_per_key.write().unwrap();
            for key in keys {
                calls_per_key.entry(key.clone()).or_default().inc();
            }
        }

        self.fetcher.fetch(keys, values).await
    }
}
