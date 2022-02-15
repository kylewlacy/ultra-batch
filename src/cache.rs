use crate::LoadError;
use chashmap::CHashMap;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

/// Holds the results of loading a batch of data from a [`Fetcher`](crate::Fetcher).
/// Implementors of [`Fetcher`](crate::Fetcher) should call [`insert`](Cache::insert)
/// for each value that was loaded in a batch request.
pub struct Cache<'a, K, V> {
    map_ref: &'a CHashMap<K, CacheState<V>>,
}

impl<'a, K, V> Cache<'a, K, V>
where
    K: Clone + Hash + Eq,
    V: Clone,
{
    /// Insert a value into the cache for the given key.
    pub fn insert(&mut self, key: K, value: V) {
        self.map_ref.insert(key, CacheState::Loaded(value));
    }

    pub(crate) fn mark_keys_not_found(&mut self, keys: Vec<K>) {
        for key in keys {
            self.map_ref
                .alter(key, |value| Some(value.unwrap_or(CacheState::NotFound)));
        }
    }
}

#[derive(Clone)]
pub(crate) struct CacheStore<K, V> {
    map: Arc<CHashMap<K, CacheState<V>>>,
}

impl<K, V> CacheStore<K, V> {
    pub(crate) fn new() -> Self {
        let map = Arc::new(CHashMap::new());
        CacheStore { map }
    }

    pub(crate) fn as_cache(&'_ self) -> Cache<'_, K, V> {
        let map_ref = &*self.map;
        Cache { map_ref }
    }
}

#[derive(Clone)]
enum CacheState<V> {
    Loaded(V),
    NotFound,
}

pub(crate) struct CacheLookup<K, V>
where
    K: Hash + Eq,
{
    keys: Vec<K>,
    entries: HashMap<K, Option<CacheState<V>>>,
}

impl<K, V> CacheLookup<K, V>
where
    K: Clone + Hash + Eq,
    V: Clone,
{
    pub(crate) fn new(keys: Vec<K>) -> Self {
        let entries = keys.iter().map(|key| (key.clone(), None)).collect();
        CacheLookup { keys, entries }
    }

    pub(crate) fn reload_keys_from_cache_store(&mut self, cache_store: &CacheStore<K, V>) {
        let keys: Vec<K> = self.entries.keys().into_iter().cloned().collect();
        for key in keys {
            self.entries
                .entry(key.clone())
                .and_modify(|mut load_state| match load_state {
                    Some(_) => {}
                    ref mut load_state @ None => {
                        **load_state = cache_store.map.get(&key).as_deref().cloned();
                    }
                });
        }
    }

    pub(crate) fn pending_keys(&self) -> Vec<K> {
        self.entries
            .iter()
            .filter_map(|(key, value)| match value {
                None => Some(key.clone()),
                Some(_) => None,
            })
            .collect()
    }

    pub(crate) fn lookup_result(&self) -> Result<Vec<V>, LoadError> {
        self.keys
            .iter()
            .map(|key| {
                let load_state = self
                    .entries
                    .get(key)
                    .expect("Cache lookup is missing an expected key");
                match load_state {
                    Some(CacheState::Loaded(value)) => Ok(value.clone()),
                    Some(CacheState::NotFound) | None => Err(LoadError::NotFound),
                }
            })
            .collect()
    }

    pub(crate) fn lookup(&mut self, cache_store: &CacheStore<K, V>) -> CacheLookupState<V> {
        self.reload_keys_from_cache_store(cache_store);
        let pending_keys = self.pending_keys();

        if pending_keys.is_empty() {
            CacheLookupState::Done(self.lookup_result())
        } else {
            CacheLookupState::Pending
        }
    }
}

pub(crate) enum CacheLookupState<V> {
    Done(Result<Vec<V>, LoadError>),
    Pending,
}
