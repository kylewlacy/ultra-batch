use std::collections::HashMap;
use std::hash::Hash;
use crate::LoadError;

pub struct Cache<K, V> {
    map: cht::HashMap<K, CacheState<V>>,
}

impl<K, V> Cache<K, V>
where
    K: Clone + Hash + Eq,
    V: Clone,
{
    pub(crate) fn new() -> Self {
        Cache {
            map: cht::HashMap::new(),
        }
    }

    pub fn insert(&self, key: K, value: V) {
        self.map.insert_and(key, CacheState::Loaded(value), |_| {});
    }

    pub(crate) fn mark_keys_not_found(&self, keys: Vec<K>) {
        for key in keys {
            // Insert `CacheState::NotFound` if the cache doesn't already contain the key
            self.map.insert_or_modify(key, CacheState::NotFound, |_key, current_value| current_value.clone());
        }
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
        CacheLookup {
            keys,
            entries,
        }
    }

    pub(crate) fn reload_keys_from_cache(&mut self, cache: &Cache<K, V>) {
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

    pub(crate) fn pending_keys(&self) -> Vec<K> {
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

    pub(crate) fn lookup_result(&self) -> Result<Vec<V>, LoadError> {
        self.keys
            .iter()
            .map(|key| {
                let load_state = self.entries.get(key)
                    .expect("Cache lookup is missing an expected key");
                match load_state {
                    Some(CacheState::Loaded(value)) => Ok(value.clone()),
                    Some(CacheState::NotFound) | None => Err(LoadError::NotFound),
                }
            })
            .collect()
    }

    pub(crate) fn lookup(&mut self, cache: &Cache<K, V>) -> CacheLookupState<V> {
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

pub(crate) enum CacheLookupState<V> {
    Done(Result<Vec<V>, LoadError>),
    Pending,
}
