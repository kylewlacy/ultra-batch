use async_trait::async_trait;
use std::hash::Hash;
use std::fmt::Display;
use crate::Cache;

#[async_trait]
pub trait Fetcher {
    type Key: Clone + Hash + Eq + Send + Sync;
    type Value: Clone + Send + Sync;
    type Error: Display + Send + Sync + 'static;

    async fn fetch(&self, keys: &[Self::Key], values: &Cache<Self::Key, Self::Value>) -> Result<(), Self::Error>;
}
