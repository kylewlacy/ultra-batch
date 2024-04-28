//! Batch and cache database queries or other potentially expensive data
//! lookups. The main motivation for this library is to solve the
//! "N + 1" query problem seen in GraphQL and elsewhere. This library takes
//! heavy influence from the GraphQL Foundation's [DataLoader](https://github.com/graphql/dataloader).
//!
//! The most common entrypoints to this library are the [`BatchFetcher`] type
//! (used to queue and load data in batches) and the [`Fetcher`] trait (used by
//! [`BatchFetcher`]s to actually retrieve the data).

pub(crate) mod batch_fetcher;
pub(crate) mod cache;
pub(crate) mod fetcher;

pub use batch_fetcher::{BatchFetcher, BatchFetcherBuilder, LoadError};
pub use cache::Cache;
pub use fetcher::Fetcher;
