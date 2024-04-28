//! Batch and cache database queries, mutations, or other potentially expensive
//! operations. The main motivation for this library is to solve the "N + 1"
//! query problem seen in GraphQL and elsewhere. This library takes heavy
//! influence from the GraphQL Foundation's [DataLoader](https://github.com/graphql/dataloader).
//!
//! For batched data queries, see the [`BatchFetcher`] type (used to queue and
//! load data in batches) and the [`Fetcher`] trait (used by [`BatchFetcher`]s
//! to actually retrieve the data). For other operations including mutations
//! or more advanced query operations, see the [`BatchExecutor`] type and
//! the [`Executor`] trait.

pub(crate) mod batch_executor;
pub(crate) mod batch_fetcher;
pub(crate) mod cache;
pub(crate) mod executor;
pub(crate) mod fetcher;

pub use batch_executor::{BatchExecutor, BatchExecutorBuilder, ExecuteError};
pub use batch_fetcher::{BatchFetcher, BatchFetcherBuilder, LoadError};
pub use cache::Cache;
pub use executor::Executor;
pub use fetcher::Fetcher;
