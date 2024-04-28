use crate::Cache;
use std::fmt::Display;
use std::future::Future;
use std::hash::Hash;

/// A trait for fetching values from some datastore in bulk. A `Fetcher`
/// will be given an array of keys and should insert fetched values into
/// a given cache. Implementing `Fetcher` will allow queries to be batch
/// using a [`Batcher`](crate::Batcher). See the [`Batcher`](crate::Batcher)
/// docs for details about batching, caching, and error semantics.
///
/// Implementors should use [`async-trait`](https://crates.io/crates/async-trait)
/// to implement this trait.
///
/// # Examples
///
/// ```
/// # use ultra_batch::{Fetcher, Cache};
/// # #[derive(Clone, Copy, Hash, PartialEq, Eq)] struct UserId(usize);
/// # #[derive(Clone)] struct User { id: UserId }
/// # struct DbConnection(std::sync::Arc<Vec<User>>);
/// # impl DbConnection {
/// #     async fn get_users_by_ids(&self, user_ids: &[UserId]) -> anyhow::Result<Vec<User>> {
/// #         let users = user_ids.iter().flat_map(|id| self.0.iter().nth(id.0).cloned());
/// #         Ok(users.collect())
/// #     }
/// # }
/// struct UserFetcher {
///     db_conn: DbConnection,
/// }
///
/// impl Fetcher for UserFetcher {
///     type Key = UserId;
///     type Value = User;
///     type Error = anyhow::Error;
///
///     async fn fetch(&self, keys: &[UserId], values: &mut Cache<'_, UserId, User>) -> anyhow::Result<()> {
///         let users = self.db_conn.get_users_by_ids(keys).await?;
///         for user in users {
///             values.insert(user.id, user);
///         }
///         Ok(())
///     }
/// }
/// ```
pub trait Fetcher {
    /// The type used to look up a single value in a batch.
    type Key: Clone + Hash + Eq + Send + Sync;

    /// The type returned in a batch. `Value` is usually a single database
    /// record, but could also be a more sophisticated type, such as a
    /// `Vec` of values for a `Fetcher` that deals with one-to-many
    /// relationships.
    type Value: Clone + Send + Sync;

    /// The error indicating that fetching a batch failed.
    type Error: Display + Send + Sync + 'static;

    /// Retrieve the values associated with the given keys, and insert them into
    /// `values` if found. If `Ok(_)` is returned, then any keys not inserted
    /// into `values` will be marked as "not found" (meaning any future attempts
    /// to retrieve them will fail). If `Err(_)` is returned, then the caller(s)
    /// waiting on the batch will receive a [`LoadError::FetchError`](crate::LoadError::FetchError)
    /// with the message from returned error (note that any values inserted into
    /// `values` before the `Err(_)` is returned will still be cached). See the
    /// [`Batcher`](crate::Batcher) docs for more details.
    fn fetch(
        &self,
        keys: &[Self::Key],
        values: &mut Cache<'_, Self::Key, Self::Value>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
