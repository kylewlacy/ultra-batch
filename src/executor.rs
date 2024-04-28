use std::fmt::Display;
use std::future::Future;

/// A trait for using a batch of values to execute some operation, such
/// as a bulk insertion in a datastore. An `Executor` will be given an
/// array of values and should handle each value, then return a result for
/// each. Implementing `Executor` will allow operations to be batched by
/// using a [`BatchExecutor`](crate::BatchExecutor). See the [`BatchExecutor`](crate::BatchExecutor)
/// docs for details about batching and error semantics.
pub trait Executor {
    /// The input value provided by the caller to do something.
    type Value: Clone + Send + Sync;

    /// The output value returned by the executor back to the caller for each
    /// input value.
    type Result: Clone + Send + Sync;

    /// The error indicating that executing a batch failed.
    type Error: Display + Send + Sync + 'static;

    /// Execute the operation for each value in the batch, returning a result
    /// for each value. If `Ok(_)` is returned, a `Vec` should be returned,
    /// where each element corresponds to the result of the input value at
    /// the same index. If no element is present for a given input value,
    /// then the caller will not receive a value. If `Err(_)` is returned,
    /// then the caller waiting on the batch will receive an [`ExecuteError::ExecutorError`](crate::ExecuteError::ExecutorError).
    fn execute(
        &self,
        values: Vec<Self::Value>,
    ) -> impl Future<Output = Result<Vec<Self::Result>, Self::Error>> + Send;
}
