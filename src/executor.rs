use std::fmt::Display;
use std::future::Future;

pub trait Executor {
    type Value: Clone + Send + Sync;

    type Result: Clone + Send + Sync;

    type Error: Display + Send + Sync + 'static;

    fn execute(
        &self,
        values: Vec<Self::Value>,
    ) -> impl Future<Output = Result<Vec<Self::Result>, Self::Error>> + Send;
}
