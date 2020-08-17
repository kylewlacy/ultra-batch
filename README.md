# `ultra-batch`

[![Crate](https://img.shields.io/crates/v/ultra-batch)](https://crates.io/crates/ultra-batch)
[![Docs](https://docs.rs/ultra-batch/badge.svg)](https://docs.rs/ultra-batch)

`ultra-batch` is a Rust library to batch and cache database queries or other potentially expensive data lookups. The main motivation for this library is to solve the ["N + 1" query problem](https://stackoverflow.com/q/97197) seen in GraphQL and elsewhere. This library takes heavy influence from the GraphQL Foundation's [DataLoader](https://github.com/graphql/dataloader). It's designed primarily for dealing with database queries, but it can be used to batch any potentially expensive data loading operations.

## Example Use

First, add [`tokio`](https://crates.io/crates/tokio), [`async-trait`](https://crates.io/crates/async-trait), and [`anyhow`](https://crates.io/crates/anyhow) (optional) as dependencies.

```rust
use async_trait::async_trait;
use ultra_batch::{Fetcher, Batcher, Cache};

#[derive(Debug, Clone)]
struct User {
    id: u64,
    // User model from your DB, etc.
}

struct UserFetcher {
    // Database connection, etc.
}

#[async_trait]
impl Fetcher for UserFetcher {
    // The thing we can use to look up a single `User` (like an ID)
    type Key = u64;

    // The thing we are trying to look up
    type Value = User;

    // Used to indicate the batch request failed (DB connection failed, etc)
    type Error = anyhow::Error;

    // Fetch a batch of users
    async fn fetch(
        &self,
        keys: &[Self::Key],
        values: &mut Cache<'_, Self::Key, Self::Value>,
    ) -> Result<(), Self::Error> {
        let users: Vec<User> = todo!(); // Fetch users based on the given keys
        for user in users {
            values.insert(user.id, user); // Insert all users we found
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let fetcher = UserFetcher { /* ... */ };
    let batcher = Batcher::new(fetcher).build();

    // Retrieve a user by ID. If `load` gets called in other tasks/threads
    // at the same time, then all the requested IDs will get batched together
    let user = batcher.load(123).await?;

    println!("User: {:?}", user);

    Ok(())
}
```

## Alternative projects

- [`dataloader-rs`](https://github.com/cksac/dataloader-rs)
- [`batchloader`](https://github.com/Lucretiel/batchloader)

## License

Licensed under either the [MIT license](LICENSE-MIT) or [Apache 2.0 license](LICENSE-APACHE) (licensee's choice).
