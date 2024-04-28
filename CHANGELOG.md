# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]
### Breaking
- **Bump Minimum Supported Rust Version from 1.56 to 1.75**.
- **Get rid of `async-trait`**. Rust 1.75 included support for [`async fn` in traits](https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits.html), and `ultra-batch` now uses this instead of the `async-trait` crate. This means all `impl`s of `Fetcher` should no longer use the `#[async_trait]` macro.
- **Rename `Batcher` to `BatchFetcher`**.
- **Update `BatchFetcherBuilder::label()` to take an `Into<Cow<'static, str>>` instead of `Into<String>`**. This makes it cheaper to clone `BatchFetcher`, since cloning a static string is now much cheaper.

### Added
- **Added `BatchExecutor` type and `Executor` trait**. These types are similar to `BatchFetcher` and `Fetcher`, but are more suitable for database mutations (such as bulk inserts/deletes), or for cases where you want more fine-grained control over how fetching works.

### Changed
- **Remove some unnecessary associated type bounds on `Fetcher::Error`**.

## [v0.2.0] - 2022-02-15
### Breaking
- **Upgrade from Tokio 0.2.x to Tokio 1.0.x** ([#1](https://github.com/kylewlacy/ultra-batch/pull/1)). Thanks to [@maldrake](https://github.com/maldrake) for contributing this change!
- **Rename `Batcher::new(/* ... */).build()` to `Batcher::build(/* ... */).finish()`**. These new function names are meant to be more idiomatic, and additionally silence a Clipping warning in the project.
- **Bump the Minimum Supported Rust Version to v1.56**. The MSRV wasn't tracked previously, but `ultra-batch` did previously work as far back as Rust 1.45.0. Future changes to the MSRV will be documented in the Changelog going forward.

### Changed
- Switch from [log](https://crates.io/crates/log) to [Tracing](https://crates.io/crates/tracing) crate for logging. For compatibility, the `log` feature of `ultra-batch` can be enabled. This uses the `log` feature of Tracing, so see the [Tracing docs on the `log` feature](https://docs.rs/tracing/0.1.30/tracing/index.html#emitting-log-records) for more details.

## [v0.1.1] - 2020-08-20
### Added
- Implement logging using Rust's [log](https://crates.io/crates/log) crate. If you're using [`env_logger`](https://crates.io/crates/env_logger) or [`slog-envlogger`](https://crates.io/crates/slog-envlogger), then you can output logs in your app by setting the environment variable `RUST_LOG=ultra_batch=debug` or `RUST_LOG=ultra_batch=trace`.
- Add `BatcherBuilder.label` method to set a label when building a `Batcher`. Currently, this is used for clearer log messages and panic messages when working with multiple `Batcher`s.

### Changed
- Use [`chashmap::CHashMap`](https://docs.rs/chashmap/2.2.2/chashmap/struct.CHashMap.html) to cache values (rather than a `tokio::sync::RwLock<HashMap<K, V>>`). [`CHashMap`](https://docs.rs/chashmap/2.2.2/chashmap/struct.CHashMap.html) has a very similar interface to a standard `HashMap`, except it's designed for use in concurrent contexts without needing an explict lock. In practice, this seems to reduce lock contention.

## [v0.1.0] - 2020-08-16
### Added
- Initial release!

[Unreleased]: https://github.com/kylewlacy/ultra-batch/compare/v0.2.0...main
[v0.2.0]: https://github.com/kylewlacy/ultra-batch/compare/v0.1.1...v0.2.0
[v0.1.1]: https://github.com/kylewlacy/ultra-batch/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/kylewlacy/ultra-batch/releases/tag/v0.1.0
