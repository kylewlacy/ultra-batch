# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- Use [`chashmap::CHashMap`](https://docs.rs/chashmap/2.2.2/chashmap/struct.CHashMap.html) to cache values (rather than a `tokio::sync::RwLock<HashMap<K, V>>`). [`CHashMap`](https://docs.rs/chashmap/2.2.2/chashmap/struct.CHashMap.html) has a very similar interface to a standard `HashMap`, except it's designed for use in concurrent contexts without needing an explict lock. In practice, this seems to reduce lock contention.

## [v0.1.0] - 2020-08-16
### Added
- Initial release!

[Unreleased]: https://github.com/kylewlacy/ultra-batch/compare/v0.1.0...master
[v0.1.0]: https://github.com/kylewlacy/ultra-batch/releases/tag/v0.1.0
