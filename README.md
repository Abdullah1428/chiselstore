# ChiselStore

[![Rust](https://github.com/chiselstrike/chiselstore/actions/workflows/rust.yml/badge.svg)](https://github.com/chiselstrike/chiselstore/actions/workflows/rust.yml)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

This repo version of ChiselStore is an embeddable, distributed [SQLite](https://www.sqlite.org/index.html) for Rust, powered by [Omni Paxos](https://github.com/haraldng/omnipaxos).

SQLite is a fast and compact relational database management system, but it is limited to single-node configurations.
ChiselStore extends SQLite to run on a cluster of machines with the [Omni Paxos Sequence Consensus Algorithm](https://haraldng.github.io/omnipaxos/sequencepaxos/index.html).

With ChiselStore, you get the benefits of easy-to-use, embeddable SQLite but with Omni Paxos high availability and fault tolerance.

For more information, check out the following [blog post](https://glaubercosta-11125.medium.com/winds-of-change-in-web-data-728187331f53).

## Features

* SQLite with Omni Paxos's high availability and fault tolerance
* Embeddable Rust library

### Roadmap and Future Work

* Efficient node restarts (with Omni Paxos Snapshots and Compaction)
* Dynamic cluster membership (with Omni Paxos sequence consensus)
* Support executing non-deterministic SQL functions

## Getting Started

See the [example server](examples) of how to use the ChiselStore library.

## License

This project is licensed under the [MIT license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in ChiselStore by you, shall be licensed as MIT, without any additional
terms or conditions.
