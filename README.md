consistent_hash
===============

[![Crates.io: consistent_hash](http://meritbadge.herokuapp.com/consistent_hash)](https://crates.io/crates/consistent_hash)
[![Build Status](https://travis-ci.org/sile/consistent_hash.svg?branch=master)](https://travis-ci.org/sile/consistent_hash)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A Rust implementation of Consistent hashing algorithm.

[Documentation](https://docs.rs/consistent_hash)

Currently this crate only provides statically built, virtual node based hash rings.

An Informal Benchmark
----------------------

```sh
$ cat /proc/cpuinfo  | grep 'model name' | head -1
model name      : Intel(R) Core(TM) i7-6600U CPU @ 2.60GHz

$ uname -a
Linux ubuntu 4.8.0-34-generic #36-Ubuntu SMP Wed Dec 21 17:24:18 UTC 2016 x86_64 x86_64 x86_64 GNU/Linux

$ cargo run --release --example bench -- /usr/share/dict/words --vnode_count 1000 --nodes Rust Alef C++ Camlp4 CommonLisp Erlang Haskell Hermes Limbo Napier Napier88 Newsqueak NIL Sather StandardML

WORD COUNT: 99156
REAL NODE COUNT: 15
VIRTUAL NODE COUNT: 15000 (1000 per node)

SELECTED COUNT PER NODE:
- Rust:         6265
- NIL:  6642
- Sather:       7165
- Erlang:       6545
- Camlp4:       6912
- Napier88:     6287
- CommonLisp:   6901
- C++:  6504
- StandardML:   6937
- Hermes:       6166
- Newsqueak:    6725
- Alef:         6586
- Haskell:      6240
- Limbo:        6754
- Napier:       6527

ELAPSED: 18 ms
WORDS PER SECOND: 5342456
```
