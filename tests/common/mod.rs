// Shared test stubs for oxideav-pipeline integration tests. Each test
// imports `mod common;` and uses `common::stub::*`.
//
// `#[allow(dead_code)]` because every test only uses a subset of the
// stubs — Rust would otherwise warn for whichever entry points the
// individual binary doesn't reference.
#![allow(dead_code)]

pub mod stub;
