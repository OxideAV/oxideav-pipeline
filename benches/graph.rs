//! Graph-resolution micro-benchmarks.
//!
//! The executor resolves every job through the same cold path — parse
//! JSON → `validate()` (reference integrity + cycle detection) →
//! `to_dag()` (alias inlining + node emission) — and diagnostics
//! walk the result with `Dag::describe()` / `TrackInput::walk`.
//! None of this is per-packet work, but engines that build jobs
//! dynamically (a queue server resolving hundreds of submitted jobs,
//! a UI re-validating on every keystroke) sit on these functions, and
//! the alias-inlining + cycle-guard logic is recursive — this bench
//! keeps its cost visible as the schema grows.
//!
//! Three synthetic shapes, all built in-memory (no I/O, no codecs):
//!
//! * **wide** — one output with N independent source tracks (fan-in
//!   heavy: track loop + selector resolution dominate).
//! * **deep** — one track wrapped in N nested filter nodes (recursion
//!   heavy: `build_input` / `check_refs_in_input` / `walk` descend N
//!   levels).
//! * **aliased** — a linear chain of N aliases ending at a source,
//!   referenced by one output (alias-inlining + cycle-guard heavy;
//!   `validate`'s DFS and `to_dag`'s visiting stack both traverse it).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use oxideav_pipeline::Job;

/// One output, `n` audio tracks, each from its own source URI.
fn wide_job_json(n: usize) -> String {
    let tracks: Vec<String> = (0..n)
        .map(|i| format!(r#"{{"from": "in_{i}.wav"}}"#))
        .collect();
    format!(r#"{{"out.mkv": {{"audio": [{}]}}}}"#, tracks.join(","))
}

/// One output whose single track is `n` nested filter wrappers over a
/// source leaf. Targets the reserved `@null` sink: a filter-terminated
/// track without a `codec` is frame-producing, which only reserved
/// sinks accept (a file output would make `to_dag` error, correctly).
fn deep_job_json(n: usize) -> String {
    let mut input = r#"{"from": "in.wav"}"#.to_string();
    for i in 0..n {
        input = format!(r#"{{"filter": "f{i}", "params": {{"gain_db": 1}}, "input": {input}}}"#);
    }
    format!(r#"{{"@null": {{"audio": [{input}]}}}}"#)
}

/// A linear chain of `n` aliases (`@a0 <- @a1 <- … <- @a(n-1)`), with
/// the output reading the last link.
fn aliased_job_json(n: usize) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(n + 1);
    parts.push(r#""@a0": {"all": [{"from": "in.wav"}]}"#.to_string());
    for i in 1..n {
        parts.push(format!(
            r#""@a{i}": {{"all": [{{"from": "@a{}"}}]}}"#,
            i - 1
        ));
    }
    parts.push(format!(
        r#""out.mkv": {{"audio": [{{"from": "@a{}"}}]}}"#,
        n - 1
    ));
    format!("{{{}}}", parts.join(","))
}

fn bench_parse_validate_dag(c: &mut Criterion) {
    let mut group = c.benchmark_group("resolve");
    for (label, json) in [
        ("wide-64", wide_job_json(64)),
        ("deep-64", deep_job_json(64)),
        ("aliased-64", aliased_job_json(64)),
    ] {
        // Parse alone (serde + top-level walk).
        group.bench_with_input(BenchmarkId::new("parse", label), &json, |b, j| {
            b.iter(|| Job::from_json(black_box(j)).unwrap())
        });
        // Validate alone on a pre-parsed job (ref integrity + cycles).
        let job = Job::from_json(&json).unwrap();
        group.bench_with_input(BenchmarkId::new("validate", label), &job, |b, j| {
            b.iter(|| black_box(j).validate().unwrap())
        });
        // DAG build alone (alias inlining + node emission + cycle guard).
        group.bench_with_input(BenchmarkId::new("to_dag", label), &job, |b, j| {
            b.iter(|| black_box(j).to_dag().unwrap())
        });
    }
    group.finish();
}

fn bench_walk_and_describe(c: &mut Criterion) {
    let mut group = c.benchmark_group("walk");
    let deep = Job::from_json(&deep_job_json(64)).unwrap();
    let dag = deep.to_dag().unwrap();
    group.bench_function("describe/deep-64", |b| {
        b.iter(|| black_box(&dag).describe())
    });
    let track = &deep.outputs["@null"].audio[0];
    group.bench_function("track_input_walk/deep-64", |b| {
        b.iter(|| {
            let mut n = 0usize;
            black_box(&track.input).walk(|_| n += 1);
            black_box(n)
        })
    });
    group.bench_function("track_input_leaf/deep-64", |b| {
        b.iter(|| black_box(&track.input).leaf().kind_str())
    });
    group.finish();
}

criterion_group!(benches, bench_parse_validate_dag, bench_walk_and_describe);
criterion_main!(benches);
