# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Multi-output parallelism: `Executor::run` on a job with several
  outputs and `threads ≥ 2` now runs the outputs concurrently instead
  of one after another (closing the long-standing "multi-output
  parallelism is a deliberate follow-up" note in the executor module
  doc). Outputs are chunked into document-order waves whose width is
  clamped through `ExecutionContext::effective_workers(n_outputs)` —
  the same clamp codecs use for internal fan-out — and each wave's
  outputs run on scoped threads with an even split of the job budget
  as their codec-internal `ExecutionContext`. Preparation stays
  sequential in document order (setup-error precedence unchanged).
  Error contract: the earliest failing output in DOCUMENT order wins
  deterministically, healthy wave-mates still deliver their complete
  streams, and waves after the failure never start. `threads == 1`
  keeps the strictly-sequential serial loop; `spawn()` remains
  single-output. Pinned by `tests/multi_output_parallel.rs`
  (completeness + serial stats parity, a rendezvous proving real
  overlap, wave-width ≤ budget, and three error-precedence /
  wave-teardown contracts).

### Changed

- The executor's thread-budget autodetect goes through
  `ExecutionContext::auto()` instead of querying host parallelism
  directly — `ExecutionContext` is the framework's single threading
  authority, and every downstream clamp (codec fan-out, the new
  multi-output wave width) derives from the budget it resolves.

### Fixed

- The serial path (`threads == 1` / single-thread `run`) no longer
  pumps sources that belong to the job's OTHER outputs. The source
  probe opens every source leaf in the document, but `run_output`'s
  pump loop then drained every opened source to EOF — on a
  multi-output job every output re-read every other output's source,
  inflating `ExecutorStats::packets_read` (2× on a two-output job) and
  doing dead demux work quadratic in the output count. The pipelined
  path has always dropped route-less pumps in `prepare_pipelined_run`;
  the serial path now retains only the URIs its own tracks reference,
  restoring serial/pipelined stats parity on multi-output jobs (pinned
  by `tests/multi_output_parallel.rs`).

- Error-propagation coherence pinned by `tests/error_propagation.rs`:
  a failing sink or a mid-stream source failure surfaces the original
  typed error from `Executor::run` on both executor paths — no hang,
  first-error-wins, serial delivers all pre-failure frames, pipelined
  trades in-flight frames (bounded by channel depth) for prompt
  teardown.
- Criterion micro-benchmarks (`benches/graph.rs`) over the
  graph-resolution cold path: parse / validate / to_dag on wide (64
  tracks), deep (64 nested filters), and alias-chained (64 links)
  synthetic jobs, plus `Dag::describe` and `TrackInput::walk` / `leaf`.
- The staged (pipelined) runner now drives packet-shape and
  frame-shape sources natively. Bytes-shape URIs keep their demuxer
  thread; packet-shape URIs (RTMP-style) get an identical packet-pump
  thread without the container layer; frame-shape URIs (generators,
  rendered scenes) get a frame-pump thread that feeds the per-track
  frame-stage chains directly — no demux, no decode stage.
  Consequences: `Executor::spawn` (the playback path — live seek /
  progress / abort handle) now works over typed sources instead of
  failing with an internal "opener returned non-bytes shape" error,
  and `Executor::run` with `threads ≥ 2` no longer silently degrades
  typed-source jobs to the serial path. Seeks dispatched against a
  typed source surface `BarrierKind::SeekRejected` with the dispatch
  generation (`PacketSource` / `FrameSource` have no seek surface) and
  the stream keeps flowing. Serial/pipelined stats parity for typed
  sources is pinned by `tests/typed_source_staged.rs`.
- Validation completeness: `Job::validate` now rejects an explicitly
  empty / whitespace `codec` string with the track context (previously
  it survived to codec resolution and failed with an opaque "no codec
  registered" message), and rejects a `stream_selector` whose `kind`
  contradicts the typed track list it appears in (`video` selector
  inside `audio: [...]` — the selector kind won at DAG-build and muxed
  a mislabeled stream). Kind-free selectors in typed buckets and
  arbitrary selector kinds inside `all:` stay legal. `Job::from_json`
  additionally rejects the bare `@` top-level key (an alias whose name
  is empty after the sigil is unreferencable).

### Fixed

- `TrackInput` deserialization is now key-directed (pick the variant
  by which of `from` / `convert` / `render3d` / `filter` is present,
  in the historical priority order) instead of `#[serde(untagged)]`.
  The untagged derive re-tried every candidate variant at every
  nesting level of the recursive `input` field, making parse cost
  grow exponentially with filter-chain depth — a 64-deep chain
  effectively never finished parsing (found by the new `deep-64`
  bench, which ran 15+ minutes without completing). Key-directed
  dispatch parses the same chain in microseconds; a missing
  discriminator now reports the four expected keys instead of
  serde's generic "did not match any variant". Wire format unchanged.
- `all:` track fan-out now pins each duplicate's selector to a
  `(kind, per-kind ordinal)` pair. Pre-fix the expansion left the
  ordinal as `index: None`, so a source with two streams of the SAME
  media type (dual-audio, dual-subtitle, …) resolved BOTH duplicates
  to the first matching stream — the sink received two copies of
  stream 0 and the second stream's data silently vanished from the
  output on both the serial and pipelined paths. The two fan-out
  helpers (`expand_all_tracks` / `expand_all_tracks_pump`) now share
  one implementation. Pinned by a dual-audio stub demuxer whose
  per-stream payload fill bytes let the test attribute every delivered
  frame to its source stream (`tests/all_tracks_fan_out_same_kind.rs`,
  serial + pipelined).
- `Job::to_dag` now returns `Err(InvalidData)` on alias cycles instead
  of recursing without bound. The method's docs promised defensive
  validation, but a direct `to_dag` call on an unvalidated cyclic job
  (self-cycle, mutual cycle, or a cycle threaded through a filter
  wrapper) overflowed the stack during alias inlining. A per-track
  visiting stack in `build_source` rejects alias re-entry on the
  current descent path while still allowing legal diamond re-use of
  the same alias from different tracks.

## [0.1.11](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.10...v0.1.11) - 2026-06-09

### Other

- TrackInput typed accessors (kind_str / is_* / as_* / upstream / leaf / walk)
- drop release-plz.toml — use release-plz defaults across the workspace

### Added

- `TrackInput` typed accessors: `kind_str` (stable per-variant
  discriminator string), `is_source` / `is_filter` / `is_convert` /
  `is_render3d` predicates, `as_source` / `as_filter` / `as_convert` /
  `as_render3d` borrowing accessors, `upstream` (single-step wrapper
  descent), `leaf` (descend to terminal `Source` / `Render3D` node),
  and `walk` (outer-to-leaf visitor). Consumers building lints,
  diagnostics, or DAG transforms over a parsed `Job` no longer need
  to re-match the enum themselves. Purely additive — no existing
  variant / field / signature changed. Test count `schema::tests`
  12 → 21.

## [0.1.10](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.9...v0.1.10) - 2026-06-07

### Added

- TrackInput::Render3D — Phase C-3f schema bridge from Job to Render3D DAG

### Added

- `TrackInput::Render3D { source, backend, opts }` schema variant +
  `Render3DNode` payload — Phase C-3f schema bridge from `Job` to
  `DagNode::Render3D`. JSON shape:
  `{ "render3d": "<uri>", "backend": "<name>", "opts": { ... } }`.
  `Job::build_input` lowers this directly to the runtime DAG node;
  the executor still relies on the user-installed
  `RenderSourceFactory` (Phase C-3c) to instantiate the actual
  `FrameSource`. Unlocks consumer crates (e.g. oxideav-cli-convert)
  building Jobs that produce Render3D nodes via the standard
  `Job::to_dag()` path.

## [0.1.9](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.8...v0.1.9) - 2026-06-07

### Added

- render_source_factory callback — Phase C-3c wires Render3D execution

## [0.1.8](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.7...v0.1.8) - 2026-06-07

### Added

- DagNode::Render3D — Phase C-2 source variant for the 3D-render path

### Other

- surface copy-stage packets_copied via Progress
- surface encoder packets_encoded via Progress
- retry EOF Progress send to ride out backed-up receivers
- surface demuxer packets_read via Progress
- handle SourceOutput::MultiTitle + non-exhaustive wildcard
- surface decoder-skip count via Progress + ExecutorStats
- bump elapsed_micros drain + deadline timeouts for slow CI
- surface wall-clock progress via Progress::elapsed_micros
- surface in-flight packet bytes via Progress::queue_bytes
- memory-bounded packet queue (Executor::with_max_queue_bytes)
- configurable per-track channel-depth budget
- seek_with_generation returns the assigned generation
- extend BarrierKind::SeekFlush with landed_pts + time_base
- drain sink channel in background to unblock executor.finish
- graceful seek failure via BarrierKind::SeekRejected

## [0.1.6](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.5...v0.1.6) - 2026-05-05

### Other

- apply cargo fmt (rustfmt CI fix)
- move CodecPreferences + walker out of oxideav-core into pipeline

## [0.1.5](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.4...v0.1.5) - 2026-05-05

### Other

- multi-stream transcode_simple + CodecPreferences plumbing
- tolerate per-packet decoder errors instead of killing stream
- pipelined fallback panic on typed-source jobs (task #389)

### Fixed

- Executor: per-packet decoder errors no longer kill the entire stream. Both
  `run_decode_stage` (pipelined path, `staged.rs`) and `pump_packet` /
  `drain` (serial path, `executor.rs`) used to propagate any non-`NeedMore`
  / non-`Eof` `receive_frame` / `send_packet` error as a fatal `return
  Err(e)`, exiting the worker thread and starving every downstream sink.
  Real-world fallout: oxideplay sat at 00:00 on a real-world H.264/AAC mp4
  (`congress_mtgox_coins.mp4`) because the AAC decoder returned
  `invalid data: bitreader: out of bits` on the third packet — recoverable
  per the codec's own `pending.take()` semantics, but the executor took
  it as fatal and the audio clock never advanced. Decoders now follow the
  same logged-and-skipped policy the H.264 decoder already uses internally
  for per-slice errors. Regression coverage:
  `tests/decoder_error_tolerance.rs` exercises both serial and pipelined
  paths through a flaky stub decoder that errors on every 5th packet and
  asserts at least 35 of 50 frames still reach the sink (pre-fix: ~4).
- Executor: `run_output_pipelined` no longer panics with `Option::unwrap() on a
  None` when a typed-source job (e.g. `oxideav convert "xc:red" out.png` and
  every other `generate://` URI fed to the convert verb) falls back from the
  pipelined to the serial path. Previously the probe rewrote `Demuxer { source }`
  → `FrameSource { source }` in a clone of the DAG and handed *that* clone to
  `run_output`, but `run_output`'s own `resolve_source_shapes` only collects
  URIs from `Demuxer` nodes — so the second pass found none and
  `sources_by_uri.get(&pl.source_uri).unwrap()` blew up at executor.rs:269.
  The fix passes the *original* DAG (with its `Demuxer` leaves) to the
  fallback so `run_output` can re-discover and re-open the URIs itself.
  The `.unwrap()` is also rewritten as a descriptive `Error::invalid` so future
  resolver bugs surface as a normal error rather than a panic. Regression
  coverage in `tests/source_variants.rs` exercises both the FrameSource and
  PacketSource shapes through the pipelined path.

## [0.1.4](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.3...v0.1.4) - 2026-05-03

### Other

- bump oxideav-audio-filter dev-dep 0.0 -> 0.1
- replace never-match regex with semver_check = false

## [0.1.3](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.2...v0.1.3) - 2026-05-02

### Other

- migrate to centralized OxideAV/.github reusable workflows
- stay on 0.1.x during heavy dev (semver_check=false)
- branch on SourceOutput shape (Bytes/Packets/Frames)
- non-blocking round-robin so a slow track doesn't starve a fast one
- stream frames per-receive instead of drain-then-send
- adopt slim VideoFrame/AudioFrame shape
- pin release-plz to patch-only bumps

### Added

- DAG: `PacketSource` and `FrameSource` node variants matching the typed-source
  shapes in `oxideav-core::SourceOutput`. The executor probes every `Demuxer`
  leaf via `SourceRegistry::open`, then rewrites the node when the registry
  hands back `SourceOutput::Packets` / `SourceOutput::Frames` so downstream
  decode (for packets) or filter+sink (for frames) consumes the right shape
  without an intervening container demux or decoder.
- Executor: `SourcePump` enum + `open_source` / `resolve_source_shapes`
  helpers. `run_output` now branches per source shape — bytes goes through
  the historical demuxer chain, packet sources skip demux, frame sources
  skip both demux and decode and route directly into the filter chain (or
  the sink if no filter is declared).
- Test: `tests/source_variants.rs` exercises all three shapes end-to-end via
  in-tree mocks; each path produces the expected single audio frame.

### Changed

- Pipelined runner falls back to the serial path when any source resolves
  to a non-bytes shape. The staged-worker variants for `PacketSource` /
  `FrameSource` are tracked as follow-up work — RTMP and the generator
  both run fine on the serial path, which is the default for live playback
  on the typical thread budget.

## [0.1.2](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.1...v0.1.2) - 2026-04-25

### Other

- release v0.1.1

## [0.1.1](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.0...v0.1.1) - 2026-04-25

### Other

- drop oxideav-codec/oxideav-container shims, import from oxideav-core
- take RuntimeContext, drop separate filter registry + audio/image filter deps
- drop oxideav-basic dev-dep, use in-tree stub demuxer
- expand `all:` tracks to one runtime per source stream
- pipe input port into spectrogram factory
- auto-decode at @display / @out when no codec is declared
- drop JobSink Send super-bound, require Send only at sink-override site
- add seek-barrier flow + Executor::spawn / ExecutorHandle
- drop mux receivers before join to fix abort-path deadlock
- integration test for spectrogram multi-port output per plan § 5/10
- collapse executor to StreamFilter via FilterRegistry per plan § 6/7/8
- FilterRegistry + legacy adapters per plan § 2/3
- propagate video-filter output params (width/height/pixfmt) to encoder
- wire video filters (resize/blur/edge) via new image_filter feature
- bump oxideav-source dep to "0.1"
- absorb oxideav-job: JSON job graph + executor folded in
- release v0.0.3

## [0.1.0](https://github.com/OxideAV/oxideav-pipeline/compare/v0.0.3...v0.1.0) - 2026-04-19

### Other

- bump version to 0.1.0
